from __future__ import annotations

import operator
from collections.abc import (
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Any, ClassVar, TypeVar, cast
from uuid import UUID

import attrs
import pymongo
from attrs.validators import instance_of
from bson import CodecOptions, UuidRepresentation
from bson.codec_options import TypeEncoder, TypeRegistry
from pymongo import ASCENDING, DeleteOne, UpdateOne
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError

from .._enums import CoalescePolicy, ConflictPolicy, JobOutcome
from .._events import (
    DataStoreEvent,
    JobAcquired,
    JobAdded,
    JobReleased,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    TaskAdded,
    TaskRemoved,
    TaskUpdated,
)
from .._exceptions import (
    ConflictingIdError,
    DeserializationError,
    SerializationError,
    TaskLookupError,
)
from .._structures import Job, JobResult, Schedule, ScheduleResult, Task
from .._utils import create_repr
from ..abc import EventBroker
from .base import BaseExternalDataStore

T = TypeVar("T", bound=Mapping[str, Any])


class CustomEncoder(TypeEncoder):
    def __init__(self, python_type: type, encoder: Callable):
        self._python_type = python_type
        self._encoder = encoder

    @property
    def python_type(self) -> type:
        return self._python_type

    def transform_python(self, value: Any) -> Any:
        return self._encoder(value)


def marshal_timestamp(timestamp: datetime | None, key: str) -> Mapping[str, Any]:
    if timestamp is None:
        return {key: None, key + "_utcoffset": None}

    return {
        key: timestamp.timestamp(),
        key + "_utcoffset": cast(timedelta, timestamp.utcoffset()).total_seconds()
        // 60,
    }


def marshal_document(document: dict[str, Any]) -> None:
    if "id" in document:
        document["_id"] = document.pop("id")

    for key, value in list(document.items()):
        if isinstance(value, datetime):
            document.update(marshal_timestamp(document[key], key))


def unmarshal_timestamps(document: dict[str, Any]) -> None:
    for key in list(document):
        if key.endswith("_utcoffset") and (value := document.pop(key)) is not None:
            offset = timedelta(seconds=value * 60)
            tzinfo = timezone(offset)
            time_micro = document[key[:-10]]
            document[key[:-10]] = datetime.fromtimestamp(time_micro, tzinfo)


def task_revision_key(task_id: str, revision: int) -> dict[str, Any]:
    return {"task_id": task_id, "revision": revision}


def task_revision_filter(task_id: str, revision: int) -> dict[str, Any]:
    # Dotted field matching is used because exact embedded document matches do not
    # work reliably with the custom type registry codec options
    return {"_id.task_id": task_id, "_id.revision": revision}


def task_to_revision_document(
    task: Task, serializer: Any, *, current: bool = True, job_count: int = 0
) -> dict[str, Any]:
    document = task.marshal(serializer)
    task_id = document.pop("id")
    document["_id"] = task_revision_key(task_id, document["revision"])
    # Keep the task ID as a regular field as well, for queries and indexes
    document["task_id"] = task_id
    # "current" marks the latest configuration generation of a live task;
    # "job_count" is the number of queued or running jobs bound to this generation.
    # These enable atomic, race-free pruning with single-collection filters.
    document["current"] = current
    document["job_count"] = job_count
    return document


@attrs.define(eq=False, repr=False)
class MongoDBDataStore(BaseExternalDataStore):
    """
    Uses a MongoDB server to store data.

    When started, this data store creates the appropriate indexes on the given database
    if they're not already present.

    Operations are retried (in accordance to ``retry_settings``) when an operation
    raises :exc:`pymongo.errors.ConnectionFailure`.

    :param client_or_uri: an asynchronous PyMongo client or a MongoDB connection URI
    :param database: name of the database to use

    .. note:: The data store will not manage the life cycle of any client instance
        passed to it, so you need to close the client after you're done with it.

    .. note:: Datetimes are stored as integers along with their UTC offsets instead of
        BSON datetimes due to the BSON datetimes only being accurate to the millisecond
        while Python datetimes are accurate to the microsecond.
    """

    client_or_uri: AsyncMongoClient | str = attrs.field(
        validator=instance_of((AsyncMongoClient, str))
    )
    database: str = attrs.field(
        default="apscheduler", kw_only=True, validator=instance_of(str)
    )

    _client: AsyncMongoClient = attrs.field(init=False)
    _close_on_exit: bool = attrs.field(init=False, default=False)
    _task_attrs: ClassVar[list[str]] = [field.name for field in attrs.fields(Task)]
    _schedule_attrs: ClassVar[list[str]] = [
        field.name for field in attrs.fields(Schedule)
    ]
    _job_attrs: ClassVar[list[str]] = [field.name for field in attrs.fields(Job)]
    _tasks: AsyncCollection = attrs.field(init=False)
    _task_revisions: AsyncCollection = attrs.field(init=False)
    _schedules: AsyncCollection = attrs.field(init=False)
    _jobs: AsyncCollection = attrs.field(init=False)
    _jobs_results: AsyncCollection = attrs.field(init=False)

    @property
    def _temporary_failure_exceptions(self) -> tuple[type[Exception], ...]:
        return (ConnectionFailure,)

    def __attrs_post_init__(self) -> None:
        if isinstance(self.client_or_uri, str):
            self._client = AsyncMongoClient(self.client_or_uri)
            self._close_on_exit = True
        else:
            self._client = self.client_or_uri

        type_registry = TypeRegistry(
            [
                CustomEncoder(timedelta, timedelta.total_seconds),
                CustomEncoder(ConflictPolicy, operator.attrgetter("name")),
                CustomEncoder(CoalescePolicy, operator.attrgetter("name")),
                CustomEncoder(JobOutcome, operator.attrgetter("name")),
            ]
        )
        codec_options: CodecOptions = CodecOptions(
            type_registry=type_registry,
            uuid_representation=UuidRepresentation.STANDARD,
        )
        database = self._client.get_database(self.database, codec_options=codec_options)
        self._tasks = database["tasks"]
        self._task_revisions = database["task_revisions"]
        self._schedules = database["schedules"]
        self._jobs = database["jobs"]
        self._jobs_results = database["job_results"]

    def __repr__(self) -> str:
        server_descriptions = self._client.topology_description.server_descriptions()
        return create_repr(self, host=list(server_descriptions))

    async def _initialize(self) -> None:
        async with self._client.start_session() as session:
            if self.start_from_scratch:
                await self._tasks.delete_many({}, session=session)
                await self._task_revisions.delete_many({}, session=session)
                await self._schedules.delete_many({}, session=session)
                await self._jobs.delete_many({}, session=session)
                await self._jobs_results.delete_many({}, session=session)

            await self._task_revisions.create_index("task_id", session=session)
            await self._task_revisions.create_index(
                [("current", ASCENDING), ("job_count", ASCENDING)], session=session
            )
            await self._schedules.create_index("task_id", session=session)
            await self._schedules.create_index("next_fire_time", session=session)
            await self._schedules.create_index("acquired_by", session=session)
            await self._jobs.create_index("task_id", session=session)
            await self._jobs.create_index("task_revision", session=session)
            await self._jobs.create_index("schedule_id", session=session)
            await self._jobs.create_index("created_at", session=session)
            await self._jobs.create_index("acquired_by", session=session)
            await self._jobs_results.create_index("expires_at", session=session)

    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker, logger: Logger
    ) -> None:
        if self._close_on_exit:
            exit_stack.push_async_callback(self._client.close)

        await super().start(exit_stack, event_broker, logger)
        server_info = await self._client.server_info()
        if server_info["versionArray"] < [4, 0]:
            raise RuntimeError(
                f"MongoDB server must be at least v4.0; current version = "
                f"{server_info['version']}"
            )

        async for attempt in self._retry():
            with attempt:
                await self._initialize()

    async def add_task(self, task: Task) -> Task:
        async for attempt in self._retry():
            with attempt:
                previous = await self._tasks.find_one({"_id": task.id})
                if previous is None:
                    document = task.marshal(self.serializer)
                    document["_id"] = document.pop("id")
                    document["revision"] = 1
                    document["running_jobs"] = 0
                    await self._tasks.insert_one(document)
                    await self._task_revisions.insert_one(
                        task_to_revision_document(
                            attrs.evolve(task, revision=1, running_jobs=0),
                            self.serializer,
                        )
                    )
                    created = True
                else:
                    created = False
                    # Timedeltas are stored as seconds, so normalize before
                    # comparing
                    stored_misfire_grace_time = (
                        task.misfire_grace_time.total_seconds()
                        if task.misfire_grace_time is not None
                        else None
                    )
                    config_changed = any(
                        previous.get(key) != value
                        for key, value in (
                            ("func", task.func),
                            ("job_executor", task.job_executor),
                            ("max_running_jobs", task.max_running_jobs),
                            ("misfire_grace_time", stored_misfire_grace_time),
                            ("metadata", task.metadata),
                        )
                    )
                    old_revision = previous.get("revision", 1)
                    if config_changed:
                        revision = old_revision + 1
                        running_jobs = 0

                        # Backfill a snapshot of the previous configuration if this
                        # revision is missing (e.g. a task created by an older
                        # APScheduler version), so queued jobs keep a valid snapshot
                        backfill_filter = task_revision_filter(task.id, old_revision)
                        if await self._task_revisions.find_one(
                            backfill_filter,
                            projection=["_id"],
                        ) is None:
                            await self._task_revisions.insert_one(
                                {
                                    "_id": task_revision_key(task.id, old_revision),
                                    "task_id": task.id,
                                    "revision": old_revision,
                                    "func": previous.get("func"),
                                    "job_executor": previous.get("job_executor"),
                                    "max_running_jobs": previous.get(
                                        "max_running_jobs"
                                    ),
                                    "misfire_grace_time": previous.get(
                                        "misfire_grace_time"
                                    ),
                                    "metadata": previous.get("metadata", {}),
                                    "running_jobs": previous.get("running_jobs", 0),
                                    "current": False,
                                    "job_count": 0,
                                }
                            )

                        # The previous generation is no longer the current one
                        await self._task_revisions.update_many(
                            {"task_id": task.id, "current": True},
                            {"$set": {"current": False}},
                        )
                    else:
                        revision = old_revision
                        running_jobs = previous.get("running_jobs", 0)

                    document = task.marshal(self.serializer)
                    document.pop("id")
                    document["revision"] = revision
                    document["running_jobs"] = running_jobs
                    await self._tasks.replace_one(
                        {"_id": task.id}, document, upsert=True
                    )
                    if config_changed:
                        snapshot = attrs.evolve(
                            task, revision=revision, running_jobs=0
                        )
                        await self._task_revisions.insert_one(
                            task_to_revision_document(snapshot, self.serializer)
                        )

        if created:
            await self._event_broker.publish(TaskAdded(task_id=task.id))
        else:
            await self._event_broker.publish(TaskUpdated(task_id=task.id))

        return await self.get_task(task.id)

    async def remove_task(self, task_id: str) -> None:
        async for attempt in self._retry():
            with attempt:
                if not await self._tasks.find_one_and_delete({"_id": task_id}):
                    raise TaskLookupError(task_id)

                # The current generation stops being current now
                await self._task_revisions.update_many(
                    {"task_id": task_id, "current": True},
                    {"$set": {"current": False}},
                )

                # Remove generations that no longer have any bound jobs
                await self._task_revisions.delete_many(
                    {"task_id": task_id, "current": False, "job_count": 0}
                )

        await self._event_broker.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str, revision: int | None = None) -> Task:
        async for attempt in self._retry():
            with attempt:
                if revision is None:
                    document = await self._tasks.find_one(
                        {"_id": task_id}, projection=self._task_attrs
                    )
                else:
                    document = await self._task_revisions.find_one(
                        task_revision_filter(task_id, revision)
                    )
                    if document is not None:
                        document.pop("_id")
                        document.pop("current", None)
                        document.pop("job_count", None)

        if not document:
            raise TaskLookupError(task_id)

        if revision is None:
            document["id"] = document.pop("_id")
        else:
            document["id"] = document.pop("task_id")

        task = Task.unmarshal(self.serializer, document)
        return task

    async def get_tasks(self) -> list[Task]:
        async for attempt in self._retry():
            with attempt:
                tasks: list[Task] = []
                async with self._tasks.find(
                    projection=self._task_attrs, sort=[("_id", pymongo.ASCENDING)]
                ) as cursor:
                    async for document in cursor:
                        document["id"] = document.pop("_id")
                        tasks.append(Task.unmarshal(self.serializer, document))

        return tasks

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        filters = {"_id": {"$in": list(ids)}} if ids is not None else {}
        async for attempt in self._retry():
            with attempt:
                schedules: list[Schedule] = []
                async with self._schedules.find(filters).sort("_id") as cursor:
                    async for document in cursor:
                        document["id"] = document.pop("_id")
                        unmarshal_timestamps(document)
                        try:
                            schedule = Schedule.unmarshal(self.serializer, document)
                        except DeserializationError:
                            self._logger.warning(
                                "Failed to deserialize schedule %r", document["_id"]
                            )
                            continue

                        schedules.append(schedule)

        return schedules

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        event: DataStoreEvent
        document = schedule.marshal(self.serializer)
        marshal_document(document)
        try:
            async for attempt in self._retry():
                with attempt:
                    await self._schedules.insert_one(document)
        except DuplicateKeyError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                async for attempt in self._retry():
                    with attempt:
                        await self._schedules.replace_one(
                            {"_id": schedule.id},
                            document,
                            True,
                        )

                event = ScheduleUpdated(
                    schedule_id=schedule.id,
                    task_id=schedule.task_id,
                    next_fire_time=schedule.next_fire_time,
                )
                await self._event_broker.publish(event)
        else:
            event = ScheduleAdded(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )
            await self._event_broker.publish(event)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        filters = {"_id": {"$in": list(ids)}} if ids is not None else {}
        async for attempt in self._retry():
            with attempt:
                async with (
                    self._client.start_session() as session,
                    self._schedules.find(
                        filters, projection=["_id", "task_id"], session=session
                    ) as cursor,
                ):
                    new_ids = [(doc["_id"], doc["task_id"]) async for doc in cursor]
                    if new_ids:
                        await self._schedules.delete_many(filters, session=session)

        for schedule_id, task_id in new_ids:
            await self._event_broker.publish(
                ScheduleRemoved(
                    schedule_id=schedule_id, task_id=task_id, finished=False
                )
            )

    async def acquire_schedules(
        self, scheduler_id: str, lease_duration: timedelta, limit: int
    ) -> list[Schedule]:
        schedules: list[Schedule] = []

        # Fetch up to {limit} schedules
        while len(schedules) < limit:
            async for attempt in self._retry():
                async with AsyncExitStack() as exit_stack:
                    exit_stack.enter_context(attempt)
                    session = await exit_stack.enter_async_context(
                        self._client.start_session()
                    )
                    now = datetime.now(timezone.utc)
                    documents = await self._schedules.find(
                        {
                            "next_fire_time": {"$lte": now.timestamp()},
                            "$and": [
                                {
                                    "$or": [
                                        {"paused": {"$exists": False}},
                                        {"paused": False},
                                    ]
                                },
                                {
                                    "$or": [
                                        {"acquired_by": scheduler_id},
                                        {"acquired_until": {"$exists": False}},
                                        {"acquired_until": {"$lt": now.timestamp()}},
                                    ]
                                },
                            ],
                        },
                        sort=[("next_fire_time", ASCENDING)],
                        limit=limit - len(schedules),
                        session=session,
                    ).to_list()

                    # Bail out if there are no more schedules to be acquired
                    if not documents:
                        return schedules

                    now = datetime.now(timezone.utc)
                    acquired_until = now + lease_duration
                    schedule_ids = [doc["_id"] for doc in documents]
                    result = await self._schedules.update_many(
                        {
                            "_id": {"$in": schedule_ids},
                            "$or": [
                                {"acquired_until": {"$exists": False}},
                                {"acquired_until": {"$lt": now.timestamp()}},
                            ],
                        },
                        {
                            "$set": {
                                "acquired_by": scheduler_id,
                                **marshal_timestamp(acquired_until, "acquired_until"),
                            }
                        },
                        session=session,
                    )

                    # If the number of modified schedules was smaller than expected,
                    # manually check which ones were successfully acquired
                    if result.modified_count != len(schedule_ids):
                        async with self._schedules.find(
                            {
                                "_id": {"$in": schedule_ids},
                                "acquired_by": scheduler_id,
                            },
                            sort=[("created_at", ASCENDING)],
                            projection=["_id"],
                            session=session,
                        ) as cursor:
                            acquired_schedule_ids = {doc["_id"] async for doc in cursor}
                            documents = [
                                doc
                                for doc in documents
                                if doc["_id"] in acquired_schedule_ids
                            ]

                    for doc in documents:
                        # Deserialize the schedule
                        doc["id"] = doc.pop("_id")
                        unmarshal_timestamps(doc)
                        schedules.append(Schedule.unmarshal(self.serializer, doc))

                return schedules

    async def release_schedules(
        self, scheduler_id: str, results: Sequence[ScheduleResult]
    ) -> None:
        updated_schedules: list[tuple[str, datetime | None]] = []
        finished_schedule_ids: list[str] = []
        task_ids = {result.schedule_id: result.task_id for result in results}

        requests: list[UpdateOne | DeleteOne] = []
        for result in results:
            filters = {"_id": result.schedule_id, "acquired_by": scheduler_id}
            try:
                serialized_trigger = self.serializer.serialize(result.trigger)
            except SerializationError:
                self._logger.exception(
                    "Error serializing schedule %r – removing from data store",
                    result.schedule_id,
                )
                requests.append(DeleteOne(filters))
                finished_schedule_ids.append(result.schedule_id)
                continue

            update = {
                "$unset": {
                    "acquired_by": True,
                    "acquired_until": True,
                    "acquired_until_utcoffset": True,
                },
                "$set": {
                    "trigger": serialized_trigger,
                    **marshal_timestamp(result.last_fire_time, "last_fire_time"),
                    **marshal_timestamp(result.next_fire_time, "next_fire_time"),
                },
            }
            requests.append(UpdateOne(filters, update))
            updated_schedules.append((result.schedule_id, result.next_fire_time))

        if requests:
            async for attempt in self._retry():
                with attempt:
                    async with self._client.start_session() as session:
                        await self._schedules.bulk_write(
                            requests, ordered=False, session=session
                        )

        for schedule_id, next_fire_time in updated_schedules:
            event = ScheduleUpdated(
                schedule_id=schedule_id,
                task_id=task_ids[schedule_id],
                next_fire_time=next_fire_time,
            )
            await self._event_broker.publish(event)

        for schedule_id in finished_schedule_ids:
            await self._event_broker.publish(
                ScheduleRemoved(
                    schedule_id=schedule_id,
                    task_id=task_ids[schedule_id],
                    finished=True,
                )
            )

    async def get_next_schedule_run_time(self) -> datetime | None:
        async for attempt in self._retry():
            with attempt:
                document = await self._schedules.find_one(
                    {"next_fire_time": {"$ne": None}},
                    projection=["next_fire_time", "next_fire_time_utcoffset"],
                    sort=[("next_fire_time", ASCENDING)],
                )

        if document:
            unmarshal_timestamps(document)
            return document["next_fire_time"]
        else:
            return None

    async def add_job(self, job: Job) -> None:
        async for attempt in self._retry():
            with attempt:
                # Bind the job to the current task revision (if not already bound to
                # a specific one)
                if job.task_revision is None:
                    task = await self._tasks.find_one(
                        {"_id": job.task_id}, projection=["revision"]
                    )
                    if task is not None:
                        job.task_revision = task["revision"]

                document = job.marshal(self.serializer)
                marshal_document(document)
                await self._jobs.insert_one(document)

                if job.task_revision is not None:
                    # Count the job on its bound generation. If the snapshot is
                    # missing (e.g. a task created by an older APScheduler version),
                    # reconstruct it from the current task definition first.
                    revision_filter = {
                        "task_id": job.task_id,
                        "revision": job.task_revision,
                    }
                    result = await self._task_revisions.update_one(
                        revision_filter, {"$inc": {"job_count": 1}}
                    )
                    if not result.matched_count:
                        task = await self._tasks.find_one({"_id": job.task_id})
                        if task is not None:
                            await self._task_revisions.update_one(
                                task_revision_filter(
                                    job.task_id, job.task_revision
                                ),
                                {
                                    "$setOnInsert": {
                                        "task_id": job.task_id,
                                        "revision": job.task_revision,
                                        "func": task.get("func"),
                                        "job_executor": task.get("job_executor"),
                                        "max_running_jobs": task.get(
                                            "max_running_jobs"
                                        ),
                                        "misfire_grace_time": task.get(
                                            "misfire_grace_time"
                                        ),
                                        "metadata": task.get("metadata", {}),
                                        "running_jobs": 0,
                                        "current": True,
                                    },
                                    "$inc": {"job_count": 1},
                                },
                                upsert=True,
                            )

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            task_revision=job.task_revision,
            schedule_id=job.schedule_id,
        )
        await self._event_broker.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        filters = {"_id": {"$in": list(ids)}} if ids is not None else {}
        async for attempt in self._retry():
            with attempt:
                jobs: list[Job] = []
                async with self._jobs.find(filters).sort("_id") as cursor:
                    async for document in cursor:
                        document["id"] = document.pop("_id")
                        unmarshal_timestamps(document)
                        try:
                            job = Job.unmarshal(self.serializer, document)
                        except DeserializationError:
                            self._logger.warning(
                                "Failed to deserialize job %r", document["id"]
                            )
                            continue

                        jobs.append(job)

        return jobs

    async def acquire_jobs(
        self, scheduler_id: str, lease_duration: timedelta, limit: int | None = None
    ) -> list[Job]:
        events: list[JobAcquired | JobReleased] = []
        async for attempt in self._retry():
            async with AsyncExitStack() as exit_stack:
                exit_stack.enter_context(attempt)
                session = await exit_stack.enter_async_context(
                    self._client.start_session()
                )

                # Fetch up to {limit} jobs
                now = datetime.now(timezone.utc)
                documents = await self._jobs.find(
                    {
                        "$or": [
                            {"acquired_until": {"$exists": False}},
                            {"acquired_until": {"$lt": now.timestamp()}},
                        ]
                    },
                    sort=[("created_at", ASCENDING)],
                    limit=limit,
                    session=session,
                ).to_list()

                # Mark them as acquired by this scheduler
                acquired_until = now + lease_duration
                job_ids = [doc["_id"] for doc in documents]
                result = await self._jobs.update_many(
                    {
                        "_id": {"$in": job_ids},
                        "$or": [
                            {"acquired_until": {"$exists": False}},
                            {"acquired_until": {"$lt": now.timestamp()}},
                        ],
                    },
                    {
                        "$set": {
                            "acquired_by": scheduler_id,
                            **marshal_timestamp(acquired_until, "acquired_until"),
                        }
                    },
                )

                # If the number of modified jobs was smaller than expected, manually
                # check which jobs were successfully acquired
                if result.modified_count != len(job_ids):
                    async with self._jobs.find(
                        {
                            "_id": {"$in": job_ids},
                            "acquired_by": scheduler_id,
                        },
                        sort=[("created_at", ASCENDING)],
                        projection=["_id"],
                        session=session,
                    ) as cursor:
                        acquired_job_ids = {doc["_id"] async for doc in cursor}
                        documents = [
                            doc for doc in documents if doc["_id"] in acquired_job_ids
                        ]

                acquired_jobs: list[Job] = []
                skipped_job_ids: list[UUID] = []
                for doc in documents:
                    # Deserialize the job
                    doc["id"] = doc.pop("_id")
                    unmarshal_timestamps(doc)
                    try:
                        job = Job.unmarshal(self.serializer, doc)
                    except DeserializationError as exc:
                        # Deserialization failed, so record the exception as the job
                        # result
                        result = JobResult(
                            job_id=doc["id"],
                            task_revision=doc.get("task_revision"),
                            outcome=JobOutcome.missed_start_deadline,
                            finished_at=now,
                            expires_at=now
                            + timedelta(seconds=doc["result_expiration_time"]),
                            exception=exc,
                        )
                        events.append(
                            await self._release_job(
                                session,
                                result,
                                scheduler_id,
                                doc["task_id"],
                                doc["schedule_id"],
                                doc["scheduled_fire_time"],
                                doc.get("task_revision"),
                                decrement_running_job_count=False,
                            )
                        )
                        continue

                    # Discard the job if its start deadline has passed
                    if job.start_deadline and job.start_deadline < now:
                        result = JobResult.from_job(
                            job,
                            JobOutcome.missed_start_deadline,
                            finished_at=now,
                        )
                        events.append(
                            await self._release_job(
                                session,
                                result,
                                scheduler_id,
                                job.task_id,
                                job.schedule_id,
                                job.scheduled_fire_time,
                                job.task_revision,
                                decrement_running_job_count=False,
                            )
                        )
                        continue

                    # Try to increment the running jobs count on the bound task
                    # revision
                    slot_filters: list[dict[str, Any]]
                    if job.task_revision is None:
                        # Legacy jobs without a bound revision use the current task
                        # definition
                        slot_filters = [
                            {"_id": job.task_id},
                            {
                                "$or": [
                                    {"max_running_jobs": None},
                                    {
                                        "$expr": {
                                            "$gt": [
                                                "$max_running_jobs",
                                                "$running_jobs",
                                            ]
                                        }
                                    },
                                ]
                            },
                        ]
                        update_task_result = await self._tasks.update_one(
                            {"$and": slot_filters},
                            {"$inc": {"running_jobs": 1}},
                            session=session,
                        )
                    else:
                        update_task_result = await self._task_revisions.update_one(
                            {
                                "task_id": job.task_id,
                                "revision": job.task_revision,
                                "$or": [
                                    {"max_running_jobs": None},
                                    {
                                        "$expr": {
                                            "$gt": [
                                                "$max_running_jobs",
                                                "$running_jobs",
                                            ]
                                        }
                                    },
                                ],
                            },
                            {"$inc": {"running_jobs": 1}},
                            session=session,
                        )
                        if update_task_result.matched_count:
                            # Mirror the increment on the current revision's task row
                            await self._tasks.update_one(
                                {
                                    "_id": job.task_id,
                                    "revision": job.task_revision,
                                },
                                {"$inc": {"running_jobs": 1}},
                                session=session,
                            )

                    if not update_task_result.matched_count:
                        self._logger.debug(
                            "Skipping job %s because task %r revision %r has the "
                            "maximum number of jobs already running",
                            job.id,
                            job.task_id,
                            job.task_revision,
                        )
                        skipped_job_ids.append(job.id)
                        continue

                    job.acquired_by = scheduler_id
                    job.acquired_until = now + lease_duration
                    acquired_jobs.append(job)
                    events.append(JobAcquired.from_job(job, scheduler_id=scheduler_id))

                # Release jobs skipped due to max job slots being reached
                if skipped_job_ids:
                    await self._jobs.update_many(
                        {
                            "_id": {"$in": skipped_job_ids},
                            "acquired_by": scheduler_id,
                        },
                        {
                            "$unset": {
                                "acquired_by": True,
                                "acquired_until": True,
                                "acquired_until_utcoffset": True,
                            },
                        },
                    )

        # Publish the appropriate events
        for event in events:
            await self._event_broker.publish(event)

        return acquired_jobs

    async def _release_job(
        self,
        session: AsyncClientSession,
        result: JobResult,
        scheduler_id: str,
        task_id: str,
        schedule_id: str | None = None,
        scheduled_fire_time: datetime | None = None,
        task_revision: int | None = None,
        *,
        decrement_running_job_count: bool = True,
    ) -> JobReleased:
        # Record the job result
        if result.expires_at > result.finished_at:
            document = result.marshal(self.serializer)
            document["_id"] = document.pop("job_id")
            marshal_document(document)
            await self._jobs_results.insert_one(document, session=session)

        # Delete the job
        await self._jobs.delete_one({"_id": result.job_id}, session=session)

        # Decrement the running jobs counter if the job had been successfully acquired
        if decrement_running_job_count:
            if task_revision is not None:
                await self._task_revisions.update_one(
                    {"task_id": task_id, "revision": task_revision},
                    {"$inc": {"running_jobs": -1}},
                    session=session,
                )
                # Mirror the decrement on the current revision's task row
                await self._tasks.update_one(
                    {"_id": task_id, "revision": task_revision},
                    {"$inc": {"running_jobs": -1}},
                    session=session,
                )
            else:
                await self._tasks.find_one_and_update(
                    {"_id": task_id},
                    {"$inc": {"running_jobs": -1}},
                    session=session,
                )

        # Drop the job's reference on the bound task generation. If this was the last
        # bound job and the generation is no longer current, remove it atomically.
        if task_revision is not None:
            deleted = await self._task_revisions.delete_one(
                {
                    **task_revision_filter(task_id, task_revision),
                    "current": False,
                    "job_count": 1,
                },
                session=session,
            )
            if not deleted.deleted_count:
                await self._task_revisions.update_one(
                    {
                        "task_id": task_id,
                        "revision": task_revision,
                        "job_count": {"$gt": 0},
                    },
                    {"$inc": {"job_count": -1}},
                    session=session,
                )

        # Notify other schedulers
        return JobReleased.from_result(
            result,
            scheduler_id,
            task_id,
            schedule_id,
            scheduled_fire_time,
            task_revision,
        )

    async def release_job(self, scheduler_id: str, job: Job, result: JobResult) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self._client.start_session() as session:
                    event = await self._release_job(
                        session,
                        result,
                        scheduler_id,
                        job.task_id,
                        job.schedule_id,
                        job.scheduled_fire_time,
                        job.task_revision,
                    )

                    # Notify other schedulers
                    await self._event_broker.publish(event)

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        async for attempt in self._retry():
            with attempt:
                document = await self._jobs_results.find_one_and_delete({"_id": job_id})

        if document:
            document["job_id"] = document.pop("_id")
            unmarshal_timestamps(document)
            return JobResult.unmarshal(self.serializer, document)
        else:
            return None

    async def extend_acquired_schedule_leases(
        self, scheduler_id: str, schedule_ids: set[str], duration: timedelta
    ) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self._client.start_session() as session:
                    new_acquired_until = (
                        datetime.now(timezone.utc) + duration
                    ).timestamp()
                    await self._schedules.update_many(
                        filter={
                            "acquired_by": scheduler_id,
                            "_id": {"$in": list(schedule_ids)},
                        },
                        update={"$set": {"acquired_until": new_acquired_until}},
                        session=session,
                    )

    async def extend_acquired_job_leases(
        self, scheduler_id: str, job_ids: set[UUID], duration: timedelta
    ) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self._client.start_session() as session:
                    new_acquired_until = (
                        datetime.now(timezone.utc) + duration
                    ).timestamp()
                    await self._jobs.update_many(
                        filter={
                            "acquired_by": scheduler_id,
                            "_id": {"$in": list(job_ids)},
                        },
                        update={"$set": {"acquired_until": new_acquired_until}},
                        session=session,
                    )

    async def reap_abandoned_jobs(self, scheduler_id: str) -> None:
        async for attempt in self._retry():
            events: list[JobReleased] = []
            with attempt:
                async with (
                    self._client.start_session() as session,
                    self._jobs.find(
                        filter={"acquired_by": scheduler_id},
                        sort=[("created_at", ASCENDING)],
                    ) as cursor,
                ):
                    async for doc in cursor:
                        doc["id"] = doc.pop("_id")
                        unmarshal_timestamps(doc)
                        job = Job.unmarshal(
                            self.serializer, {**doc, "args": (), "kwargs": {}}
                        )
                        result = JobResult.from_job(job, JobOutcome.abandoned)
                        event = await self._release_job(
                            session,
                            result,
                            scheduler_id,
                            job.task_id,
                            job.schedule_id,
                            job.scheduled_fire_time,
                            job.task_revision,
                        )
                        events.append(event)

            for event in events:
                await self._event_broker.publish(event)

    async def cleanup(self) -> None:
        events: list[JobReleased | ScheduleRemoved | ScheduleUpdated] = []
        async for attempt in self._retry():
            with attempt:
                async with self._client.start_session() as session:
                    # Purge expired job results
                    now = datetime.now(timezone.utc)
                    await self._jobs_results.delete_many(
                        {"expires_at": {"$lte": now.timestamp()}}, session=session
                    )

                    # Find finished schedules
                    async with self._schedules.find(
                        {"next_fire_time": None},
                        projection=["_id", "task_id"],
                        session=session,
                    ) as cursor:
                        if finished_schedules := {
                            item["_id"]: item["task_id"] async for item in cursor
                        }:
                            # Find distinct schedule IDs of jobs associated with these
                            # schedules
                            for schedule_id in await self._jobs.distinct(
                                "schedule_id",
                                {"schedule_id": {"$in": list(finished_schedules)}},
                                session=session,
                            ):
                                finished_schedules.pop(schedule_id)

                    # Finish any jobs whose leases have expired
                    filters = {"acquired_until": {"$lt": now.timestamp()}}
                    async with self._jobs.find(
                        filters,
                        projection=[
                            "_id",
                            "acquired_by",
                            "task_id",
                            "task_revision",
                            "schedule_id",
                            "scheduled_fire_time",
                            "scheduled_fire_time_utcoffset",
                            "result_expiration_time",
                        ],
                    ) as cursor:
                        async for doc in cursor:
                            unmarshal_timestamps(doc)
                            result = JobResult(
                                job_id=doc["_id"],
                                task_revision=doc.get("task_revision"),
                                outcome=JobOutcome.abandoned,
                                finished_at=now,
                                expires_at=now
                                + timedelta(seconds=doc["result_expiration_time"]),
                            )
                            events.append(
                                await self._release_job(
                                    session,
                                    result,
                                    doc["acquired_by"],
                                    doc["task_id"],
                                    doc["schedule_id"],
                                    doc["scheduled_fire_time"],
                                    doc.get("task_revision"),
                                )
                            )

                    # Release any schedules whose leases have expired
                    expired_schedule_ids: list[str] = []
                    async with self._schedules.find(
                        {"acquired_until": {"$lt": now.timestamp()}},
                        projection=[
                            "_id",
                            "task_id",
                            "next_fire_time",
                            "next_fire_time_utcoffset",
                        ],
                        session=session,
                    ) as cursor:
                        async for doc in cursor:
                            unmarshal_timestamps(doc)
                            expired_schedule_ids.append(doc["_id"])
                            events.append(
                                ScheduleUpdated(
                                    schedule_id=doc["_id"],
                                    task_id=doc["task_id"],
                                    next_fire_time=doc["next_fire_time"],
                                )
                            )

                    if expired_schedule_ids:
                        await self._schedules.update_many(
                            {"_id": {"$in": expired_schedule_ids}},
                            {
                                "$unset": {
                                    "acquired_by": True,
                                    "acquired_until": True,
                                    "acquired_until_utcoffset": True,
                                }
                            },
                            session=session,
                        )

                    # Delete finished schedules that not having any associated jobs
                    if finished_schedules:
                        await self._schedules.delete_many(
                            {"_id": {"$in": list(finished_schedules)}},
                            session=session,
                        )
                        for schedule_id, task_id in finished_schedules.items():
                            events.append(
                                ScheduleRemoved(
                                    schedule_id=schedule_id,
                                    task_id=task_id,
                                    finished=True,
                                )
                            )

                    # Purge task configuration generations that are no longer current
                    # and no longer referenced by any queued or running jobs
                    await self._task_revisions.delete_many(
                        {"current": False, "job_count": 0},
                        session=session,
                    )

        # Publish any events produced from the operations
        for event in events:
            await self._event_broker.publish(event)

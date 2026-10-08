from __future__ import annotations

from bisect import bisect_left, bisect_right, insort_right
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import MAXYEAR, datetime, timedelta, timezone
from functools import partial
from uuid import UUID

import attrs

from .. import JobOutcome
from .._enums import ConflictPolicy
from .._events import (
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
from .._exceptions import ConflictingIdError, TaskLookupError
from .._structures import Job, JobResult, Schedule, ScheduleResult, Task
from .._utils import create_repr
from .base import BaseDataStore

max_datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


@attrs.define(eq=False, repr=False)
class MemoryDataStore(BaseDataStore):
    """
    Stores scheduler data in memory, without serializing it.

    Can be shared between multiple schedulers within the same event loop.
    """

    _tasks: dict[str, Task] = attrs.Factory(dict)
    _task_revisions: dict[tuple[str, int], Task] = attrs.Factory(dict)
    _schedules: list[Schedule] = attrs.Factory(list)
    _schedules_by_id: dict[str, Schedule] = attrs.Factory(dict)
    _schedules_by_task_id: dict[str, set[Schedule]] = attrs.Factory(
        partial(defaultdict, set)
    )
    _jobs_by_id: dict[UUID, Job] = attrs.Factory(dict)
    _jobs_by_task_id: dict[str, set[Job]] = attrs.Factory(partial(defaultdict, set))
    _jobs_by_task_revision: dict[tuple[str, int | None], set[Job]] = attrs.Factory(
        partial(defaultdict, set)
    )
    _jobs_by_schedule_id: dict[str, set[Job]] = attrs.Factory(partial(defaultdict, set))
    _job_results: dict[UUID, JobResult] = attrs.Factory(dict)

    def __repr__(self) -> str:
        return create_repr(self)

    @staticmethod
    def _task_configs_equal(task: Task, other: Task) -> bool:
        return (
            task.func == other.func
            and task.job_executor == other.job_executor
            and task.max_running_jobs == other.max_running_jobs
            and task.misfire_grace_time == other.misfire_grace_time
            and task.metadata == other.metadata
        )

    def _get_job_task(self, job: Job) -> Task | None:
        """Return the task configuration generation the job is bound to."""
        if job.task_revision is not None:
            return self._task_revisions.get((job.task_id, job.task_revision))

        return self._tasks.get(job.task_id)

    def _prune_task_revision(self, task_id: str, revision: int | None) -> None:
        """Remove an old task configuration generation if nothing references it."""
        if revision is None:
            return

        current = self._tasks.get(task_id)
        if current is not None and current.revision == revision:
            # This is the current revision of an existing task; always retain it
            return

        if self._jobs_by_task_revision.get((task_id, revision)):
            # Jobs are still queued or running against this revision
            return

        self._task_revisions.pop((task_id, revision), None)

    def _find_schedule_index(self, schedule: Schedule) -> int:
        left_index = bisect_left(self._schedules, schedule)
        right_index = bisect_right(self._schedules, schedule)
        return self._schedules.index(schedule, left_index, right_index + 1)

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        if ids is None:
            return self._schedules.copy()

        return [
            schedule
            for schedule in self._schedules
            if ids is None or schedule.id in ids
        ]

    async def add_task(self, task: Task) -> Task:
        old = self._tasks.get(task.id)
        if old is None:
            stored = attrs.evolve(task, revision=1, running_jobs=0)
            self._tasks[task.id] = stored
            self._task_revisions[(task.id, 1)] = stored
            await self._event_broker.publish(TaskAdded(task_id=task.id))
            return stored

        # Only create a new configuration generation if the configuration actually
        # changed; otherwise keep the current revision and its job accounting
        if self._task_configs_equal(task, old):
            stored = attrs.evolve(
                task, revision=old.revision, running_jobs=old.running_jobs
            )
        else:
            stored = attrs.evolve(task, revision=old.revision + 1, running_jobs=0)

        self._tasks[task.id] = stored
        self._task_revisions[(task.id, stored.revision)] = stored
        await self._event_broker.publish(TaskUpdated(task_id=task.id))
        return stored

    async def remove_task(self, task_id: str) -> None:
        if task_id not in self._tasks:
            raise TaskLookupError(task_id)

        del self._tasks[task_id]

        # Any task configuration generations still referenced by queued or running
        # jobs are deliberately retained (they are cleaned up once the last
        # referencing job has finished)
        revision_keys = [
            key for key in self._task_revisions if key[0] == task_id
        ]
        for revision_task_id, revision_number in revision_keys:
            self._prune_task_revision(revision_task_id, revision_number)

        await self._event_broker.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str, revision: int | None = None) -> Task:
        if revision is None:
            task = self._tasks.get(task_id)
        else:
            task = self._task_revisions.get((task_id, revision))

        if task is None:
            raise TaskLookupError(task_id)

        return task

    async def get_tasks(self) -> list[Task]:
        return sorted(self._tasks.values())

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        old_schedule = self._schedules_by_id.get(schedule.id)
        if old_schedule is not None:
            if conflict_policy is ConflictPolicy.do_nothing:
                return
            elif conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id)

            index = self._find_schedule_index(old_schedule)
            del self._schedules[index]
            self._schedules_by_task_id[old_schedule.task_id].remove(old_schedule)

        self._schedules_by_id[schedule.id] = schedule
        self._schedules_by_task_id[schedule.task_id].add(schedule)
        insort_right(self._schedules, schedule)

        event: ScheduleUpdated | ScheduleAdded
        if old_schedule is not None:
            event = ScheduleUpdated(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )
        else:
            event = ScheduleAdded(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )

        await self._event_broker.publish(event)

    async def remove_schedules(
        self, ids: Iterable[str], *, finished: bool = False
    ) -> None:
        for schedule_id in ids:
            schedule = self._schedules_by_id.pop(schedule_id, None)
            if schedule:
                self._schedules.remove(schedule)
                event = ScheduleRemoved(
                    schedule_id=schedule.id,
                    task_id=schedule.task_id,
                    finished=finished,
                )
                await self._event_broker.publish(event)

    async def acquire_schedules(
        self, scheduler_id: str, lease_duration: timedelta, limit: int
    ) -> list[Schedule]:
        now = datetime.now(timezone.utc)
        acquired_until = now + lease_duration
        schedules: list[Schedule] = []
        for schedule in self._schedules:
            if schedule.next_fire_time is None or schedule.next_fire_time > now:
                # The schedule is either exhausted or not yet due. There will be no
                # schedules that are due after this one, so we can stop here.
                break
            elif schedule.paused:
                # The schedule is paused
                continue
            elif schedule.acquired_until is not None:
                if (
                    schedule.acquired_by != scheduler_id
                    and now <= schedule.acquired_until
                ):
                    # The schedule has been acquired by another scheduler and the
                    # timeout has not expired yet
                    continue

            schedules.append(schedule)
            schedule.acquired_by = scheduler_id
            schedule.acquired_until = acquired_until
            if len(schedules) == limit:
                break

        return schedules

    async def release_schedules(
        self, scheduler_id: str, results: Sequence[ScheduleResult]
    ) -> None:
        # Send update events for schedules
        for result in results:
            # Remove the schedule
            schedule = self._schedules_by_id[result.schedule_id]
            index = self._find_schedule_index(schedule)
            del self._schedules[index]

            # Re-add the schedule to its new position
            schedule.last_fire_time = result.last_fire_time
            schedule.next_fire_time = result.next_fire_time
            schedule.acquired_by = None
            schedule.acquired_until = None
            insort_right(self._schedules, schedule)
            event = ScheduleUpdated(
                schedule_id=result.schedule_id,
                task_id=schedule.task_id,
                next_fire_time=result.next_fire_time,
            )
            await self._event_broker.publish(event)

    async def get_next_schedule_run_time(self) -> datetime | None:
        return self._schedules[0].next_fire_time if self._schedules else None

    async def add_job(self, job: Job) -> None:
        # Bind the job to the current task configuration generation (if not already
        # bound to a specific one)
        if job.task_revision is None:
            task = self._tasks.get(job.task_id)
            if task is not None:
                job.task_revision = task.revision

        self._jobs_by_id[job.id] = job
        self._jobs_by_task_id[job.task_id].add(job)
        self._jobs_by_task_revision[(job.task_id, job.task_revision)].add(job)
        if job.schedule_id is not None:
            self._jobs_by_schedule_id[job.schedule_id].add(job)

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            task_revision=job.task_revision,
            schedule_id=job.schedule_id,
        )
        await self._event_broker.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        if ids is not None:
            ids = frozenset(ids)

        if ids is None:
            return list(self._jobs_by_id.values())

        return [
            job for job in self._jobs_by_id.values() if ids is None or job.id in ids
        ]

    async def acquire_jobs(
        self, scheduler_id: str, lease_duration: timedelta, limit: int | None = None
    ) -> list[Job]:
        now = datetime.now(timezone.utc)
        acquired_until = now + lease_duration
        jobs: list[Job] = []
        job_results: dict[Job, JobResult] = {}
        for job in self._jobs_by_id.values():
            task = self._get_job_task(job)

            # If neither the bound task revision nor a current task definition
            # exists, the job can never be run
            if task is None:
                job_results[job] = JobResult(
                    job_id=job.id,
                    task_revision=job.task_revision,
                    outcome=JobOutcome.missed_start_deadline,
                    finished_at=now,
                    expires_at=now + job.result_expiration_time,
                )
                continue

            # Skip already acquired jobs (unless the acquisition lock has expired)
            if job.acquired_until is not None:
                if job.acquired_until >= now:
                    continue
                else:
                    task.running_jobs -= 1

            # Discard the job if its start deadline has passed
            if job.start_deadline and job.start_deadline < now:
                job_results[job] = JobResult(
                    job_id=job.id,
                    task_revision=job.task_revision,
                    outcome=JobOutcome.missed_start_deadline,
                    finished_at=now,
                    expires_at=now + job.result_expiration_time,
                )
                continue

            # Skip the job if no more slots are available
            if (
                task.max_running_jobs is not None
                and task.running_jobs >= task.max_running_jobs
            ):
                self._logger.debug(
                    "Skipping job %s because task %r has the maximum number of %d jobs "
                    "already running",
                    job.id,
                    job.task_id,
                    task.running_jobs,
                )
                continue

            # Mark the job as acquired by this worker
            jobs.append(job)
            job.acquired_by = scheduler_id
            job.acquired_until = acquired_until

            # Increment the number of running jobs for this task
            task.running_jobs += 1

            # Exit the loop if enough jobs have been acquired
            if len(jobs) == limit:
                break

        # Publish the appropriate events
        for job in jobs:
            await self._event_broker.publish(
                JobAcquired.from_job(job, scheduler_id=scheduler_id)
            )

        # Discard the jobs that could not start
        for job, result in job_results.items():
            await self.release_job(scheduler_id, job, result)

        return jobs

    async def release_job(self, scheduler_id: str, job: Job, result: JobResult) -> None:
        # Record the job result
        if result.expires_at > result.finished_at:
            self._job_results[result.job_id] = result

        # Decrement the number of running jobs on the bound task revision
        if job.acquired_by:
            task = self._get_job_task(job)
            if task is not None:
                task.running_jobs -= 1

        # Delete the job
        job = self._jobs_by_id.pop(result.job_id)

        # Remove the job from the jobs belonging to its task
        task_jobs = self._jobs_by_task_id[job.task_id]
        task_jobs.remove(job)
        if not task_jobs:
            del self._jobs_by_task_id[job.task_id]

        # Remove the job from the jobs belonging to its task revision
        revision_jobs = self._jobs_by_task_revision[(job.task_id, job.task_revision)]
        revision_jobs.remove(job)
        if not revision_jobs:
            del self._jobs_by_task_revision[(job.task_id, job.task_revision)]

        # If this was a scheduled job, remove the job from the set of jobs belonging to
        # this schedule
        if job.schedule_id:
            schedule_jobs = self._jobs_by_schedule_id[job.schedule_id]
            schedule_jobs.remove(job)
            if not schedule_jobs:
                del self._jobs_by_schedule_id[job.schedule_id]

        # Remove the bound task revision if it is no longer needed
        self._prune_task_revision(job.task_id, job.task_revision)

        # Notify other schedulers
        await self._event_broker.publish(
            JobReleased.from_result(
                result,
                scheduler_id,
                job.task_id,
                job.schedule_id,
                job.scheduled_fire_time,
                job.task_revision,
            )
        )

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        return self._job_results.pop(job_id, None)

    async def extend_acquired_schedule_leases(
        self, scheduler_id: str, schedule_ids: set[str], duration: timedelta
    ) -> None:
        acquired_until = datetime.now(timezone.utc) + duration
        for schedule in self._schedules:
            if schedule.acquired_by == scheduler_id and schedule.id in schedule_ids:
                schedule.acquired_until = acquired_until

    async def extend_acquired_job_leases(
        self, scheduler_id: str, job_ids: set[UUID], duration: timedelta
    ) -> None:
        acquired_until = datetime.now(timezone.utc) + duration
        for job in self._jobs_by_id.values():
            if job.acquired_by == scheduler_id and job.id in job_ids:
                job.acquired_until = acquired_until

    async def reap_abandoned_jobs(self, scheduler_id: str) -> None:
        now = datetime.now(timezone.utc)
        for job in list(self._jobs_by_id.values()):
            if job.acquired_by == scheduler_id:
                result = JobResult.from_job(
                    job=job, outcome=JobOutcome.abandoned, finished_at=now
                )
                await self.release_job(job.acquired_by, job, result)

    async def cleanup(self) -> None:
        # Clean up expired job results
        now = datetime.now(timezone.utc)
        expired_job_ids = [
            result.job_id
            for result in self._job_results.values()
            if result.expires_at <= now
        ]
        for job_id in expired_job_ids:
            del self._job_results[job_id]

        # Finish any jobs whose leases have expired
        expired_jobs = [
            job
            for job in self._jobs_by_id.values()
            if job.acquired_until is not None and job.acquired_until < now
        ]
        for job in expired_jobs:
            result = JobResult.from_job(
                job=job, outcome=JobOutcome.abandoned, finished_at=now
            )
            assert job.acquired_by is not None
            await self.release_job(job.acquired_by, job, result)

        # Release any schedules whose leases have expired
        for schedule in self._schedules:
            if schedule.acquired_until is not None and schedule.acquired_until < now:
                schedule.acquired_by = None
                schedule.acquired_until = None
                event = ScheduleUpdated(
                    schedule_id=schedule.id,
                    task_id=schedule.task_id,
                    next_fire_time=schedule.next_fire_time,
                )
                await self._event_broker.publish(event)

        # Clean up finished schedules that have no running jobs
        finished_schedule_ids = [
            schedule_id
            for schedule_id, schedule in self._schedules_by_id.items()
            if schedule.next_fire_time is None
            and schedule_id not in self._jobs_by_schedule_id
        ]
        await self.remove_schedules(finished_schedule_ids, finished=True)

        # Purge task configuration generations that are no longer current and are not
        # referenced by any queued or running jobs
        for task_id, revision in list(self._task_revisions):
            self._prune_task_revision(task_id, revision)

from __future__ import annotations

import sys
from base64 import b64encode
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from logging import Logger

import anyio
import pytest
from _pytest.logging import LogCaptureFixture
from anyio import (
    CancelScope,
    Event as AnyIOEvent,
    create_memory_object_stream,
    fail_after,
)

from apscheduler import Event, EventBrokerReconnected, ScheduleAdded
from apscheduler.abc import EventBroker
from apscheduler.eventbrokers import base as broker_base
from apscheduler.eventbrokers.local import LocalEventBroker
from apscheduler.serializers.json import JSONSerializer

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    UTC = timezone.utc

pytestmark = pytest.mark.anyio


class DummyExternalEventBroker(broker_base.BaseExternalEventBroker):
    """An external broker implementation that performs no actual I/O."""

    async def publish(self, event: Event) -> None:
        pass


async def wait_for(predicate, timeout: float = 3) -> None:  # noqa: ASYNC109
    with fail_after(timeout):
        while not predicate():  # noqa: ASYNC110
            await anyio.sleep(0.01)


async def test_publish_subscribe(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream[Event](2)
    with send, receive:
        event_broker.subscribe(send.send)
        event_broker.subscribe(send.send_nowait)
        event = ScheduleAdded(
            schedule_id="schedule1",
            task_id="task1",
            next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
        )
        await event_broker.publish(event)

        with fail_after(3):
            event1 = await receive.receive()
            event2 = await receive.receive()

    assert event1 == event2
    assert isinstance(event1, ScheduleAdded)
    assert isinstance(event1.timestamp, datetime)
    assert event1.schedule_id == "schedule1"
    assert event1.task_id == "task1"
    assert event1.next_fire_time == datetime(2021, 9, 11, 12, 31, 56, 254867, UTC)


async def test_subscribe_one_shot(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream[Event](2)
    with send, receive:
        event_broker.subscribe(send.send, one_shot=True)
        event = ScheduleAdded(
            schedule_id="schedule1",
            task_id="task1",
            next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
        )
        await event_broker.publish(event)
        event = ScheduleAdded(
            schedule_id="schedule2",
            task_id="task1",
            next_fire_time=datetime(2021, 9, 12, 8, 42, 11, 968481, UTC),
        )
        await event_broker.publish(event)

        with fail_after(3):
            received_event = await receive.receive()

        with pytest.raises(TimeoutError), fail_after(0.1):
            await receive.receive()

    assert isinstance(received_event, ScheduleAdded)
    assert received_event.schedule_id == "schedule1"
    assert received_event.task_id == "task1"


async def test_unsubscribe(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream[Event]()
    with send, receive:
        subscription = event_broker.subscribe(send.send)
        await event_broker.publish(Event())
        with fail_after(3):
            await receive.receive()

        subscription.unsubscribe()
        await event_broker.publish(Event())
        with pytest.raises(TimeoutError), fail_after(0.1):
            await receive.receive()


async def test_publish_no_subscribers(
    event_broker: EventBroker, caplog: LogCaptureFixture
) -> None:
    await event_broker.publish(Event())
    assert not caplog.text


async def test_publish_exception(
    event_broker: EventBroker, caplog: LogCaptureFixture
) -> None:
    def bad_subscriber(event: Event) -> None:
        raise Exception("foo")

    timestamp = datetime.now(UTC)
    send, receive = create_memory_object_stream[Event]()
    with send, receive:
        event_broker.subscribe(bad_subscriber)
        event_broker.subscribe(send.send)
        await event_broker.publish(Event(timestamp=timestamp))

        received_event = await receive.receive()
        assert received_event.timestamp == timestamp
        assert "Error delivering Event" in caplog.text


async def test_cancel_start(raw_event_broker: EventBroker, logger: Logger) -> None:
    with CancelScope() as scope:
        scope.cancel()
        async with AsyncExitStack() as exit_stack:
            await raw_event_broker.start(exit_stack, logger)


async def test_cancel_stop(raw_event_broker: EventBroker, logger: Logger) -> None:
    with CancelScope() as scope:
        async with AsyncExitStack() as exit_stack:
            await raw_event_broker.start(exit_stack, logger)
            scope.cancel()


def test_asyncpg_broker_from_async_engine() -> None:
    pytest.importorskip("asyncpg", reason="asyncpg is not installed")
    from sqlalchemy import URL
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker

    url = URL(
        "postgresql+asyncpg",
        "myuser",
        "c /%@",
        "localhost",
        7654,
        "dbname",
        {"opt1": "foo", "opt2": "bar"},
    )
    engine = create_async_engine(url)
    broker = AsyncpgEventBroker.from_async_sqla_engine(engine)
    assert isinstance(broker, AsyncpgEventBroker)
    assert broker.dsn == (
        "postgresql://myuser:c %2F%25%40@localhost:7654/dbname?opt1=foo&opt2=bar"
    )


def test_psycopg_broker_from_async_engine() -> None:
    pytest.importorskip(
        "psycopg", exc_type=ImportError, reason="psycopg is not installed"
    )
    from sqlalchemy import URL
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.eventbrokers.psycopg import PsycopgEventBroker

    url = URL(
        "postgresql+psycopg",
        "myuser",
        "c /%@",
        "localhost",
        7654,
        "dbname",
        {"opt1": "foo", "opt2": "bar"},
    )
    engine = create_async_engine(url)
    broker = PsycopgEventBroker.from_async_sqla_engine(engine)
    assert isinstance(broker, PsycopgEventBroker)
    assert broker.conninfo == (
        "postgresql://myuser:c %2F%25%40@localhost:7654/dbname?opt1=foo&opt2=bar"
    )


async def test_local_delivery_is_ordered(logger: Logger) -> None:
    """Each subscriber must receive events in the order they were published in."""
    received: list[ScheduleAdded] = []
    broker = LocalEventBroker()
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        broker.subscribe(received.append)
        for i in range(20):
            await broker.publish(
                ScheduleAdded(
                    schedule_id=f"schedule{i}",
                    task_id="task1",
                    next_fire_time=None,
                )
            )

        await wait_for(lambda: len(received) == 20)

    assert [event.schedule_id for event in received] == [
        f"schedule{i}" for i in range(20)
    ]


async def test_slow_subscriber_does_not_leak(
    logger: Logger, caplog: LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A subscriber that cannot keep up gets a bounded backlog and dropped events with
    a warning, instead of growing memory without bound, and stays responsive
    afterwards.

    """
    monkeypatch.setattr(broker_base, "DEFAULT_SUBSCRIPTION_BACKLOG", 2)
    blocker = AnyIOEvent()
    received: list[Event] = []

    async def slow_callback(event: Event) -> None:
        await blocker.wait()
        received.append(event)

    broker = LocalEventBroker()
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        broker.subscribe(slow_callback)

        with caplog.at_level("WARNING"):
            for _ in range(10):
                await broker.publish(Event())

        assert "too slow" in caplog.text

        # Unblock the subscriber. Its bounded backlog (2 events) is drained, and
        # afterwards it receives new events normally.
        blocker.set()
        await wait_for(lambda: len(received) == 2)
        await broker.publish(Event())
        await broker.publish(Event())
        await wait_for(lambda: len(received) == 4)


def test_unsubscribe_after_stop_is_noop(logger: Logger) -> None:
    broker = LocalEventBroker()

    async def scenario() -> None:
        async with AsyncExitStack() as exit_stack:
            await broker.start(exit_stack, logger)
            return broker.subscribe(lambda event: None)

    subscription = anyio.run(scenario)

    # Unsubscribing after the broker has stopped must not raise
    subscription.unsubscribe()


def test_notification_envelope_carries_origin_and_sequence() -> None:
    broker = DummyExternalEventBroker(serializer=JSONSerializer())
    event = ScheduleAdded(
        schedule_id="schedule1",
        task_id="task1",
        next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
    )
    payload = broker.generate_notification(event)
    origin, seq, restored = broker.decode_notification(payload)
    assert origin == broker._origin
    assert seq == 1
    assert restored == event

    payload2 = broker.generate_notification(event)
    _, seq2, restored2 = broker.decode_notification(payload2)
    assert seq2 == 2
    assert restored2 == event

    # Same for the text based payload (PostgreSQL NOTIFY)
    payload3 = broker.generate_notification_str(event)
    origin3, seq3, restored3 = broker.decode_notification_str(payload3)
    assert origin3 == broker._origin
    assert seq3 == 3
    assert restored3 == event


def test_legacy_notification_without_envelope_is_accepted() -> None:
    broker = DummyExternalEventBroker(serializer=JSONSerializer())
    event = ScheduleAdded(
        schedule_id="schedule1",
        task_id="task1",
        next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
    )
    serialized = broker.serializer.serialize(event.marshal())
    origin, seq, restored = broker.decode_notification(
        b"ScheduleAdded " + serialized
    )
    assert origin is None
    assert seq is None
    assert restored == event

    origin, seq, restored = broker.decode_notification_str(
        "ScheduleAdded " + b64encode(serialized).decode("ascii")
    )
    assert origin is None
    assert seq is None
    assert restored == event


async def test_looped_back_notification_from_self_is_suppressed(
    logger: Logger,
) -> None:
    received: list[Event] = []
    broker = DummyExternalEventBroker(serializer=JSONSerializer())
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        broker.subscribe(received.append)
        event = ScheduleAdded(
            schedule_id="schedule1", task_id="task1", next_fire_time=None
        )
        await broker.dispatch_remote_event(broker._origin, 1, event)
        await anyio.sleep(0.1)

    assert received == []


async def test_duplicate_notifications_are_suppressed(logger: Logger) -> None:
    received: list[ScheduleAdded] = []
    broker = DummyExternalEventBroker(serializer=JSONSerializer())
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        broker.subscribe(received.append)

        remote_origin = "0" * 32
        await broker.dispatch_remote_event(
            remote_origin,
            1,
            ScheduleAdded(schedule_id="a", task_id="task1", next_fire_time=None),
        )
        # An exact redelivery must not be presented as new state
        await broker.dispatch_remote_event(
            remote_origin,
            1,
            ScheduleAdded(schedule_id="a-dup", task_id="task1", next_fire_time=None),
        )
        await wait_for(lambda: len(received) == 1)
        await anyio.sleep(0.1)
        assert len(received) == 1
        assert received[0].schedule_id == "a"


async def test_sequence_gap_signals_resynchronization_boundary(
    logger: Logger, caplog: LogCaptureFixture
) -> None:
    received: list[Event] = []
    broker = DummyExternalEventBroker(serializer=JSONSerializer())
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        broker.subscribe(received.append)

        remote_origin = "1" * 32
        await broker.dispatch_remote_event(
            remote_origin,
            1,
            ScheduleAdded(schedule_id="a", task_id="t", next_fire_time=None),
        )
        with caplog.at_level("WARNING"):
            await broker.dispatch_remote_event(
                remote_origin,
                3,
                ScheduleAdded(schedule_id="c", task_id="t", next_fire_time=None),
            )

        await wait_for(lambda: len(received) == 3)
        assert isinstance(received[0], ScheduleAdded)
        assert received[0].schedule_id == "a"
        assert isinstance(received[1], EventBrokerReconnected)
        assert isinstance(received[2], ScheduleAdded)
        assert received[2].schedule_id == "c"
        assert "gap detected" in caplog.text


async def test_signal_reconnected_delivers_local_boundary(logger: Logger) -> None:
    received: list[Event] = []
    broker = DummyExternalEventBroker(serializer=JSONSerializer())
    async with AsyncExitStack() as exit_stack:
        await broker.start(exit_stack, logger)
        broker.subscribe(received.append)
        await broker.signal_reconnected()
        await wait_for(
            lambda: any(
                isinstance(ev, EventBrokerReconnected) for ev in received
            )
        )



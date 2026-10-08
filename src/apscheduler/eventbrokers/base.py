from __future__ import annotations

from base64 import b64decode, b64encode
from collections import OrderedDict
from collections.abc import Callable, Iterable
from contextlib import AsyncExitStack
from inspect import iscoroutine
from logging import Logger
from typing import Any
from uuid import uuid4

import attrs
from anyio import (
    CapacityLimiter,
    WouldBlock,
    create_memory_object_stream,
    create_task_group,
    to_thread,
)
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from .. import _events
from .._events import Event, EventBrokerReconnected
from .._exceptions import DeserializationError
from .._retry import RetryMixin
from ..abc import EventBroker, Serializer, Subscription
from ..serializers.json import JSONSerializer

#: Maximum number of events buffered per local subscription. If a subscriber cannot
#: keep up with the event rate, new events are dropped (with a warning) instead of
#: being buffered without bound.
DEFAULT_SUBSCRIPTION_BACKLOG = 256

#: Maximum number of remote publisher origins whose last seen sequence number is
#: remembered for duplicate and gap detection.
MAX_TRACKED_ORIGINS = 128

#: Prefix of the ordered, origin-tagged notification envelope used by external event
#: brokers (``aps2 <origin> <sequence> <event type> <payload>``).
ENVELOPE_PREFIX = "aps2"


@attrs.define(eq=False, frozen=True)
class LocalSubscription(Subscription):
    callback: Callable[[Event], Any]
    event_types: set[type[Event]] | None
    one_shot: bool
    is_async: bool
    token: object
    _source: BaseEventBroker
    _send: MemoryObjectSendStream[Event]
    _receive: MemoryObjectReceiveStream[Event]

    def unsubscribe(self) -> None:
        self._source.unsubscribe(self.token)


@attrs.define(kw_only=True)
class BaseEventBroker(EventBroker):
    _logger: Logger = attrs.field(init=False)
    _subscriptions: dict[object, LocalSubscription] = attrs.field(
        init=False, factory=dict
    )
    _task_group: TaskGroup = attrs.field(init=False)
    _thread_limiter: CapacityLimiter = attrs.field(init=False)
    _started: bool = attrs.field(init=False, default=False)

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        self._logger = logger
        self._task_group = await exit_stack.enter_async_context(create_task_group())
        self._thread_limiter = CapacityLimiter(1)
        self._started = True

        # Close every subscription feed on shutdown (before the task group is
        # exited) so that the persistent delivery workers drain and terminate
        # instead of keeping the task group waiting forever
        exit_stack.callback(self._close_subscription_feeds)
        exit_stack.callback(setattr, self, "_started", False)

        # Start delivery workers for subscriptions created before the broker was
        # started
        for subscription in list(self._subscriptions.values()):
            self._start_delivery_worker(subscription)

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        is_async: bool = True,
        one_shot: bool = False,
    ) -> Subscription:
        types = set(event_types) if event_types else None
        send, receive = create_memory_object_stream[Event](
            DEFAULT_SUBSCRIPTION_BACKLOG
        )
        token = object()
        subscription = LocalSubscription(
            callback, types, one_shot, is_async, token, self, send, receive
        )
        self._subscriptions[token] = subscription
        if self._started:
            self._start_delivery_worker(subscription)

        return subscription

    def unsubscribe(self, token: object) -> None:
        subscription = self._subscriptions.pop(token, None)
        if subscription is not None:
            # Closing the send stream ends the delivery worker with an
            # end-of-stream signal, so that no references to the callback are left
            # behind and the worker releases its resources
            subscription._send.close()

    def _close_subscription_feeds(self) -> None:
        for subscription in list(self._subscriptions.values()):
            subscription._send.close()

        self._subscriptions.clear()

    def _start_delivery_worker(self, subscription: LocalSubscription) -> None:
        self._task_group.start_soon(
            self._deliver_events,
            subscription,
            name=f"{self.__class__.__name__} event delivery",
        )

    async def _deliver_events(self, subscription: LocalSubscription) -> None:
        with subscription._receive:
            async for event in subscription._receive:
                await self._deliver_event(subscription, event)

    async def publish_local(self, event: Event) -> None:
        event_type = type(event)
        one_shot_tokens: list[object] = []
        for subscription in self._subscriptions.values():
            if (
                subscription.event_types is None
                or event_type in subscription.event_types
            ):
                try:
                    subscription._send.send_nowait(event)
                except WouldBlock:
                    # The subscriber is too slow to keep up. Drop the event rather
                    # than letting its backlog grow without bound, and let the
                    # subscriber know that it has fallen behind.
                    self._logger.warning(
                        "%s: subscriber %r is too slow to process events; "
                        "dropping a %s event",
                        self.__class__.__name__,
                        subscription.callback,
                        event_type.__name__,
                    )

                if subscription.one_shot:
                    one_shot_tokens.append(subscription.token)

        for token in one_shot_tokens:
            self.unsubscribe(token)

    async def _deliver_event(
        self, subscription: LocalSubscription, event: Event
    ) -> None:
        try:
            if subscription.is_async:
                retval = subscription.callback(event)
                if iscoroutine(retval):
                    await retval
            else:
                await to_thread.run_sync(
                    subscription.callback, event, limiter=self._thread_limiter
                )
        except Exception:
            self._logger.exception(
                "Error delivering %s event", event.__class__.__name__
            )


@attrs.define(kw_only=True)
class BaseExternalEventBroker(BaseEventBroker, RetryMixin):
    """
    Base class for event brokers that use an external service.

    Every notification published by a broker instance is tagged with a random,
    process-local *origin ID* and a monotonically increasing *sequence number*. This
    envelope makes three things identifiable for the receivers:

    * **their own messages** – notifications that loop back to the publisher through
      the external broker are recognized by their origin ID and not delivered again
      (local subscribers already received the event once, deterministically, at
      publish time)
    * **duplicates** – a redelivered notification (broker retry or QoS > 0
      redelivery) with an already seen sequence number is suppressed
    * **gaps and reconnection boundaries** – when a sequence number is missing or
      the listener subscription is re-established, a
      :class:`~apscheduler.EventBrokerReconnected` event is delivered locally so
      that subscribers can re-query the authoritative state

    The envelope is backwards compatible: notifications without one (sent by older
    broker versions) are still accepted.

    :param serializer: the serializer used to (de)serialize events for transport
    """

    serializer: Serializer = attrs.field(factory=JSONSerializer)

    _origin: str = attrs.field(init=False, factory=lambda: uuid4().hex)
    _sequence: int = attrs.field(init=False, default=0)
    _last_seen_sequences: OrderedDict[str, int] = attrs.field(
        init=False, factory=OrderedDict
    )

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def generate_notification(self, event: Event) -> bytes:
        serialized = self.serializer.serialize(event.marshal())
        seq = self._next_sequence()
        header = (
            f"{ENVELOPE_PREFIX} {self._origin} {seq} "
            f"{event.__class__.__name__}"
        ).encode("ascii")
        return header + b" " + serialized

    def generate_notification_str(self, event: Event) -> str:
        serialized = self.serializer.serialize(event.marshal())
        seq = self._next_sequence()
        header = (
            f"{ENVELOPE_PREFIX} {self._origin} {seq} "
            f"{event.__class__.__name__}"
        )
        return header + " " + b64encode(serialized).decode("ascii")

    def _reconstitute_event(self, event_type: str, serialized: bytes) -> Event | None:
        try:
            kwargs = self.serializer.deserialize(serialized)
        except DeserializationError:
            self._logger.exception(
                "Failed to deserialize an event of type %s",
                event_type,
                extra={"serialized": serialized},
            )
            return None

        try:
            event_class = getattr(_events, event_type)
        except AttributeError:
            self._logger.error(
                "Receive notification for a nonexistent event type: %s",
                event_type,
                extra={"serialized": serialized},
            )
            return None

        try:
            return event_class.unmarshal(kwargs)
        except Exception:
            self._logger.exception("Error reconstituting event of type %s", event_type)
            return None

    def reconstitute_event(self, payload: bytes) -> Event | None:
        try:
            event_type_bytes, serialized = payload.split(b" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        event_type = event_type_bytes.decode("ascii", errors="replace")
        return self._reconstitute_event(event_type, serialized)

    def reconstitute_event_str(self, payload: str) -> Event | None:
        try:
            event_type, b64_serialized = payload.split(" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        return self._reconstitute_event(event_type, b64decode(b64_serialized))

    def decode_notification(
        self, payload: bytes
    ) -> tuple[str | None, int | None, Event | None]:
        """
        Decode a binary notification into an ``(origin, sequence, event)`` tuple.

        ``origin`` and ``sequence`` are ``None`` for notifications that do not carry
        the ordering envelope (e.g. sent by older broker versions).

        """
        try:
            marker, remainder = payload.split(b" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None, None, None

        if marker != ENVELOPE_PREFIX.encode("ascii"):
            # Legacy notification without the ordering envelope
            return None, None, self.reconstitute_event(payload)

        try:
            origin_bytes, seq_bytes, body = remainder.split(b" ", 2)
            origin = origin_bytes.decode("ascii")
            seq = int(seq_bytes)
        except ValueError:
            self._logger.error(
                "Received malformatted notification envelope",
                extra={"payload": payload},
            )
            return None, None, None

        return origin, seq, self.reconstitute_event(body)

    def decode_notification_str(
        self, payload: str
    ) -> tuple[str | None, int | None, Event | None]:
        """
        Decode a text notification into an ``(origin, sequence, event)`` tuple.

        ``origin`` and ``sequence`` are ``None`` for notifications that do not carry
        the ordering envelope (e.g. sent by older broker versions).

        """
        try:
            marker, remainder = payload.split(" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None, None, None

        if marker != ENVELOPE_PREFIX:
            # Legacy notification without the ordering envelope
            return None, None, self.reconstitute_event_str(payload)

        try:
            origin, seq_text, body = remainder.split(" ", 2)
            seq = int(seq_text)
        except ValueError:
            self._logger.error(
                "Received malformatted notification envelope",
                extra={"payload": payload},
            )
            return None, None, None

        return origin, seq, self.reconstitute_event_str(body)

    def _remember_sequence(self, origin: str, seq: int) -> None:
        self._last_seen_sequences[origin] = seq
        self._last_seen_sequences.move_to_end(origin)
        while len(self._last_seen_sequences) > MAX_TRACKED_ORIGINS:
            self._last_seen_sequences.popitem(last=False)

    async def dispatch_remote_event(
        self, origin: str | None, seq: int | None, event: Event | None
    ) -> None:
        """
        Deliver an event received from the external broker to local subscribers.

        Suppresses the publisher's own looped-back notifications and redelivered
        duplicates, detects gaps in the sequence and signals a resynchronization
        boundary when events may have been missed.

        """
        if event is None:
            return

        if origin is not None and origin == self._origin:
            self._logger.debug(
                "%s: suppressing a looped-back notification originating from this "
                "broker instance",
                self.__class__.__name__,
            )
            return

        if origin is not None and seq is not None:
            last_seq = self._last_seen_sequences.get(origin)
            if last_seq is not None and seq <= last_seq:
                self._logger.debug(
                    "%s: suppressing a duplicate notification (origin=%s, "
                    "sequence=%d, last=%d)",
                    self.__class__.__name__,
                    origin,
                    seq,
                    last_seq,
                )
                return

            if last_seq is not None and seq > last_seq + 1:
                self._logger.warning(
                    "%s: gap detected in the event sequence of origin %s (last "
                    "seen: %d, received: %d) – some events may have been missed",
                    self.__class__.__name__,
                    origin,
                    last_seq,
                    seq,
                )
                await self.publish_local(EventBrokerReconnected())

            self._remember_sequence(origin, seq)

        await self.publish_local(event)

    async def signal_reconnected(self) -> None:
        """
        Signal local subscribers that the external subscription has been
        (re-)established and they should re-query the state they are interested in,
        as events may have been missed while the connection was down.

        """
        self._logger.info(
            "%s: subscription to the external broker re-established; signaling a "
            "resynchronization boundary",
            self.__class__.__name__,
        )
        await self.publish_local(EventBrokerReconnected())

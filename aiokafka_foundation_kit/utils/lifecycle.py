"""Shared async lifecycle helper for aiokafka clients."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Protocol, TypeVar

from aiokafka.errors import KafkaError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

logger = logging.getLogger(__name__)


class _StartStopClient(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...


_ClientT = TypeVar("_ClientT", bound=_StartStopClient)


@asynccontextmanager
async def managed_kafka_client(
    client: _ClientT,
    *,
    name: str,
    on_started: Callable[[_ClientT], Awaitable[None]] | None = None,
    on_stopped: Callable[[_ClientT], Awaitable[None]] | None = None,
) -> AsyncIterator[_ClientT]:
    """Start ``client``, run ``on_started``, yield it, then stop it.

    Stop errors are caught and logged so the caller's original exception (if
    any) is preserved.
    """
    await client.start()
    logger.info("Kafka %s started", name)
    if on_started is not None:
        await on_started(client)

    try:
        yield client
    finally:
        try:
            await client.stop()
            logger.info("Kafka %s stopped", name)
            if on_stopped is not None:
                await on_stopped(client)
        except KafkaError:
            logger.exception("Error stopping Kafka %s", name)

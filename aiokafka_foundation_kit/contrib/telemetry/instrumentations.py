"""OpenTelemetry instrumentation functions for aiokafka."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from opentelemetry.trace import Span, TracerProvider

    AsyncProduceHook = Callable[[Span, tuple[Any, ...], dict[str, Any]], Awaitable[None]]
    AsyncConsumeHook = Callable[[Span, Any, tuple[Any, ...], dict[str, Any]], Awaitable[None]]


def instrument_aiokafka(
    *,
    tracer_provider: TracerProvider | None = None,
    async_produce_hook: AsyncProduceHook | None = None,
    async_consume_hook: AsyncConsumeHook | None = None,
    **kwargs: Any,
) -> None:
    """Instrument aiokafka clients for OpenTelemetry tracing.

    Automatically traces all aiokafka producer and consumer operations including
    message publishing, consumption, and offset commits.

    Args:
        tracer_provider: Optional tracer provider. Defaults to the global provider.
        async_produce_hook: Async callback executed before sending a message.
            Signature: ``async def hook(span: Span, args, kwargs) -> None``.
            Use it to enrich producer spans with custom attributes.
        async_consume_hook: Async callback executed after consuming a message.
            Signature: ``async def hook(span: Span, record, args, kwargs) -> None``.
            Use it to enrich consumer spans with custom attributes.
        **kwargs: Additional keyword arguments passed to ``AIOKafkaInstrumentor.instrument``.

    Raises:
        ImportError: If ``opentelemetry-instrumentation-aiokafka`` is not installed.

    Examples:
        Basic usage::

            from aiokafka_foundation_kit.contrib.telemetry import instrument_aiokafka

            instrument_aiokafka()

        With custom tracer provider::

            from opentelemetry.sdk.trace import TracerProvider

            provider = TracerProvider()
            instrument_aiokafka(tracer_provider=provider)

        With hooks for span enrichment::

            async def produce_hook(span, args, kwargs):
                if span and span.is_recording():
                    span.set_attribute("app.user_id", "42")

            async def consume_hook(span, record, args, kwargs):
                if span and span.is_recording():
                    span.set_attribute("app.message_id", record.headers.get("id"))

            instrument_aiokafka(
                async_produce_hook=produce_hook,
                async_consume_hook=consume_hook,
            )
    """
    try:
        from opentelemetry.instrumentation.aiokafka import (  # noqa: PLC0415
            AIOKafkaInstrumentor,
        )
    except ImportError as e:
        raise ImportError(
            "opentelemetry-instrumentation-aiokafka not installed. "
            "Install with: pip install 'aiokafka-foundation-kit[telemetry]'"
        ) from e

    overrides: dict[str, Any] = {}
    if tracer_provider is not None:
        overrides["tracer_provider"] = tracer_provider
    if async_produce_hook is not None:
        overrides["async_produce_hook"] = async_produce_hook
    if async_consume_hook is not None:
        overrides["async_consume_hook"] = async_consume_hook

    AIOKafkaInstrumentor().instrument(**kwargs, **overrides)

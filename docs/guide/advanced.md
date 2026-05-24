# Advanced

## Dishka DI integration

[Dishka](https://github.com/reagento/dishka) is a modern async-native DI framework. The
`dishka` extra adds three ready-made providers.

```bash
pip install aiokafka-foundation-kit[models,dishka]
```

### AsyncKafkaProducerProvider

Manages the `AIOKafkaProducer` at `APP` scope (started once, stopped on shutdown).
Automatically creates topics when `settings.auto_create_topics` is `True`.

```python
from dishka import make_async_container
from aiokafka_foundation_kit.contrib.di import AsyncKafkaProducerProvider
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

container = make_async_container(
    AsyncKafkaProducerProvider(),
    # Provide the settings object to the container
)
```

The provider's `get_kafka_producer` method signature:

```python
@provide
async def get_kafka_producer(
    self,
    kafka_settings: ProducerLifecycleSettingsProtocol,
    topics: Sequence[TopicConfigProtocol] | None = None,
) -> AsyncIterator[AIOKafkaProducer]: ...
```

Inject `AIOKafkaProducer` wherever you need to send messages:

```python
from dishka.integrations.fastapi import inject, FromDishka
from aiokafka import AIOKafkaProducer

@router.post("/orders")
@inject
async def create_order(producer: FromDishka[AIOKafkaProducer]) -> dict:
    await producer.send_and_wait("orders", b'{"id": "42"}')
    return {"status": "ok"}
```

### AsyncKafkaConsumerProvider

Same lifecycle pattern for the consumer side.

```python
from aiokafka_foundation_kit.contrib.di import AsyncKafkaConsumerProvider

container = make_async_container(AsyncKafkaConsumerProvider(), ...)
```

Provider signature:

```python
@provide
async def get_kafka_consumer(
    self,
    kafka_settings: ConsumerSettingsProtocol,
    topics: tuple[str, ...] | None = None,
) -> AsyncIterator[AIOKafkaConsumer]: ...
```

### KafkaInfraProvider

Resolves physical topic names (with optional prefix) from a logical catalog. Typically used
together with `AsyncKafkaProducerProvider` so topics are created before the producer starts.

```python
from aiokafka_foundation_kit.contrib.di import KafkaInfraProvider
from aiokafka_foundation_kit.contrib.models import (
    BaseKafkaInfraSettings,
    KafkaTopicSettings,
)

infra_settings = BaseKafkaInfraSettings(
    topic_prefix="prod",
    topic_catalog={
        "orders": KafkaTopicSettings(num_partitions=6, replication_factor=3),
        "payments": KafkaTopicSettings(num_partitions=3, replication_factor=3),
    },
    consumer_subscriptions=["orders"],
)
```

`KafkaInfraProvider` provides:

- `Sequence[TopicConfig]` — physical topic configs with prefix applied, e.g. `prod.orders`.
- `tuple[str, ...]` — resolved consumer subscription topics, e.g. `("prod.orders",)`.

---

## dependency-injector integration

For projects using the classic [dependency-injector](https://python-dependency-injector.ets-labs.org/)
framework, two pre-built containers are available.

```bash
pip install aiokafka-foundation-kit[models,dependency-injector]
```

### KafkaProducerContainer

```python
from dependency_injector.wiring import inject, Provide
from aiokafka_foundation_kit.contrib.dependency_injector import KafkaProducerContainer
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

container = KafkaProducerContainer()
container.kafka_settings.override(settings)

# Wire and use
@inject
async def send_event(
    producer: AIOKafkaProducer = Provide[KafkaProducerContainer.producer],
) -> None:
    await producer.send_and_wait("events", b"data")
```

The container exposes:

| Attribute | Type | Description |
| --- | --- | --- |
| `kafka_settings` | `Dependency` | Inject a `ProducerLifecycleSettingsProtocol` |
| `topics` | `Dependency` | Optional `Sequence[TopicConfigProtocol]` |
| `auto_create_topics` | `Object` | `bool`, defaults to `False` |
| `producer` | `Resource` | `AIOKafkaProducer` with full lifecycle |

### KafkaConsumerContainer

```python
from aiokafka_foundation_kit.contrib.dependency_injector import KafkaConsumerContainer
from aiokafka_foundation_kit.contrib.models import BaseKafkaConsumerSettings

settings = BaseKafkaConsumerSettings(
    bootstrap_servers="localhost:9092",
    group_id="my-group",
)

container = KafkaConsumerContainer()
container.kafka_settings.override(settings)
container.topics.override(["orders", "payments"])
```

| Attribute | Type | Description |
| --- | --- | --- |
| `kafka_settings` | `Dependency` | Inject a `ConsumerSettingsProtocol` |
| `topics` | `Dependency` | Optional `Sequence[str]` of topic names |
| `consumer` | `Resource` | `AIOKafkaConsumer` with full lifecycle |

---

## OpenTelemetry instrumentation

Distributed tracing for Kafka messages via `opentelemetry-instrumentation-aiokafka`.

```bash
pip install aiokafka-foundation-kit[telemetry]
```

Call `instrument_aiokafka` once at application startup, before any producers or consumers
are created:

```python
from opentelemetry.sdk.trace import TracerProvider
from aiokafka_foundation_kit.contrib.telemetry import instrument_aiokafka

tracer_provider = TracerProvider()

instrument_aiokafka(tracer_provider=tracer_provider)
```

### Async hooks for span enrichment

Use `async_produce_hook` / `async_consume_hook` to add custom attributes to spans:

```python
from opentelemetry.trace import Span
from aiokafka import AIOKafkaProducer

async def enrich_produce_span(span: Span, args: tuple, kwargs: dict) -> None:
    span.set_attribute("messaging.system", "kafka")
    span.set_attribute("app.version", "1.2.3")

instrument_aiokafka(
    tracer_provider=tracer_provider,
    async_produce_hook=enrich_produce_span,
)
```

`async_consume_hook` receives `(span, record, args, kwargs)` where `record` is the
`ConsumerRecord` being processed.

---

## Topic prefix handling

`BaseKafkaInfraSettings.topic_prefix` applies a namespace prefix to every topic name.
This is especially useful in multi-tenant or multi-environment setups where topics are shared
across a single Kafka cluster.

```
topic_prefix = "prod-eu"
logical name  = "orders"
physical name = "prod-eu.orders"
```

The prefix field normalises environment-variable quirks automatically — `"None"`, `"null"`,
`""` and `None` all resolve to no prefix:

```python
from aiokafka_foundation_kit.contrib.models import BaseKafkaInfraSettings

# All of these produce topic_prefix=None
BaseKafkaInfraSettings(topic_prefix=None)
BaseKafkaInfraSettings(topic_prefix="None")   # stringified JSON null from Vault
BaseKafkaInfraSettings(topic_prefix="null")
BaseKafkaInfraSettings(topic_prefix="")
```

---

## Low-level lifecycle helper

`managed_kafka_client` is the shared building block behind `producer_lifecycle` and
`consumer_lifecycle`. Use it to wrap any object that has `.start()` / `.stop()` coroutines:

```python
from aiokafka_foundation_kit.utils.lifecycle import managed_kafka_client
from aiokafka import AIOKafkaProducer

producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")

async with managed_kafka_client(producer, name="producer") as p:
    await p.send_and_wait("topic", b"data")
```

Stop errors are caught, logged, and swallowed so the calling coroutine always gets a clean exit.

---

## Building the common config dict

`build_kafka_common_config` translates a `KafkaSettingsProtocol` into the `kwargs` dict
accepted by `AIOKafkaProducer` / `AIOKafkaConsumer`. This is useful when you need to
construct aiokafka clients manually with additional parameters not covered by the helpers:

```python
from aiokafka import AIOKafkaProducer
from aiokafka_foundation_kit import build_kafka_common_config
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

common = build_kafka_common_config(settings)
# {'bootstrap_servers': 'localhost:9092', 'security_protocol': 'PLAINTEXT', ...}

producer = AIOKafkaProducer(**common, max_request_size=10_485_760)
```

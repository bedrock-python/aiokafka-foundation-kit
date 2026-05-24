# Quick start

## Installation

```bash
# Minimal — AIOKafka factories and lifecycle helpers
pip install aiokafka-foundation-kit

# With Pydantic settings models and orjson acceleration
pip install aiokafka-foundation-kit[models,orjson]

# Full stack (adds Dishka, dependency-injector, OpenTelemetry)
pip install aiokafka-foundation-kit[all]
```

**Requires Python 3.11+.**

---

## Producer

### Lifecycle context manager (recommended)

`producer_lifecycle` starts the producer, yields it, and guarantees a clean stop even when an
exception is raised:

```python
from aiokafka_foundation_kit import producer_lifecycle
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

async with producer_lifecycle(settings) as producer:
    await producer.send_and_wait("my-topic", b'{"event": "order_placed"}')
```

### With topic auto-creation

Pass `topics` and `auto_create_topics=True` to ensure all topics exist before the producer starts:

```python
from aiokafka_foundation_kit import producer_lifecycle, TopicConfig
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

topics = [
    TopicConfig(name="orders", num_partitions=6, replication_factor=3),
    TopicConfig(name="payments", num_partitions=3, replication_factor=3),
]

async with producer_lifecycle(settings, topics=topics, auto_create_topics=True) as producer:
    await producer.send_and_wait("orders", b'{"id": "42"}')
```

### Lifecycle hooks

`on_started` / `on_stopped` accept async callables that receive the live producer instance:

```python
from aiokafka import AIOKafkaProducer

async def on_ready(producer: AIOKafkaProducer) -> None:
    print("Producer ready")

async with producer_lifecycle(settings, on_started=on_ready) as producer:
    ...
```

### Low-level factory

When you manage the lifecycle yourself (e.g. inside a custom DI framework):

```python
from aiokafka_foundation_kit import create_async_kafka_producer

producer = create_async_kafka_producer(settings)
await producer.start()
try:
    await producer.send_and_wait("topic", b"data")
finally:
    await producer.stop()
```

---

## Consumer

### Lifecycle context manager (recommended)

```python
from aiokafka_foundation_kit import consumer_lifecycle
from aiokafka_foundation_kit.contrib.models import BaseKafkaConsumerSettings

settings = BaseKafkaConsumerSettings(
    bootstrap_servers="localhost:9092",
    group_id="order-processor",
)

async with consumer_lifecycle(settings, topics=("orders",)) as consumer:
    async for msg in consumer:
        print(f"partition={msg.partition} offset={msg.offset}: {msg.value!r}")
        await consumer.commit()
```

!!! note "At-least-once delivery"
    `enable_auto_commit` defaults to `False`. Call `await consumer.commit()` after
    processing each message (or batch) to guarantee at-least-once delivery.

### Multiple topics

```python
async with consumer_lifecycle(
    settings,
    topics=("orders", "payments", "refunds"),
) as consumer:
    async for msg in consumer:
        ...
```

### Low-level factory

```python
from aiokafka_foundation_kit import create_async_kafka_consumer

consumer = create_async_kafka_consumer(settings, topics=["orders"])
await consumer.start()
try:
    async for msg in consumer:
        ...
finally:
    await consumer.stop()
```

---

## Health check

Probe whether the Kafka cluster is reachable. Returns `True` when the broker responds within
the timeout:

```python
from aiokafka_foundation_kit import check_kafka_health_async
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

is_healthy = await check_kafka_health_async(settings, timeout_seconds=5.0)
if not is_healthy:
    raise RuntimeError("Kafka cluster is unreachable")
```

Pair this with a `/healthz` HTTP endpoint or a Kubernetes startup probe.

---

## Topic management

Create topics idempotently — topics that already exist are logged and skipped silently:

```python
from aiokafka_foundation_kit import ensure_topics_async, TopicConfig
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

topics = [
    TopicConfig(
        name="orders",
        num_partitions=6,
        replication_factor=3,
        topic_configs={"retention.ms": "604800000"},  # 7 days
    ),
    TopicConfig(name="dead-letter-orders", num_partitions=1, replication_factor=3),
]

await ensure_topics_async(topics, settings)
```

---

## JSON serialisation

`dumps_bytes` / `loads_bytes` are the default serialisers used internally and can be used
standalone. When [orjson](https://github.com/ijl/orjson) is installed (`pip install
aiokafka-foundation-kit[orjson]`) it is used automatically; otherwise the standard `json`
module is used as a fallback.

```python
from aiokafka_foundation_kit.utils.json import dumps_bytes, loads_bytes

payload = {"event": "order_placed", "order_id": 42}

raw = dumps_bytes(payload)   # bytes
data = loads_bytes(raw)      # dict

# Raw bytes pass through unchanged (e.g. already-serialised Avro / Protobuf)
assert dumps_bytes(b"binary") == b"binary"

# None (tombstone) is preserved
assert loads_bytes(None) is None
```

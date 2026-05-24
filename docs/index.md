# aiokafka-foundation-kit

Async Kafka foundation library — factories, settings, DI providers, and OpenTelemetry on top of [aiokafka](https://github.com/aio-libs/aiokafka).

## Features

| Feature | Description |
| --- | --- |
| **Protocol-based config** | Settings are defined as `typing.Protocol` — any dict-like or Pydantic model works |
| **Pydantic models** | Ready-made `BaseKafkaProducerSettings` / `BaseKafkaConsumerSettings` with validation |
| **Lifecycle helpers** | `producer_lifecycle` / `consumer_lifecycle` async context managers for clean start/stop |
| **Topic management** | `ensure_topics_async` creates topics idempotently before your producer starts |
| **Health checks** | `check_kafka_health_async` probes broker reachability in one line |
| **JSON serialisation** | `dumps_bytes` / `loads_bytes` with transparent [orjson](https://github.com/ijl/orjson) acceleration |
| **Dishka providers** | `AsyncKafkaProducerProvider`, `AsyncKafkaConsumerProvider`, `KafkaInfraProvider` |
| **dependency-injector** | `KafkaProducerContainer` / `KafkaConsumerContainer` for classic DI |
| **OpenTelemetry** | `instrument_aiokafka` wires distributed tracing with one call |

## Installation

```bash
# Core (AIOKafka factories + lifecycle helpers only)
pip install aiokafka-foundation-kit

# Everything
pip install aiokafka-foundation-kit[all]

# Pick what you need
pip install aiokafka-foundation-kit[models,orjson,dishka,telemetry]
```

## Minimal example

```python
from aiokafka_foundation_kit import producer_lifecycle, consumer_lifecycle
from aiokafka_foundation_kit.contrib.models import (
    BaseKafkaProducerSettings,
    BaseKafkaConsumerSettings,
)

producer_settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")
consumer_settings = BaseKafkaConsumerSettings(
    bootstrap_servers="localhost:9092",
    group_id="my-group",
)

# Producer
async with producer_lifecycle(producer_settings) as producer:
    await producer.send_and_wait("events", b'{"type": "ping"}')

# Consumer
async with consumer_lifecycle(consumer_settings, topics=("events",)) as consumer:
    async for msg in consumer:
        print(msg.value)
```

## Next steps

- [Quick start guide](guide/quickstart.md) — end-to-end producer + consumer examples
- [Configuration](guide/configuration.md) — all settings fields and security modes
- [Advanced](guide/advanced.md) — DI providers, telemetry, topic management
- [API Reference](reference/index.md) — full symbol documentation

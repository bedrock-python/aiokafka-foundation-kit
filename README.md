# aiokafka-foundation-kit

[![PyPI](https://img.shields.io/pypi/v/aiokafka-foundation-kit?color=blue)](https://pypi.org/project/aiokafka-foundation-kit/)
[![Python](https://img.shields.io/pypi/pyversions/aiokafka-foundation-kit)](https://pypi.org/project/aiokafka-foundation-kit/)
[![License](https://img.shields.io/github/license/bedrock-python/aiokafka-foundation-kit)](LICENSE)
[![CI](https://github.com/bedrock-python/aiokafka-foundation-kit/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/bedrock-python/aiokafka-foundation-kit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bedrock-python/aiokafka-foundation-kit/graph/badge.svg)](https://codecov.io/gh/bedrock-python/aiokafka-foundation-kit)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://bedrock-python.github.io/aiokafka-foundation-kit/)

Async Kafka foundation library — settings, client factories, lifecycle helpers, topic management and DI wiring on top of [aiokafka](https://github.com/aio-libs/aiokafka).

> [!TIP]
> **Building this with an AI assistant?** Hand it
> **[one page](https://bedrock-python.github.io/aiokafka-foundation-kit/agents/)** instead
> of the whole site: the public API with every settings field and its real default, the
> producer and consumer lifecycle and commit semantics, the rules that break code when
> they are broken, the mistakes models actually make with this API, and a map of which
> page to fetch for the rest. Every docs page is also served as raw Markdown at its own
> URL, and a **Copy page** button at the top of each one hands it straight to a chat
> window.

## Features

- **AsyncIO-first** — everything is built on `aiokafka`; there is no synchronous mirror
- **Protocol-based config** — every entry point takes a `typing.Protocol`, so a Pydantic model, a dataclass or a stub all work
- **Pydantic settings** — `BaseKafkaProducerSettings` / `BaseKafkaConsumerSettings` with validation, via the `models` extra
- **Lifecycle helpers** — `producer_lifecycle` / `consumer_lifecycle` own start and stop, including on failure
- **Topic management** — `ensure_topics_async` creates topics idempotently before a producer starts
- **Health checks** — `check_kafka_health_async` probes broker reachability
- **JSON serialisation** — `dumps_bytes` / `loads_bytes`, with transparent `orjson` acceleration
- **DI integration** — Dishka providers and dependency-injector containers
- **OpenTelemetry** — `instrument_aiokafka` wires the aiokafka instrumentor in one call
- **Type-safe** — full annotations, checked with mypy

## Installation

```bash
# Core functionality
pip install aiokafka-foundation-kit

# With all optional dependencies
pip install aiokafka-foundation-kit[all]

# Or specific features
pip install aiokafka-foundation-kit[models,orjson,dishka,dependency-injector,telemetry]
```

## Quick Start

### Producer

```python
from aiokafka_foundation_kit import TopicConfig, producer_lifecycle
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")
topics = [TopicConfig(name="events", num_partitions=3, replication_factor=1)]

async with producer_lifecycle(settings, topics=topics, auto_create_topics=True) as producer:
    # dicts are serialised to JSON; keys must already be bytes
    await producer.send_and_wait("events", {"event": "user_created", "user_id": "123"})
```

`producer_lifecycle` yields aiokafka's own `AIOKafkaProducer`, started, and stops it when the
block exits — including when the block raises.

### Consumer

```python
from aiokafka_foundation_kit import consumer_lifecycle
from aiokafka_foundation_kit.contrib.models import BaseKafkaConsumerSettings

settings = BaseKafkaConsumerSettings(
    bootstrap_servers="localhost:9092",
    group_id="my-group",
)

async with consumer_lifecycle(settings, topics=("events",)) as consumer:
    async for message in consumer:
        print(message.value)   # already decoded from JSON
        await consumer.commit()
```

`enable_auto_commit` defaults to `False`, so commit after processing to get at-least-once
delivery. The `async for` loop is yours to write: this library manages the client, not the
message flow.

## Documentation

Full documentation at [bedrock-python.github.io/aiokafka-foundation-kit](https://bedrock-python.github.io/aiokafka-foundation-kit/).

[For AI agents](https://bedrock-python.github.io/aiokafka-foundation-kit/agents/) is the whole library on one page — the API surface, the rules that break code when they are broken, the mistakes models make, and a map of which page to fetch for the rest — written to be handed to a coding assistant.

## Development

```bash
# Install dependencies
uv sync --group dev --all-extras

# Lint, format check and mypy
make check

# Run tests
make test-unit
make test
```

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.

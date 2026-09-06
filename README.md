# aiokafka-foundation-kit

[![PyPI](https://img.shields.io/pypi/v/aiokafka-foundation-kit?color=blue)](https://pypi.org/project/aiokafka-foundation-kit/)
[![Python](https://img.shields.io/pypi/pyversions/aiokafka-foundation-kit)](https://pypi.org/project/aiokafka-foundation-kit/)
[![License](https://img.shields.io/github/license/bedrock-python/aiokafka-foundation-kit)](LICENSE)
[![CI](https://github.com/bedrock-python/aiokafka-foundation-kit/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/bedrock-python/aiokafka-foundation-kit/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bedrock-python/aiokafka-foundation-kit/graph/badge.svg)](https://codecov.io/gh/bedrock-python/aiokafka-foundation-kit)
[![Docs](https://img.shields.io/badge/docs-online-blue)](https://bedrock-python.github.io/aiokafka-foundation-kit/)

Async Kafka foundation library with producer, consumer, metrics, and Dishka providers built on [aiokafka](https://github.com/aio-libs/aiokafka).

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

- 🚀 **AsyncIO-first**: Built on top of `aiokafka` for high-performance async operations
- 📊 **Observability**: Built-in Prometheus metrics and OpenTelemetry support
- 🔧 **Pydantic Settings**: Type-safe configuration with Pydantic
- 💉 **Dishka Integration**: Dependency injection providers for easy integration
- 🔄 **Retry Logic**: Configurable retry policies with `tenacity`
- 🏥 **Health Checks**: Kafka cluster health monitoring
- 📝 **Topic Management**: Automatic topic creation and management
- 🎯 **Type-Safe**: Full type annotations with mypy strict mode

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

### Producer Example

```python
from aiokafka_foundation_kit import (
    BaseKafkaProducerSettings,
    AsyncKafkaPublisher,
    create_async_kafka_producer,
)

# Configure
settings = BaseKafkaProducerSettings(
    bootstrap_servers="localhost:9092",
)

# Create producer
producer = create_async_kafka_producer(settings)
await producer.start()

# Create publisher
publisher = AsyncKafkaPublisher(producer)
await publisher.start()

# Publish
await publisher.publish(
    topic="events",
    value={"event": "user_created", "user_id": "123"},
    event_type="user_created",
)
```

### Consumer Example

```python
from aiokafka_foundation_kit import (
    BaseKafkaConsumerSettings,
    AsyncKafkaConsumerRunner,
    create_async_kafka_consumer,
)

# Configure
settings = BaseKafkaConsumerSettings(
    bootstrap_servers="localhost:9092",
    group_id="my-group",
)

# Create consumer
consumer = create_async_kafka_consumer(settings, topics=["events"])
client = AsyncKafkaConsumerClient(consumer)
await client.start()

# Handle messages
async def handle(message):
    print(f"Received: {message.value}")

runner = AsyncKafkaConsumerRunner(client, handle)
await runner.start()
```

## Documentation

Full documentation at [bedrock-python.github.io/aiokafka-foundation-kit](https://bedrock-python.github.io/aiokafka-foundation-kit/).

[For AI agents](https://bedrock-python.github.io/aiokafka-foundation-kit/agents/) is the whole library on one page — the API surface, the rules that break code when they are broken, the mistakes models make, and a map of which page to fetch for the rest — written to be handed to a coding assistant.

## Development

```bash
# Install dependencies
uv sync --group dev

# Run tests
make test

# Run linter
make lint

# Format code
make fmt
```

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.

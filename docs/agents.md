# aiokafka-foundation-kit for AI agents

> One page holding everything a coding assistant needs to configure and drive
> aiokafka-foundation-kit correctly, plus a map of where the rest of the documentation
> keeps the details it leaves out. Give an agent this page rather than the whole site.

| | |
|---|---|
| Package | `aiokafka-foundation-kit` on PyPI, import root `aiokafka_foundation_kit` |
| Requires | Python 3.11+, `aiokafka>=0.12.0,<1.0.0` (0.14 in the lock), a reachable Kafka broker |
| Install | `pip install aiokafka-foundation-kit` · extras: `models`, `orjson`, `dishka`, `dependency-injector`, `telemetry`, `all` |
| Async | everything. The factories, both lifecycles, `ensure_topics_async` and `check_kafka_health_async` |
| Sync | none. There is no synchronous mirror, and the factories must be called from inside a running event loop |
| Source | <https://github.com/bedrock-python/aiokafka-foundation-kit> |

## How to read this page

Every page of this site is also served as raw Markdown at its own URL with `.md` in place
of the trailing slash — this page is `/agents.md`, the configuration guide is
`/guide/configuration.md` — so anything the map below points at can be fetched as plain
text rather than scraped out of HTML. The **Copy page** control at the top of a page does
the same thing for a human with a chat window open. The one exception is the API
reference: its Markdown is a list of instructions to a docstring renderer rather than the
API, so it carries neither the control nor a `.md` twin — read it as HTML, or read the
docstrings in the source.

Top to bottom before writing code. [Rules that hold or break the code](#rules-that-hold-or-break-the-code)
is the section correctness lives in — those are the things the library will not save you
from. Every name used below is in the public API; if you need something not listed here,
fetch the page the [documentation map](#documentation-map) points at rather than guessing
a method that sounds plausible.

## Scope

**It does** turn a settings object into a configured `AIOKafkaProducer` or
`AIOKafkaConsumer`, own the start/stop of that client as an async context manager, create
topics idempotently before a producer starts, probe whether a cluster answers, serialise
message values as JSON, and hand the same wiring to dishka, dependency-injector and
OpenTelemetry.

**It does not** wrap aiokafka. Once a client is yielded you are holding an
`AIOKafkaProducer` / `AIOKafkaConsumer` and every send, poll, commit, seek and rebalance
callback is aiokafka's own API — read aiokafka's documentation for those. There is no
publisher class, no consumer runner, no message router, no retry policy, no dead-letter
handling, no outbox, no schema registry, no Prometheus metrics and no synchronous
variant. It runs no loop of its own: `consumer_lifecycle` subscribes, and the `async for`
over the consumer is yours to write.

## Mental model

Four nouns:

* **Settings** — any object with the right attributes. Every entry point takes a
  `typing.Protocol`, not a base class, so a Pydantic model, a dataclass, a
  `SimpleNamespace` or a `MagicMock` all work. The `models` extra ships Pydantic
  implementations with the defaults below; nothing checks a hand-rolled object, so a
  missing attribute surfaces as `AttributeError` when the client is built.
* **Factory** — `create_async_kafka_producer(settings)` /
  `create_async_kafka_consumer(settings, topics)` read the settings and return a client
  that has not been started. Both go through `build_kafka_common_config(settings)`, which
  is the one place connection, SASL and TLS keys are assembled.
* **Lifecycle** — `producer_lifecycle` / `consumer_lifecycle` are async context managers
  around `managed_kafka_client`: start, optional `on_started` hook, yield, stop in a
  `finally`, optional `on_stopped` hook. `producer_lifecycle` can also run
  `ensure_topics_async` first.
* **Topics** — `TopicConfig` is a frozen dataclass describing one topic;
  `ensure_topics_async(topics, settings)` creates each one and treats an existing topic as
  success.

The contrib packages are three thin wrappers over that: `contrib.di` for dishka providers,
`contrib.dependency_injector` for declarative containers, `contrib.telemetry` for one call
to the OpenTelemetry aiokafka instrumentor. None of them is imported by
`aiokafka_foundation_kit/__init__.py`; `contrib/__init__.py` exports nothing, so every
contrib name is an explicit import from its own submodule and raises `ImportError` naming
the extra when its dependency is missing.

## Wiring

```python
import asyncio

from aiokafka_foundation_kit import (
    TopicConfig,
    consumer_lifecycle,
    producer_lifecycle,
)
from aiokafka_foundation_kit.contrib.models import (
    BaseKafkaConsumerSettings,
    BaseKafkaProducerSettings,
)

producer_settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")
consumer_settings = BaseKafkaConsumerSettings(
    bootstrap_servers="localhost:9092",
    group_id="order-processor",
)

topics = [TopicConfig(name="orders", num_partitions=6, replication_factor=1)]


async def produce() -> None:
    async with producer_lifecycle(
        producer_settings,
        topics=topics,
        auto_create_topics=True,        # both arguments are needed; neither alone does anything
    ) as producer:
        await producer.send_and_wait("orders", {"id": 42})   # dict -> JSON bytes


async def consume() -> None:
    async with consumer_lifecycle(consumer_settings, topics=("orders",)) as consumer:
        async for message in consumer:                       # the loop is yours
            await handle(message.value)                      # already decoded from JSON
            await consumer.commit()                          # enable_auto_commit is False


asyncio.run(produce())
```

Everything happens inside a running loop on purpose: aiokafka's constructors take the
running loop, so building a client at import time raises `RuntimeError`.

## The API

### The root package

`from aiokafka_foundation_kit import …`

| Name | Signature | Returns |
|---|---|---|
| `create_async_kafka_producer` | `(settings, *, value_serializer=None)` | `AIOKafkaProducer`, **not started** |
| `producer_lifecycle` | `(settings, *, topics=None, auto_create_topics=False, name="producer", on_started=None, on_stopped=None)` | async context manager yielding a started `AIOKafkaProducer` |
| `create_async_kafka_consumer` | `(settings, topics=None, *, value_deserializer=None)` | `AIOKafkaConsumer`, **not started** |
| `consumer_lifecycle` | `(settings, *, topics=None, name="consumer", on_started=None, on_stopped=None)` | async context manager yielding a started `AIOKafkaConsumer` |
| `ensure_topics_async` | `(topics, settings)` | `None` |
| `TopicConfig` | frozen dataclass `(name, num_partitions, replication_factor, replica_assignment=None, topic_configs=None)` | — |
| `check_kafka_health_async` | `(settings, timeout_seconds=5.0)` | `bool` |
| `build_kafka_common_config` | `(settings)` | `dict[str, Any]` of aiokafka keyword arguments |
| `__version__` | — | `str` |

Also at the root, for annotations only: `KafkaSettingsProtocol`, `ProducerSettingsProtocol`,
`ConsumerSettingsProtocol`, `TopicConfigProtocol`.

`topics` on `producer_lifecycle` is a `Sequence[TopicConfigProtocol]`; `topics` on
`consumer_lifecycle` is a `tuple[str, ...]` of names. `on_started` and `on_stopped` are
async callables taking the client. `name` appears only in log messages and matters when
one process runs several clients.

### Public, but not at the root

| Name | Import from |
|---|---|
| `dumps_bytes`, `loads_bytes`, `managed_kafka_client` | `aiokafka_foundation_kit.utils` |
| `KafkaConnectionSettingsProtocol`, `KafkaSaslSettingsProtocol`, `KafkaSslSettingsProtocol` | `aiokafka_foundation_kit.config` |
| `ProducerLifecycleSettingsProtocol` | `aiokafka_foundation_kit.config.producer` — it is *not* re-exported by `.config` |
| `KafkaSecurityProtocol`, `KafkaSaslMechanism`, `KafkaAcks`, `KafkaCompressionType`, `KafkaOffsetReset` | `aiokafka_foundation_kit.config.kafka`, or `aiokafka_foundation_kit.contrib.models` |
| every contrib name | its own submodule — `contrib/__init__.py` exports nothing |

`managed_kafka_client(client, *, name, on_started=None, on_stopped=None)` wraps anything
with async `start()` / `stop()`; both lifecycles are built on it.
`dumps_bytes(value)` returns `bytes` unchanged and JSON-encodes everything else;
`loads_bytes(value)` returns `None` for `value is None` (a tombstone) and JSON-decodes
otherwise. Both use `orjson` when the `orjson` extra is installed and the standard
library `json` when it is not.

### Settings protocols

Structural, so any object with these attributes qualifies. The hierarchy:

```
KafkaConnectionSettingsProtocol   bootstrap_servers, client_id, security_protocol,
│                                 metadata_max_age_ms, get_sasl_password()
├── KafkaSaslSettingsProtocol     + sasl_mechanism, sasl_username
└── KafkaSslSettingsProtocol      + ssl_cafile, ssl_certfile, ssl_keyfile, ssl_check_hostname

KafkaSettingsProtocol             = SASL + SSL, the base every entry point accepts
├── ProducerSettingsProtocol      + acks, compression_type, enable_idempotence,
│   │                               max_batch_size, linger_ms, request_timeout_ms
│   └── ProducerLifecycleSettingsProtocol   + auto_create_topics
└── ConsumerSettingsProtocol      + group_id, auto_offset_reset, enable_auto_commit,
                                    session_timeout_ms, heartbeat_interval_ms,
                                    max_poll_records, max_poll_interval_ms,
                                    fetch_max_wait_ms, fetch_min_bytes, fetch_max_bytes
```

Note that the password is a **method**, `get_sasl_password() -> str | None`, not an
attribute. `TopicConfigProtocol` is `name`, `num_partitions`, `replication_factor`,
`replica_assignment`, `topic_configs`.

### Pydantic settings — `contrib.models`

`pip install "aiokafka-foundation-kit[models]"`. These are plain `BaseModel`s, so extra
keyword arguments are ignored rather than rejected, and `bootstrap_servers` (and
`group_id`) are the only required fields.

`BaseKafkaSettings` — connection, SASL and TLS:

| Field | Type | Default |
|---|---|---|
| `bootstrap_servers` | `str` | **required** |
| `client_id` | `str | None` | `None` |
| `metadata_max_age_ms` | `int` | `300000` |
| `security_protocol` | `"PLAINTEXT" | "SASL_PLAINTEXT" | "SSL" | "SASL_SSL"` | `"PLAINTEXT"` |
| `sasl_mechanism` | `"PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | None` | `None` |
| `sasl_username` | `str | None` | `None` |
| `sasl_password` | `SecretStr | None` | `None` |
| `ssl_cafile` / `ssl_certfile` / `ssl_keyfile` | `str | None` | `None` |
| `ssl_check_hostname` | `bool` | `True` |

Two validators run after construction: a `SASL_*` protocol requires `sasl_mechanism`,
`sasl_username` and a non-empty `sasl_password`; `SSL` and `SASL_SSL` require `ssl_cafile`.
Either failure is a `pydantic.ValidationError`.

`BaseKafkaProducerSettings` = `BaseKafkaSettings` + `KafkaAutoCreateMixin`:

| Field | Type | Default |
|---|---|---|
| `acks` | `"0" | "1" | "all"` | `"all"` |
| `compression_type` | `"gzip" | "snappy" | "lz4" | "zstd" | None` | `"gzip"` |
| `enable_idempotence` | `bool` | `True` |
| `max_batch_size` | `int` | `16384` |
| `linger_ms` | `int` | `5` |
| `request_timeout_ms` | `int` | `30000` |
| `auto_create_topics` | `bool` | `False` |
| `default_partitions` | `int` | `3` |
| `default_replication_factor` | `int | None` | `None`, required when `auto_create_topics=True` |

`BaseKafkaConsumerSettings` = `BaseKafkaSettings` +:

| Field | Type | Default |
|---|---|---|
| `group_id` | `str` | **required** |
| `auto_offset_reset` | `"earliest" | "latest"` | `"earliest"` |
| `enable_auto_commit` | `bool` | `False` |
| `session_timeout_ms` | `int` | `30000` |
| `heartbeat_interval_ms` | `int` | `3000` |
| `max_poll_records` | `int` | `500` |
| `max_poll_interval_ms` | `int` | `300000` |
| `fetch_max_wait_ms` | `int` | `500` |
| `fetch_min_bytes` | `int` | `1` |
| `fetch_max_bytes` | `int` | `52428800` |

`BaseKafkaInfraSettings` describes a topic namespace rather than a connection:
`topic_prefix: str | None = None`, `topic_catalog: dict[str, KafkaTopicSettings] | None = None`,
`consumer_subscriptions: list[str] | None = None`. `KafkaTopicSettings` is
`num_partitions=3`, `replication_factor=3`, `topic_configs=None`.
`normalize_kafka_topic_prefix_value(value)` is the prefix coercion on its own: `None`,
`""`, whitespace, `"None"` and `"null"` in any case all become `None`, and so does any
non-string.

The mixins are exported separately for composing your own model:
`KafkaConnectionMixin`, `KafkaSaslMixin`, `KafkaSslMixin`, `KafkaAutoCreateMixin`.

### dishka — `contrib.di`

`pip install "aiokafka-foundation-kit[dishka]"`. Three `Provider` subclasses, all
`Scope.APP`, all raising `ImportError` from `__init__` when dishka is absent.

| Provider | Provides | Needs in the container |
|---|---|---|
| `AsyncKafkaProducerProvider` | `AIOKafkaProducer` | `ProducerLifecycleSettingsProtocol` and `Sequence[TopicConfigProtocol] | None` |
| `AsyncKafkaConsumerProvider` | `AIOKafkaConsumer` | `ConsumerSettingsProtocol` and `tuple[str, ...] | None` |
| `KafkaInfraProvider` | `Sequence[TopicConfig]` and `tuple[str, ...]` | `KafkaProducerInfraSettingsProtocol` and `KafkaConsumerInfraSettingsProtocol` |

`AsyncKafkaProducerProvider` is the only place `settings.auto_create_topics` is read.
`KafkaInfraProvider` turns `topic_catalog` into `TopicConfig` objects and
`consumer_subscriptions` into names, applying `topic_prefix` as `f"{prefix}.{name}"`.
The infrastructure protocols — `KafkaInfraBaseSettingsProtocol`,
`KafkaProducerInfraSettingsProtocol`, `KafkaConsumerInfraSettingsProtocol`,
`KafkaTopicSettingsProtocol` — are exported for the settings side.

The two sets of types do not line up on their own; see rule 10.

### dependency-injector — `contrib.dependency_injector`

`pip install "aiokafka-foundation-kit[dependency-injector]"`.

| Container | Providers |
|---|---|
| `KafkaProducerContainer` | `kafka_settings` (`Dependency`), `topics` (`Dependency`), `auto_create_topics` (`Object(False)`), `producer` (`Resource`) |
| `KafkaConsumerContainer` | `kafka_settings` (`Dependency`), `topics` (`Dependency`), `consumer` (`Resource`) |

Override the dependencies, `await container.init_resources()`, then
`producer = await container.producer()`; `await container.shutdown_resources()` stops the
client. See rule 11 — `topics` has no working default.

### OpenTelemetry — `contrib.telemetry`

`pip install "aiokafka-foundation-kit[telemetry]"`.
`instrument_aiokafka(*, tracer_provider=None, async_produce_hook=None, async_consume_hook=None, **kwargs)`
calls `AIOKafkaInstrumentor().instrument(...)`, which patches aiokafka globally — call it
once at startup, before any client is built. Only the arguments you pass are forwarded, so
omitting `tracer_provider` leaves the global provider in place. `async_produce_hook` is
`async (span, args, kwargs)`, `async_consume_hook` is `async (span, record, args, kwargs)`.
Extra `**kwargs` go straight to the instrumentor.

## Rules that hold or break the code

1. **Build clients inside a running event loop.** `create_async_kafka_producer`,
   `create_async_kafka_consumer` and both lifecycles construct aiokafka objects, and
   aiokafka's constructor takes the running loop. Called at import time or from
   synchronous module scope it raises
   `RuntimeError: The object should be created within an async function or provide loop directly.`
2. **TLS does not currently work through these helpers.**
   `build_kafka_common_config` emits `ssl_cafile`, `ssl_certfile`, `ssl_keyfile` and
   `ssl_check_hostname` for `SSL` and `SASL_SSL`, and aiokafka accepts none of them — it takes a
   prepared `ssl_context`. Any `SSL` or `SASL_SSL` settings object therefore
   raises `TypeError: … got an unexpected keyword argument 'ssl_check_hostname'` from the
   producer, the consumer and `ensure_topics_async` alike. Build the client yourself until
   that is fixed: take the dict, drop the four `ssl_*` keys, and pass
   `ssl_context=aiokafka.helpers.create_ssl_context(cafile=…, certfile=…, keyfile=…)`.
   `SASL_PLAINTEXT` and `PLAINTEXT` are unaffected.
3. **`producer_lifecycle` does not read `settings.auto_create_topics`.** It has its own
   keyword, defaulting to `False`, and creates topics only when `auto_create_topics=True`
   *and* `topics` is non-empty. Passing one without the other silently creates nothing.
   The settings field is honoured only by `AsyncKafkaProducerProvider` and by whatever you
   pass to the dependency-injector container.
4. **`enable_auto_commit` is `False` by default on `BaseKafkaConsumerSettings`.** Nothing
   commits for you: call `await consumer.commit()` after processing, or the group re-reads
   from the last committed offset on every restart and rebalance. The factory forwards the
   flag as-is, so a settings object that omits it fails with `AttributeError`, not a
   default.
5. **`enable_idempotence=True` (the default) allows only `acks="all"`.** aiokafka raises
   `ValueError: Invalid ACKS parameter` for `"0"` or `"1"`. The `KafkaAcks` alias offers
   three values; two of them require `enable_idempotence=False`.
6. **Only `gzip` compression works out of the box.** `snappy`, `lz4` and `zstd` need
   `cramjam`, which this package does not depend on — without it aiokafka raises
   `RuntimeError: Compression library for lz4 not found` at construction. Install
   `aiokafka[lz4]` (or `[snappy]`, `[zstd]`) alongside.
7. **Values are JSON, keys are bytes.** The producer factory installs
   `value_serializer=dumps_bytes`, so a `dict` is encoded for you and `bytes` pass through
   untouched. It installs no `key_serializer`, so `key=` must already be `bytes` or
   aiokafka raises. On the consumer side `value_deserializer=loads_bytes` means
   `message.value` is a decoded object; a topic carrying Avro, protobuf or plain text needs
   `value_deserializer=lambda raw: raw` or the decode raises inside aiokafka's fetcher.
8. **An `on_started` hook that raises leaks a started client.** The hook runs after
   `start()` but before the `try`/`finally` that stops it, so the exception propagates with
   the client still connected. Keep `on_started` total, or do the work inside the `async
   with` body instead.
9. **Only `KafkaError` is swallowed on stop.** `managed_kafka_client` catches
   `KafkaError` from `stop()`, logs it, and skips `on_stopped`; anything else — `OSError`,
   `asyncio.CancelledError`, a `TimeoutError` — propagates out of the context manager and
   can mask the body's own exception.
10. **dishka resolves by exact type, and the kit's providers ask for optionals.**
    `AsyncKafkaProducerProvider` wants `Sequence[TopicConfigProtocol] | None`, while
    `KafkaInfraProvider` supplies `Sequence[TopicConfig]`; the consumer wants
    `tuple[str, ...] | None` against a supplied `tuple[str, ...]`. Python's `= None`
    default is not a dishka default, so a container holding both providers fails to build
    with `GraphMissingFactoryError`. Add a one-method bridge provider (see
    [Common mistakes](#common-mistakes)).
11. **`Dependency(default=None)` is not a default.** dependency-injector treats `None` as
    "unset", so `KafkaProducerContainer` and `KafkaConsumerContainer` raise
    `Error: Dependency "…topics" is not defined` unless you call
    `container.topics.override(...)` — pass `override(None)` when you want no topics.
12. **`default_partitions` and `default_replication_factor` are validated and never
    used.** Nothing in the library reads them; the shape of a created topic comes from
    `TopicConfig` (where `num_partitions` and `replication_factor` are both required) or
    from `KafkaTopicSettings` in a `topic_catalog`. The validator that demands
    `default_replication_factor` when `auto_create_topics=True` is the only thing they do.
13. **The consumer lifecycle subscribes; it does not consume.** You write the `async for`,
    and your per-batch work has to finish inside `max_poll_interval_ms` (300 s by default)
    or the broker evicts the member and the partitions rebalance under you.
14. **`ensure_topics_async` tolerates exactly one failure.** Each topic is created on its
    own and `TopicAlreadyExistsError` is logged and ignored; every other broker error —
    `replication_factor` above the broker count is the usual one — propagates and aborts
    the rest of the sequence. It also never *changes* an existing topic: partition counts
    and `topic_configs` of a topic that already exists are left alone.
15. **`check_kafka_health_async` opens a real producer connection**, and its
    `timeout_seconds` becomes `request_timeout_ms`, not an `asyncio.timeout` around the
    call — a DNS or TCP stall can outlast it. It returns `False` (and logs a traceback at
    `ERROR`) for `TimeoutError`, `KafkaError` and `OSError`, and lets anything else out.
    It is a probe, not a cheap one; do not call it per request.
16. **A `topic_prefix` joins with a dot** — `prefix` + `"."` + `name` — and only
    `KafkaInfraProvider` applies it. `TopicConfig.name` and the topic names you hand
    `consumer_lifecycle` are physical names, already prefixed.
17. **The protocols are structural and unenforced.** Nothing validates a hand-rolled
    settings object; a typo or a missing attribute is an `AttributeError` at client
    construction, and `get_sasl_password` must be a callable, not a string.

## Common mistakes

```python
# WRONG — a class the library has never had, imported from the root
from aiokafka_foundation_kit import AsyncKafkaPublisher, BaseKafkaProducerSettings

# RIGHT — settings live in contrib.models, and the producer is aiokafka's own
from aiokafka_foundation_kit import producer_lifecycle
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

async with producer_lifecycle(settings) as producer:
    await producer.send_and_wait("orders", {"id": 42})
```

```python
# WRONG — built at import time, outside any loop
producer = create_async_kafka_producer(settings)   # RuntimeError

# RIGHT — inside a coroutine, and let the context manager own start/stop
async def main() -> None:
    async with producer_lifecycle(settings) as producer:
        ...
```

```python
# WRONG — topics without the flag; nothing is created, and send fails on a
# cluster with auto.create.topics.enable=false
async with producer_lifecycle(settings, topics=topics) as producer:
    ...

# RIGHT — both, every time
async with producer_lifecycle(settings, topics=topics, auto_create_topics=True) as producer:
    ...
```

```python
# WRONG — at-least-once assumed, never delivered: enable_auto_commit is False
async with consumer_lifecycle(settings, topics=("orders",)) as consumer:
    async for message in consumer:
        await handle(message.value)

# RIGHT — commit after the work, so a crash replays rather than skips
async with consumer_lifecycle(settings, topics=("orders",)) as consumer:
    async for message in consumer:
        await handle(message.value)
        await consumer.commit()
```

```python
# WRONG — a str key, and acks the idempotent producer will not accept
settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092", acks="1")
await producer.send_and_wait("orders", {"id": 42}, key="order-42")

# RIGHT — bytes for the key; turn idempotence off if you really want acks="1"
settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")
await producer.send_and_wait("orders", {"id": 42}, key=b"order-42")
```

```python
# WRONG — the two providers do not compose; the container refuses to build
container = make_async_container(SettingsProvider(), KafkaInfraProvider(),
                                 AsyncKafkaProducerProvider())

# RIGHT — bridge the exact type keys the kit's providers ask for
class KafkaBridgeProvider(Provider):
    scope = Scope.APP

    @provide
    def producer_topics(self, configs: Sequence[TopicConfig]) -> Sequence[TopicConfigProtocol] | None:
        return configs

    @provide
    def consumer_topics(self, names: tuple[str, ...]) -> tuple[str, ...] | None:
        return names

container = make_async_container(SettingsProvider(), KafkaInfraProvider(),
                                 KafkaBridgeProvider(), AsyncKafkaProducerProvider())
```

## Errors

The library defines no exception class of its own. What you will see:

| Exception | Where it comes from |
|---|---|
| `ImportError` | instantiating a contrib provider or container, or calling `instrument_aiokafka`, without the extra. The message names the extra to install. |
| `pydantic.ValidationError` | a `contrib.models` settings object: a missing `bootstrap_servers` or `group_id`, a value outside a `Literal`, an incomplete `SASL_*` or `SSL` block, `auto_create_topics=True` with no `default_replication_factor`. |
| `RuntimeError` | a client built outside a running loop, or a compression codec whose library is missing. |
| `TypeError` | `SSL` / `SASL_SSL` settings reaching aiokafka — see rule 2. |
| `ValueError` | `acks` of `"0"` or `"1"` while `enable_idempotence` is on. |
| `AttributeError` | a settings object that does not satisfy the protocol. |
| `aiokafka.errors.KafkaError` and its subclasses | every broker interaction. `TopicAlreadyExistsError` is the one `ensure_topics_async` handles; `KafkaConnectionError`, `KafkaTimeoutError`, `NodeNotReadyError`, `UnknownTopicOrPartitionError`, `CommitFailedError` and the rest reach you unchanged. |
| `dishka.exceptions.GraphMissingFactoryError` | a container missing one of the exact types in rule 10. |
| `dependency_injector.errors.Error` | a container dependency never overridden — rule 11. |

## Documentation map

Fetch a page when the task is the one named beside it.

| Page | Read it when |
|---|---|
| [Home](index.md) | the feature list, the extras, the shortest possible example |
| [Quick start](guide/quickstart.md) | writing the first producer or consumer end to end, health checks, topic creation, JSON helpers |
| [Configuration](guide/configuration.md) | every settings field and its default, the security modes, loading from the environment, implementing a protocol by hand |
| [Advanced](guide/advanced.md) | dishka providers, dependency-injector containers, OpenTelemetry, topic prefixes, `managed_kafka_client`, `build_kafka_common_config` |
| [API reference](reference/index.md) | an exact signature or docstring — HTML only, see above |
| [Changelog](changelog.md) | what changed between versions |

# Configuration

## Design: protocol-based settings

All factory functions and lifecycle helpers accept *protocols* rather than concrete types.
A `typing.Protocol` describes only the attributes and methods a settings object must expose —
it imposes no base class requirement.

This means you can pass:

- A `BaseKafkaProducerSettings` Pydantic model (provided by the `models` extra).
- Your own dataclass or plain class that matches the shape.
- A `MagicMock` in tests.

The protocol hierarchy is:

```
KafkaConnectionSettingsProtocol   ← bootstrap_servers, client_id, security_protocol,
│                                    metadata_max_age_ms, get_sasl_password()
├── KafkaSaslSettingsProtocol     ← + sasl_mechanism, sasl_username
└── KafkaSslSettingsProtocol      ← + ssl_cafile, ssl_certfile, ssl_keyfile, ssl_check_hostname

KafkaSettingsProtocol             ← combines SASL + SSL (base for producer and consumer)
├── ConsumerSettingsProtocol      ← + group_id, auto_offset_reset, enable_auto_commit, …
└── ProducerSettingsProtocol      ← + acks, compression_type, enable_idempotence, …
    └── ProducerLifecycleSettingsProtocol ← + auto_create_topics
```

---

## Pydantic models (recommended)

Install the `models` extra to get Pydantic implementations of the protocols:

```bash
pip install aiokafka-foundation-kit[models]
```

### BaseKafkaSettings

Shared base for all settings. Combines connection, SASL, and SSL mixins.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bootstrap_servers` | `str` | **required** | Comma-separated `host:port` list |
| `client_id` | `str \| None` | `None` | Client identifier visible in broker logs |
| `metadata_max_age_ms` | `int` | `300000` | How often metadata is refreshed (ms) |
| `security_protocol` | `KafkaSecurityProtocol` | `"PLAINTEXT"` | One of: `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL` |
| `sasl_mechanism` | `KafkaSaslMechanism \| None` | `None` | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` |
| `sasl_username` | `str \| None` | `None` | SASL username |
| `sasl_password` | `SecretStr \| None` | `None` | SASL password (masked in repr) |
| `ssl_cafile` | `str \| None` | `None` | Path to CA certificate |
| `ssl_certfile` | `str \| None` | `None` | Path to client certificate |
| `ssl_keyfile` | `str \| None` | `None` | Path to client private key |
| `ssl_check_hostname` | `bool` | `True` | Verify the broker hostname in the TLS certificate |

Validators enforce consistency: SASL fields are required when `security_protocol` is `SASL_*`;
`ssl_cafile` is required when `security_protocol` is `SSL` or `SASL_SSL`.

### BaseKafkaConsumerSettings

Extends `BaseKafkaSettings`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `group_id` | `str` | **required** | Consumer group identifier |
| `auto_offset_reset` | `KafkaOffsetReset` | `"earliest"` | `"earliest"` or `"latest"` |
| `enable_auto_commit` | `bool` | `False` | Broker-side auto-commit. Keep `False` for at-least-once. |
| `session_timeout_ms` | `int` | `30000` | Session timeout for group membership (ms) |
| `heartbeat_interval_ms` | `int` | `3000` | Heartbeat interval (ms) |
| `max_poll_records` | `int` | `500` | Maximum records returned per poll |
| `max_poll_interval_ms` | `int` | `300000` | Maximum time between polls before the consumer is evicted (ms) |
| `fetch_max_wait_ms` | `int` | `500` | Maximum wait for broker to fill a fetch response (ms) |
| `fetch_min_bytes` | `int` | `1` | Minimum bytes the broker should return per fetch |
| `fetch_max_bytes` | `int` | `52428800` | Maximum bytes per fetch request (50 MiB) |

### BaseKafkaProducerSettings

Extends `BaseKafkaSettings` + `KafkaAutoCreateMixin`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `acks` | `KafkaAcks` | `"all"` | `"0"`, `"1"`, or `"all"` |
| `compression_type` | `KafkaCompressionType \| None` | `"gzip"` | `gzip`, `snappy`, `lz4`, `zstd`, or `None` |
| `enable_idempotence` | `bool` | `True` | Enable exactly-once producer semantics |
| `max_batch_size` | `int` | `16384` | Maximum batch size in bytes (16 KiB) |
| `linger_ms` | `int` | `5` | Time to wait before sending a partial batch (ms) |
| `request_timeout_ms` | `int` | `30000` | Request timeout (ms) |
| `auto_create_topics` | `bool` | `False` | Create declared topics on startup |
| `default_partitions` | `int` | `3` | Partition count for auto-created topics |
| `default_replication_factor` | `int \| None` | `None` | Replication factor for auto-created topics (**required** when `auto_create_topics=True`) |

### BaseKafkaInfraSettings

Infrastructure-level settings used by `KafkaInfraProvider` (Dishka).

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `topic_prefix` | `str \| None` | `None` | Global prefix prepended to every topic name (`prefix.topic_name`) |
| `topic_catalog` | `dict[str, KafkaTopicSettings] \| None` | `None` | Logical-name → physical config map for topic creation |
| `consumer_subscriptions` | `list[str] \| None` | `None` | Logical topic names to subscribe to |

`topic_prefix` normalises `"None"`, `"null"`, and empty strings to `None` — useful when the
value comes from an env var or a secrets vault.

---

## Security modes

### PLAINTEXT (default)

No authentication or encryption. Suitable for local development.

```python
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")
```

### SASL_PLAINTEXT / SASL_SSL

SASL authentication over plaintext or TLS.

```python
settings = BaseKafkaProducerSettings(
    bootstrap_servers="kafka.internal:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_username="service-account",
    sasl_password="s3cr3t",
    ssl_cafile="/etc/ssl/certs/ca-bundle.crt",
)
```

Supported mechanisms: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.

### SSL (mutual TLS)

Client certificate authentication.

```python
settings = BaseKafkaProducerSettings(
    bootstrap_servers="kafka.internal:9093",
    security_protocol="SSL",
    ssl_cafile="/etc/certs/ca.crt",
    ssl_certfile="/etc/certs/client.crt",
    ssl_keyfile="/etc/certs/client.key",
)
```

---

## Loading settings from environment

Combine with [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) to
load from environment variables or `.env` files:

```python
from pydantic_settings import BaseSettings
from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings

class AppSettings(BaseSettings):
    kafka: BaseKafkaProducerSettings = BaseKafkaProducerSettings(
        bootstrap_servers="localhost:9092"
    )

    model_config = {"env_nested_delimiter": "__"}

# KAFKA__BOOTSTRAP_SERVERS=kafka:9092 KAFKA__GROUP_ID=my-group python app.py
settings = AppSettings()
```

---

## Custom settings (protocol implementation)

If you prefer not to depend on Pydantic, implement the protocol directly:

```python
from dataclasses import dataclass
from aiokafka_foundation_kit import producer_lifecycle

@dataclass
class MyProducerSettings:
    bootstrap_servers: str
    client_id: str | None = None
    security_protocol: str = "PLAINTEXT"
    metadata_max_age_ms: int = 300_000
    acks: str = "all"
    compression_type: str | None = "gzip"
    enable_idempotence: bool = True
    max_batch_size: int = 16_384
    linger_ms: int = 5
    request_timeout_ms: int = 30_000

    def get_sasl_password(self) -> str | None:
        return None

    # SASL fields (unused when security_protocol == "PLAINTEXT")
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_check_hostname: bool = True

settings = MyProducerSettings(bootstrap_servers="localhost:9092")

async with producer_lifecycle(settings) as producer:
    await producer.send_and_wait("topic", b"data")
```

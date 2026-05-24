# API Reference

Auto-generated from source using [mkdocstrings](https://mkdocstrings.github.io/).

---

## Core

### Factories and lifecycle

::: aiokafka_foundation_kit.create_async_kafka_producer
    options:
      heading_level: 4

::: aiokafka_foundation_kit.producer_lifecycle
    options:
      heading_level: 4

::: aiokafka_foundation_kit.create_async_kafka_consumer
    options:
      heading_level: 4

::: aiokafka_foundation_kit.consumer_lifecycle
    options:
      heading_level: 4

### Topics

::: aiokafka_foundation_kit.TopicConfig
    options:
      heading_level: 4

::: aiokafka_foundation_kit.ensure_topics_async
    options:
      heading_level: 4

### Health

::: aiokafka_foundation_kit.check_kafka_health_async
    options:
      heading_level: 4

### Utilities

::: aiokafka_foundation_kit.build_kafka_common_config
    options:
      heading_level: 4

::: aiokafka_foundation_kit.utils.json.dumps_bytes
    options:
      heading_level: 4

::: aiokafka_foundation_kit.utils.json.loads_bytes
    options:
      heading_level: 4

::: aiokafka_foundation_kit.utils.lifecycle.managed_kafka_client
    options:
      heading_level: 4

---

## Configuration protocols

::: aiokafka_foundation_kit.KafkaSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.ConsumerSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.ProducerSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.TopicConfigProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.config.kafka.KafkaConnectionSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.config.kafka.KafkaSaslSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.config.kafka.KafkaSslSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.config.producer.ProducerLifecycleSettingsProtocol
    options:
      heading_level: 4

---

## contrib.models

Requires `pip install aiokafka-foundation-kit[models]`.

::: aiokafka_foundation_kit.contrib.models.BaseKafkaSettings
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.KafkaConnectionMixin
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.KafkaSaslMixin
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.KafkaSslMixin
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.BaseKafkaConsumerSettings
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.BaseKafkaProducerSettings
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.KafkaAutoCreateMixin
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.BaseKafkaInfraSettings
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.KafkaTopicSettings
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.models.normalize_kafka_topic_prefix_value
    options:
      heading_level: 4

---

## contrib.di (Dishka)

Requires `pip install aiokafka-foundation-kit[dishka]`.

::: aiokafka_foundation_kit.contrib.di.producer.AsyncKafkaProducerProvider
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.di.consumer.AsyncKafkaConsumerProvider
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.di.infra.KafkaInfraProvider
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.di.infra.KafkaProducerInfraSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.di.infra.KafkaConsumerInfraSettingsProtocol
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.di.infra.KafkaTopicSettingsProtocol
    options:
      heading_level: 4

---

## contrib.dependency_injector

Requires `pip install aiokafka-foundation-kit[dependency-injector]`.

::: aiokafka_foundation_kit.contrib.dependency_injector.producer.KafkaProducerContainer
    options:
      heading_level: 4

::: aiokafka_foundation_kit.contrib.dependency_injector.consumer.KafkaConsumerContainer
    options:
      heading_level: 4

---

## contrib.telemetry

Requires `pip install aiokafka-foundation-kit[telemetry]`.

::: aiokafka_foundation_kit.contrib.telemetry.instrumentations.instrument_aiokafka
    options:
      heading_level: 4

---

## Type aliases

| Alias | Values |
| --- | --- |
| `KafkaSecurityProtocol` | `"PLAINTEXT"`, `"SASL_PLAINTEXT"`, `"SSL"`, `"SASL_SSL"` |
| `KafkaSaslMechanism` | `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"` |
| `KafkaAcks` | `"0"`, `"1"`, `"all"` |
| `KafkaCompressionType` | `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"` |
| `KafkaOffsetReset` | `"earliest"`, `"latest"` |

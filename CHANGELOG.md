# Changelog

## [0.2.0](https://github.com/bedrock-python/aiokafka-foundation-kit/compare/aiokafka-foundation-kit-v0.1.3...aiokafka-foundation-kit-v0.2.0) (2026-10-16)


### Features

* pass value_deserializer and value_serializer through the lifecycles and DI providers ([#54](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/54)) ([9629b2b](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/9629b2be9761d87e8fe8d700b1cc05e11c727417)), closes [#50](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/50)
* warn when ensure_topics_async finds an existing topic that differs from its TopicConfig ([#55](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/55)) ([b0ceac3](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/b0ceac32218a6f378bc1c88f111b388baec0b120)), closes [#51](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/51)


### Bug Fixes

* acks "0" and "1" reach aiokafka as integers, so they work with idempotence off ([#52](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/52)) ([be35a20](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/be35a20eb5a665d244300a6568715b47be542ff6)), closes [#48](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/48)
* stop the client when start() fails or is cancelled, in both lifecycles and the health check ([#53](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/53)) ([f29ac8a](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/f29ac8a7d2557a05f82f6062962a3e53c1ffd9c7)), closes [#49](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/49)

## [0.1.3](https://github.com/bedrock-python/aiokafka-foundation-kit/compare/aiokafka-foundation-kit-v0.1.2...aiokafka-foundation-kit-v0.1.3) (2026-09-07)


### Bug Fixes

* create topics again — NewTopic never got a usable argument set ([#23](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/23)) ([f0dd3ce](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/f0dd3cecf52cc264bbe5a1ab067ae85de897e095))

## [0.1.2](https://github.com/bedrock-python/aiokafka-foundation-kit/compare/aiokafka-foundation-kit-v0.1.1...aiokafka-foundation-kit-v0.1.2) (2026-09-06)


### Bug Fixes

* the defects the agents page turned up ([#20](https://github.com/bedrock-python/aiokafka-foundation-kit/issues/20)) ([3bd7c8a](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/3bd7c8a7a6f1e0e619072b071f4229c592a680b9))

## [0.1.1](https://github.com/bedrock-python/aiokafka-foundation-kit/compare/aiokafka-foundation-kit-v0.1.0...aiokafka-foundation-kit-v0.1.1) (2026-09-05)


### Bug Fixes

* **publish:** trigger via workflow_run instead of release event ([6e4c5db](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/6e4c5dbb464767f066319620ec687eb2c6541a15))

## 0.1.0 (2026-05-24)


### Features

* initial release ([63b1c35](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/63b1c355d659c44f52cd12d228c1f60898305ba5))


### Bug Fixes

* **models:** explicit str annotation for sasl_password secret value ([48ef080](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/48ef0800a983be05acb6c9fa25fdc6a2344cb3d6))
* **publish:** set version from release tag before build, drop extra-files from release-please ([2ad66d9](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/2ad66d91a682578af465fce913587b176e803ee0))


### Documentation

* add CI, codecov, and docs badges to README ([b583615](https://github.com/bedrock-python/aiokafka-foundation-kit/commit/b583615c75286425bb9fcce888acd198e1cbf609))

## Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

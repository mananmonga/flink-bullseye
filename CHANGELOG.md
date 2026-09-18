# Changelog

## Unreleased

Initial extraction.

- `barrier-core`: `SideInputBarrier` builder, `Lane`, `EndOffsetProbe`, `BarrierUids`; internal
  readiness tracker, gate, epoch math and readiness predicate.
- `barrier-kafka`: `KafkaEndOffsetProbe`, `KafkaEnvelope`, `KafkaEnvelopeDeserializer`.
- Flink 1.20.0, Java 17 bytecode, Flink and Kafka as `compileOnly`.

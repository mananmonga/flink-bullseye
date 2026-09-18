# Changelog

## Unreleased

Initial extraction.

- `bullseye-core`: `Bullseye` builder, `Lane`, `EndOffsetProbe`, `BullseyeUids`; internal
  readiness tracker, gate, epoch math and readiness predicate.
- `bullseye-kafka`: `KafkaEndOffsetProbe`, `KafkaEnvelope`, `KafkaEnvelopeDeserializer`.
- Flink 1.20.0, Java 17 bytecode, Flink and Kafka as `compileOnly`.

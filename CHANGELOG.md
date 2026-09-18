# Changelog

## Unreleased

Initial extraction.

- `bullseye-core`: `Bullseye` builder, `Lane`, `EndOffsetProbe`, `BullseyeUids`; internal
  readiness tracker, gate, epoch math and readiness predicate.
- `bullseye-kafka`: `KafkaEndOffsetProbe`, `KafkaEnvelope`, `KafkaEnvelopeDeserializer`.
- Flink 1.20.0, Java 17 bytecode, Flink and Kafka as `compileOnly`.
- `evalGateReleased` counts every record leaving the gate (drained and pass-through), matching the
  originating job's semantics (#1).
- `idleBackstop` default is 5 minutes, up from the inherited 30s; the first idle release logs at
  ERROR (#3).
- API-surface test asserts no `.internal` type appears in a public signature (#2).

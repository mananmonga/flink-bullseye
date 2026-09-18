# flink-bullseye

A periodic **re-sync barrier** for keyed-state side inputs in Apache Flink DataStream jobs.

When a job enriches a main stream from state built by a companion topic (rules, memberships,
reference data), there is a cold-start race: the main stream is consumed and evaluated before the
side-input topic has been replayed into state. The result is not an error. It is a silent wrong
answer: rules that never fire, enrichments stamped empty, records held forever in an unbounded
per-key buffer.

This library holds the main stream until every gated side-input lane has been consumed to the end
offsets probed at the start of the current epoch, then releases, and re-arms every interval. That
turns "complete once at t=0" into **bounded staleness**, and it is the only variant that also
detects a consumer that silently drifts behind later.

- **Flink 1.20**, Java 17 bytecode, Apache-2.0.
- `bullseye-core` depends on Flink only. `bullseye-kafka` adds a Kafka end-offset probe and an
  offset-carrying envelope. A Pulsar or Fluss user implements one interface and skips the Kafka
  module entirely.
- Flink is `compileOnly`. Nothing here ends up in your shaded jar except this library.

## Install

```kotlin
dependencies {
    implementation("io.github.mananmonga:bullseye-core:0.1.0")
    implementation("io.github.mananmonga:bullseye-kafka:0.1.0")   // Kafka users only
}
```

Both artifacts are published to Maven Central. Your build already provides Flink and, if you use
`bullseye-kafka`, `flink-connector-kafka` with its `kafka-clients`.

## Use

```java
Map<String, Object> kafka = Map.of("bootstrap.servers", brokers, "group.id", "my-job-probe");

// Side inputs come in as envelopes that carry partition and offset.
KafkaSource<KafkaEnvelope<Rule>> rulesSource = KafkaSource.<KafkaEnvelope<Rule>>builder()
        .setDeserializer(KafkaEnvelopeDeserializer.of(new RuleSchema()))
        .setTopics("rules")
        ...build();
DataStream<KafkaEnvelope<Rule>> rules = env.fromSource(rulesSource, noWatermarks(), "rules");

// Partition counts are probed once, at submission. A lane that cannot be counted must not submit.
int rulePartitions = KafkaEndOffsetProbe.partitionCount("rules", kafka);

DataStream<Observation> gated = Bullseye.forType(TypeInformation.of(Observation.class))
        .lane(Lane.of("rule", rulePartitions, KafkaEndOffsetProbe.of("rules", kafka)),
              rules, KafkaEnvelope::getPartition, KafkaEnvelope::getOffset)
        .barrierUid("eval-gate")                 // required, frozen: your savepoint contract
        .syncInterval(Duration.ofHours(1))       // re-sync cadence = staleness bound
        .apply(observations);

// The joins that READ side-input state go AFTER the barrier.
gated.keyBy(Observation::key)
     .connect(rules.map(KafkaEnvelope::getValue).broadcast(RULES))
     .process(new ApplyRules());
```

`enabled(false)` keeps the operator graph and every uid intact but never holds. Use it as the
rollback switch.

### Builder reference

| Method | Default | Meaning |
| --- | --- | --- |
| `barrierUid(String)` | **required** | Operator uid of the barrier, used verbatim. |
| `lane(Lane, DataStream, partitionOf, offsetOf)` | at least one | A gated side input, registered pre-`keyBy`. |
| `syncInterval(Duration)` | 1h | Epoch length. Bounds how stale a lane may silently become. |
| `maxBuffered(int)` | 500,000 | Records held **per subtask**. Multiply by parallelism for the heap ceiling. |
| `failOnOverflow(boolean)` | false | `true` fails the task on overflow; `false` releases early with a WARN. |
| `idleBackstop(Duration)` | 30s | How long a subtask may be held before it releases with a WARN. |
| `clock(Clock)` | system UTC | Test seam. Must be `Serializable` and agree with processing time. |
| `enabled(boolean)` | true | `false` is a passthrough that keeps uids and drains restored state. |

### Operator uids (frozen)

| Operator | uid |
| --- | --- |
| the barrier | `barrierUid` verbatim |
| per lane, offset stripper | `<lane.id>-offsets` |
| per lane, readiness tracker | `<lane.id>-readiness` |

State names are frozen too: operator list state `barrierBuffer`, broadcast state `readyState`,
tracker keyed state `highestOffset` and `declaredEpoch`. Pin the uid set in a test in your job;
`BullseyeTest.uidDerivationReproducesTheFrozenContract` shows how.

### Metrics

Counters `evalGateHeld`, `evalGateReleased`, `evalGateOverflow`, `evalGateIdleRelease`; gauges
`evalGateWaiting` (buffered records on this subtask) and `evalGateOpen` (0/1). Per lane, under
`bullseye.lane.<id>`: `probes`, `probeFailures`, `markers`.

## What it guarantees, and what it does not

- **Bounded staleness, not point-in-time correctness.** If you have usable event time on both
  sides, a temporal or versioned table join (`FOR SYSTEM_TIME AS OF`) is the sanctioned answer and
  makes the race semantically impossible. This library is for processing-time jobs over compacted
  changelog topics with no usable event time.
- **It composes with batch bootstrap.** Bootstrapping state in BATCH mode and restoring (FLIP-605,
  or the State Processor API) is strictly better for a pure cold start. It says nothing about a
  consumer that drifts behind at t+6h. Use both.
- **It is not a hard fence.** A marker means "consumed to target", not "state applied". The side
  input's state lives downstream of a `keyBy`, so a record released exactly at a boundary can race
  with side-input records still in flight. The race is heavily one-sided but is not a guarantee.
- **Readiness is offset completeness, not first-record arrival.** FLIP-17 proposed the latter, and
  it is weaker in exactly the way that produces the bug: a partially loaded side input looks ready.
- **Releases happen on events.** The barrier is a non-keyed operator with no timers. A buffered
  record leaves when the next main record or readiness marker reaches that subtask.
- **The buffer is operator `ListState` of your type.** If your type falls back to Kryo, find out
  from your serializer warnings before production.
- **Transactional producers.** Kafka's end offset sits one past the commit marker, which no consumer
  ever receives. A lane fed by a transactional producer never reaches its target with the stock
  probe. Wrap the probe or write your own.
- **A lane whose partitions are all empty** never reports and is released by the idle backstop
  with a WARN each epoch. That is a deliberate "is this side input wired?" signal. Partitions that
  are empty while others are not are handled and declare ready.

## Design

[docs/design.md](docs/design.md) holds the decisions, prior art (FLIP-17, FLIP-467, FLIP-309/327,
FLIP-605), the invariants that must not regress, and the Flink 2.x plan (FLIP-467 generalized
watermarks as the readiness transport, behind a seam that is already internal).

## Build

```bash
./gradlew build
```

Builds on JDK 21, emits Java 17 bytecode. CI additionally publishes to `mavenLocal` from JDK 21 and
compiles and runs `smoke-consumer/` on a JDK 17 runtime so the bytecode target cannot regress.

## Release

Push a tag `vX.Y.Z`. GitHub Actions builds, tests, signs and publishes to Maven Central through
the Central Portal. Secrets required: `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`,
`SIGNING_KEY`, `SIGNING_KEY_PASSWORD`. No `SNAPSHOT` pins in consumers; no laptop publishing.

## License

Apache-2.0.

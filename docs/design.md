# Design

This document is the public form of the internal decision record that motivated the extraction.
It is deliberately self-contained.

## Context

Flink has no first-class answer for **a keyed-state side input populated from a second stream**.
When a job enriches a main stream from state built by a companion topic, there is a cold-start race:
the main stream is consumed and evaluated before the side-input topic has been replayed into state.
The result is not an error, it is a silent wrong answer. In the job this was extracted from it
showed up as scoped rules that never fired (an empty scope list stamped on every record) and as
records held forever in an unbounded per-key buffer waiting for enrichment that had already gone by.

The fix is a **periodic re-sync barrier**: hold the main stream until every gated side-input lane
has been consumed to the end offsets probed at the start of the current epoch, then release; re-arm
every interval. It changes the guarantee from "complete once at t=0" to **bounded staleness**, and
it is the only variant that also detects a consumer silently drifting behind.

The problem recurred across independently written jobs (one grew a hold-until-dependency-present
gate with a timer, a buffer cap and a dead-letter queue; three others read stream-populated keyed
state with no gate at all). Vendors asked in September 2026 had no answer. The
`EndOffsetProbe` seam is what makes the barrier reusable outside Kafka.

## Prior art

Checked 2026-09-10. Short answer: partly, and not the part built here.

**FLIP-17: Side Inputs for DataStream API** is the same problem, designed and abandoned (c. 2016; no
release version was ever filled in). It proposes a `RecordBuffer` holding main-input elements in
operator state, structurally the same choice as this gate's `ListState`. Its readiness predicate,
however, is "we consider side input ready as soon as the first data arrives for that side input".
That is strictly weaker than offset completeness, and weaker in precisely the way that produced the
bug this barrier fixes: the first record arriving says nothing about completeness, so a partially
loaded side input still yields a wrong answer. This library is the dormant FLIP with the readiness
predicate fixed.

**Table/SQL has an answer; DataStream does not.** Temporal and versioned table joins
(`FOR SYSTEM_TIME AS OF`) make the race semantically impossible, but require usable event time on
both sides. The broadcast state docs concede the DataStream gap directly: read-write access only on
the broadcast side, and no ordering guarantee between the two inputs.

**FLIP-605: state catch-up using BATCH execution mode** (dev@ discussion, August 2026). Run the job
in BATCH over the backlog, snapshot, restore in STREAMING. Better than any barrier for a *pure* cold
start, and the modern form of the State Processor API approach. But it makes a claim about t=0 only
and cannot detect a consumer that drifts behind later. Complementary, not competing.

**FLIP-467: generalized watermarks** (shipped in Flink 2.0). `LongWatermark` and `BoolWatermark`
propagate through the operator graph, combined across channels with MIN/MAX/AND/OR, plus a
`combineWaitForAllChannels` flag that gates propagation until every input channel has signalled.
That is the 2.x-native spelling of the per-partition-marker-plus-broadcast mechanism this barrier
hand-rolls on 1.20: a `LongWatermark` carrying the epoch, MIN combine, wait-for-all enabled. It is
the intended v2 transport; see D9.

**FLIP-309 / FLIP-327: `isProcessingBacklog`.** A source-level "am I caught up" signal. The closest
thing in Flink to the per-topic completeness primitive needed here, but derived from watermark lag
or source state (event time again), and it exists to size checkpoint intervals and switch execution
modes, not to gate records.

**What none of them provide:** offset completeness computed pre-`keyBy`, per partition, against a
probed high-watermark. FLIP-467 supplies a channel to send readiness on but has no opinion on what
readiness means; FLIP-309 defines it by watermark lag; FLIP-17, FLIP-605 and the State Processor API
are bootstrap-shaped and say nothing about steady-state drift. The readiness predicate and the epoch
re-sync framing are the contribution. The plumbing is not.

- FLIP-17: <https://cwiki.apache.org/confluence/display/FLINK/FLIP-17:+Side+Inputs+for+DataStream+API>
- Broadcast state: <https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/>
- FLIP-467: <https://cwiki.apache.org/confluence/display/FLINK/FLIP-467:+Introduce+Generalized+Watermarks>
- FLIP-309: <https://cwiki.apache.org/confluence/display/FLINK/FLIP-309:+Support+using+larger+checkpointing+interval+when+source+is+processing+backlog>
- FLIP-327: <https://cwiki.apache.org/confluence/display/FLINK/FLIP-327:+Support+switching+from+batch+to+stream+mode+to+improve+throughput+when+processing+backlog+data>
- FLIP-605 discussion: <http://www.mail-archive.com/dev@flink.apache.org/msg88077.html>

## Decisions

**D1. Separate repository, Maven Central.** Both original consumers already resolve from Central,
so no second repository block and no personal access token. Personal-account GitHub Packages was
rejected: production builds would authenticate against a PAT the organisation cannot rotate, audit
or inherit.

**D2. Two modules.** `bullseye-core` is Flink-only. `bullseye-kafka` holds the end-offset probe and
the offset-carrying envelope. `EndOffsetProbe` is free of Flink and Kafka types, so a non-Kafka user
implements one interface without pulling the Kafka client.

**D3. Java 17 bytecode.** One consumer is on 21, another on 17. The build sets `options.release = 17`
explicitly. A 21 toolchain without it compiles and publishes fine and then fails the 17 consumer's
build with an opaque class-version error, so CI publishes from 21 and compiles and runs a smoke
consumer on a 17 runtime.

**D4. Flink is `compileOnly` and `testImplementation`, never `implementation`.** Consumers build
shaded fat jars. A library declaring Flink as a real dependency pulls it into that jar and collides
with the cluster classpath.

**D5. Flink 1.20 only for v1.** No version matrix for two consumers under one team's control. This
is what makes D9 load-bearing: Flink 2.0 ships the sanctioned transport, so v1 must be shaped to
swap onto it without a breaking change.

**D6. Zero dependency on the originating schemas.** The test of whether the extraction is clean.
Product-specific details came out on the way: the broker ACL note became a caller-supplied
`group.id`.

**D7. Operator uids are caller-supplied and frozen.** A library must never invent a uid it could
silently change between versions; a rename makes every consumer's savepoint unrestorable.
`barrierUid` is required and per-lane uids derive deterministically from the lane id. The
derivation is documented as frozen (`BullseyeUids`). State names and metric names are frozen for the
same reason.

**D8. Per-key bounded holds stay in the consumer.** A "hold this key for 30s or one epoch, then
give up" behaviour is join semantics, not barrier semantics, and shipping it would mean shipping
opinions about dead-lettering. Out of scope for v1.

**D9. Readiness transport sits behind an internal seam.** Marker delivery never appears in the
public API. On 1.20 it is a broadcast stream; on 2.x it should be FLIP-467 generalized watermarks.
Concretely: `Bullseye.apply()` owns all wiring and never accepts or returns the marker
stream; `LaneReady`, `LaneOffset` and the broadcast state descriptor are `@Internal`; and the "has
every expected partition declared epoch ≥ N" predicate lives in one place (`Readiness`) with no
Flink transport types in its signature. If the transport is swappable, v2 is a transport change; if
`.broadcast(...)` were baked into the builder's public surface, v2 would be a rewrite.

## Public API

One builder (`Bullseye`), one value type (`Lane`), one interface (`EndOffsetProbe`), one
uid helper (`BullseyeUids`). Everything else is `@Internal`.

Decisions worth stating:

1. `barrierUid` is mandatory with no default. Free today, unfixable later.
2. `TypeInformation<T>` is explicit. It names the operator-state buffer's serializer; if the
   adopter's type falls back to Kryo they need to learn that before production.
3. `Lane` carries its own partition count *and* its own probe. Kept separately it is possible to
   register a lane with no probe; one value object makes "a lane cannot look ready merely because
   none of its partitions have reported" structurally true.
4. `Lane.id` is a validated slug `String`. A closed enum would make "a marker can never name a lane
   that isn't real" a type-level property, but that cannot survive an open lane set.
5. No marker stream in the public API (D9).
6. `Clock` seam. Acceptable to call `System.currentTimeMillis()` in a job; not in a library other
   people write tests against. The clock must be `Serializable` and agree with the processing-time
   service the tracker's timers run on.

## Invariants that must not regress

Each is load-bearing and non-obvious. A well-meaning simplification breaks each one silently.

1. **Offset completeness is not computable after a `keyBy` on a business key.** A subtask only sees
   records whose key hashes to it, so it can never observe a partition's max offset. Readiness is
   computed pre-`keyBy`, keyed by *partition*, and delivered globally by broadcast. The barrier sits
   upstream of the keyed joins; a gate downstream of the join is structurally too late.
2. **One marker per (lane, partition) per epoch, never per record.** Reporting every record's offset
   forces a choice between broadcasting one marker per side-input record (amplified by parallelism)
   and throttling. Throttling is unsafe: a stream that goes quiet after a throttled emit leaves the
   barrier believing the lane is further behind than it is, holding the main stream indefinitely.
3. **The idle backstop measures hold duration, not marker silence.** A partition emits at most one
   marker per epoch, so with a 1h cadence and a 30s backstop the last marker is essentially always
   stale; every rollover would idle-release during normal operation. Worse, a genuinely drifting
   consumer emits no fresh marker either, so the barrier would idle-release in exactly the case it
   exists to catch. Anchor to "how long has this subtask actually been stuck".
4. **Epochs are derived (`floorDiv(now, interval)`), not coordinated.** No leader, no handshake, no
   shared counter; subtasks milliseconds apart still agree.
5. **A failed probe returns `null` and keeps the previous targets.** Never throw (fails the task),
   never guess high (holds against a target that may not exist), never guess low (opens on a claim
   we cannot support). Re-probing backs off for a few seconds and happens on the next undeclared
   partition's record, not on every record.
6. **Absence counts as not-ready.** Expected partition counts are probed at submission rather than
   inferred from markers seen so far. Partitions the probe reports as empty are declared ready on
   their behalf by whichever key first computes the epoch's targets; a lane whose partitions are
   *all* empty has no key and is left to the idle backstop as a deliberate signal.
7. **Readiness must be reportable when a lane is quiet**, which is why the tracker is keyed (Flink
   only offers timers in a keyed context). The tracker re-arms a timer at the next epoch boundary
   on every evaluation. The quiet case is the common one and resolves instantly.
8. **The probe result is cached per epoch across keys in a subtask.** Without it, a partition that
   is behind (the bootstrap case) probes on every record, each probe building a client, making two
   blocking round-trips and closing it, on the task thread.
9. **`drain()` runs unconditionally on the open path.** It covers an idle release, an overflow
   release, and a redeploy with the gate *disabled* after a last-state restore, where `isOpen()` is
   trivially true and the restored buffer would otherwise be silently dropped while `snapshotState`
   kept rewriting it into every checkpoint. `enabled(false)` therefore keeps the whole operator
   graph, uids included.
10. **`maxBuffered` is per subtask.** Multiply by parallelism for the real heap ceiling.
11. **The barrier is not a hard fence.** A marker means "consumed to target", not "state applied";
    the side-input state lives downstream of a `keyBy`, so a record released exactly at a boundary
    races with side-input records still in flight. Heavily one-sided, not a guarantee.
12. **Gating and splitting stay separate operators.** Folding a side-output split into the barrier
    fails: draining must be callable from the broadcast side, and a broadcast-side `Collector` has
    no `output(OutputTag, ...)`.
13. **Overflow and idle releases force the gate open for the rest of the epoch.** Releasing and
    immediately re-holding would overflow again within seconds and turn a degradation into a
    sawtooth. The gate re-arms at the next boundary.

## Publishing

- **Group id.** `io.github.mananmonga` is the no-domain fallback the Central Portal grants a verified
  GitHub account. An owned domain is preferable and can be swapped in `gradle.properties` before
  the first release; after it, the coordinate is frozen by the consumers that depend on it.
- **Artifact ids** are the module names, `bullseye-core` and `bullseye-kafka`. Also frozen after the
  first release.
- **Apache-2.0 from the first commit**, so any organisation can fork and republish under its own
  group id without needing anything from the original author.
- **Tag-triggered publish** from GitHub Actions to the Central Portal. Releases are not a laptop
  ritual; `publishToMavenLocal` exists only for the smoke consumer.
- **Bus factor.** Share the Portal namespace and the GPG key with a second person on day one. Doing
  it later means regenerating a key that has already signed published artifacts.
- **No `SNAPSHOT` pins in production consumers.**

## Migration order for an existing job

1. Publish `0.1.0`.
2. Swap the originating job to the dependency. Its operator-uid pin test must pass unchanged; the
   uid strings are the contract. Match the state descriptor names (`barrierBuffer`, `readyState`,
   `highestOffset`, `declaredEpoch`) to the job's existing ones before the first stateful deploy,
   or accept a one-time `allowNonRestoredState` restore.
3. Pilot one second consumer that has the cold-start race and no gate.
4. Only then public promotion: write-up, then possibly a FLIP if adoption appears.

A per-key ordering gate with a dead-letter queue is **not** a consumer of this library: its
readiness signal is "the dependency record arrived", not "the topic has been consumed to a probed
high-watermark".

## Anticipated objections

1. *"Why not watermark alignment plus a temporal/versioned table join?"* That is the sanctioned
   answer to one input racing ahead, and `FOR SYSTEM_TIME AS OF` gives point-in-time-correct
   lookups. This library is for a processing-time DataStream job over compacted changelog topics
   with no usable event time, wanting bounded staleness with periodic re-sync rather than
   point-in-time determinism.
2. *"Why not bootstrap state in BATCH mode and restore (FLIP-605, State Processor API)?"* For pure
   cold start that is strictly better. It makes a claim about t=0 only and cannot catch a consumer
   drifting behind later. The two compose.
3. *"On Flink 2.0 this is FLIP-467."* FLIP-467 is the right transport and the plan is to adopt it
   (D9). It does not define what readiness means, which is the actual content here.

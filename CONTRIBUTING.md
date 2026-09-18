# Contributing

- `./gradlew build` must pass with no new compiler warnings (`-Werror` is on).
- Anything that changes an operator uid, a state name or a metric name is a breaking change and
  needs a major version. `BullseyeUids` and `docs/design.md` say why.
- Read the invariants in `docs/design.md` before simplifying the tracker or the gate. Each one
  exists because the obvious simplification broke silently.
- Keep `bullseye-core` free of Kafka types and both modules free of `implementation` dependencies on
  Flink.

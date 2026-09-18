# Contributing

- `./gradlew build` must pass with no new compiler warnings (`-Werror` is on).
- Anything that changes an operator uid, a state name or a metric name is a breaking change and
  needs a major version. `BullseyeUids` and `docs/design.md` say why.
- Read the invariants in `docs/design.md` before simplifying the tracker or the gate. Each one
  exists because the obvious simplification broke silently.
- Keep `bullseye-core` free of Kafka types and both modules free of `implementation` dependencies on
  Flink.
- Any launcher JDK 17 or newer works. The wrapper pins Gradle 9.x and `gradle/gradle-daemon-jvm.properties`
  pins the daemon to a JDK 21 that Gradle provisions itself, so the JVM on your `PATH` only launches
  the client. If you ever see a build fail with nothing but a bare version string such as `25.0.3`
  as the error, that is an older Gradle's embedded Kotlin compiler choking on a new JDK; run
  `./gradlew --version` and make sure the wrapper is what is running.

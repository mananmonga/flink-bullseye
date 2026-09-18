val flinkVersion: String by rootProject.extra

description = "Side-input re-sync barrier for Flink DataStream jobs (Flink only, no Kafka client)."

dependencies {
    // D4: never `implementation`. Consumers ship shaded fat jars and the cluster owns Flink.
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")

    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-clients:$flinkVersion")
}

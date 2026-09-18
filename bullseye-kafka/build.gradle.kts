val flinkVersion: String by rootProject.extra
val kafkaConnectorVersion: String by rootProject.extra
val kafkaClientsVersion: String by rootProject.extra

description = "Kafka end-offset probe and offset-carrying envelope for flink-bullseye."

dependencies {
    api(project(":bullseye-core"))

    // Bring-your-own connector: the consumer already has flink-connector-kafka and kafka-clients on
    // its classpath, and pinning them here would fight the version its connector wants.
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    compileOnly("org.apache.kafka:kafka-clients:$kafkaClientsVersion")

    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
}

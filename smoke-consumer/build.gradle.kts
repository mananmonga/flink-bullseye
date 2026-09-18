// Consumer-side smoke test for D3 (Java 17 bytecode). Not part of the main build and never
// published. Run from CI's `consume-on-17` job, or locally:
//   ../gradlew publishToMavenLocal -PVERSION_NAME=0.0.0-smoke   (from the repo root)
//   ../gradlew run -PbarrierVersion=0.0.0-smoke                  (from this directory, JDK 17)
plugins {
    application
}

val barrierVersion: String = (findProperty("barrierVersion") as String?) ?: "0.0.0-smoke"
val flinkVersion = "1.20.0"

repositories {
    mavenLocal()
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

dependencies {
    implementation("io.github.mananmonga:barrier-core:$barrierVersion")
    implementation("io.github.mananmonga:barrier-kafka:$barrierVersion")
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")
}

application {
    mainClass.set("smoke.SmokeJob")
}

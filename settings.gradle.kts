plugins {
    // Auto-provisions the JDK requested by the toolchain block if it is not installed locally.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.9.0"
}

rootProject.name = "flink-side-input-barrier"

include("barrier-core", "barrier-kafka")

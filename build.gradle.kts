import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar

plugins {
    id("com.vanniktech.maven.publish") version "0.37.0" apply false
}

val flinkVersion by extra("1.20.0")
val kafkaConnectorVersion by extra("3.4.0-1.20")
val kafkaClientsVersion by extra("3.4.0")
val slf4jVersion by extra("1.7.36")
val junitVersion by extra("5.13.4")
val assertjVersion by extra("3.27.7")

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "com.vanniktech.maven.publish")

    group = property("GROUP") as String
    version = property("VERSION_NAME") as String

    repositories {
        mavenCentral()
    }

    configure<JavaPluginExtension> {
        toolchain {
            // Build on 21 (what the toolchain provisions), but emit Java 17 bytecode: one consumer
            // targets 17. `release` (not `sourceCompatibility`) is what actually guards this, because
            // it also pins the JDK API surface. See docs/design.md D3 and the `consume-on-17` CI job.
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(17)
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-processing", "-Werror"))
    }

    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).apply {
            addBooleanOption("Xdoclint:none", true)
            addStringOption("Xmaxwarns", "1")
            encoding = "UTF-8"
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        maxHeapSize = "1g"
        testLogging {
            events("failed", "skipped")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    dependencies {
        "compileOnly"("org.slf4j:slf4j-api:$slf4jVersion")
        "testImplementation"(platform("org.junit:junit-bom:$junitVersion"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
        "testImplementation"("org.assertj:assertj-core:$assertjVersion")
        "testImplementation"("org.slf4j:slf4j-api:$slf4jVersion")
        "testRuntimeOnly"("org.slf4j:slf4j-simple:$slf4jVersion")
    }

    // Coordinates and POM come from gradle.properties (GROUP, VERSION_NAME, POM_*); the plugin
    // reads them itself. artifactId defaults to the module name.
    configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
        configure(JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = true))
        // Sign only when a key is supplied (CI release job passes ORG_GRADLE_PROJECT_signingInMemoryKey),
        // so publishToMavenLocal works on a laptop and in the consume-on-17 job without GPG.
        if (findProperty("signingInMemoryKey") != null) {
            signAllPublications()
        }
    }
}

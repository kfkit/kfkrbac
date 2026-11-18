plugins {
    java
    id("me.champeau.jmh") version "0.7.3"
}

group = "dev.kfkit"
version = "0.1.0-SNAPSHOT"

// The broker provides the Kafka classes at runtime. Compile against the
// newest supported release.
val kafkaVersion = "4.4.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.kafka:kafka-metadata:$kafkaVersion")
    compileOnly("org.apache.kafka:kafka-clients:$kafkaVersion")
    compileOnly("org.apache.kafka:kafka-server-common:$kafkaVersion")
    // The broker ships an SLF4J; Kafka's own POM has it at runtime scope only.
    compileOnly("org.slf4j:slf4j-api:1.7.36")

    testImplementation("org.apache.kafka:kafka-metadata:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-server-common:$kafkaVersion")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")

    jmh("org.apache.kafka:kafka-metadata:$kafkaVersion")
    jmh("org.apache.kafka:kafka-clients:$kafkaVersion")
    jmh("org.apache.kafka:kafka-server-common:$kafkaVersion")
}

// The code JMH generates is not ours to keep free of warnings.
tasks.withType<JavaCompile>().matching { it.name != "jmhCompileGeneratedClasses" }.configureEach {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
    options.encoding = "UTF-8"
}

// ./gradlew jmh: the cost of authorize(), in nanoseconds per call.
jmh {
    jmhVersion = "1.37"
    fork = 1
    warmupIterations = 3
    iterations = 5
    timeOnIteration = "2s"
    warmup = "2s"
    benchmarkMode = listOf("avgt")
    timeUnit = "ns"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
            "Implementation-Vendor" to "kfkit",
        )
    }
}

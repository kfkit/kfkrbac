plugins {
    java
    `maven-publish`
    id("me.champeau.jmh") version "0.7.3"
}

group = "dev.kfkit"
// The version is in gradle.properties; a release sets it from the tag with
// -Pversion=X.Y.Z, which only works while this file does not assign it.

// The broker provides the Kafka classes at runtime. Compile against the
// newest supported release.
val kafkaVersion = "4.4.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
    // What Maven Central asks for next to the jar.
    withSourcesJar()
    withJavadocJar()
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
    // RoleTest compares these with the code: a change to them alone reruns it.
    inputs.files("docs/roles.md", "README.md")
        .withPropertyName("docs")
        .withPathSensitivity(PathSensitivity.RELATIVE)
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

tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        addBooleanOption("Xdoclint:all,-missing", true)
        addBooleanOption("Werror", true)
    }
}

// ./gradlew publishToMavenLocal: the jar, the sources, the javadoc and a POM
// with what Maven Central requires. Nothing publishes to Central yet.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name = "kfkrbac"
                description = "Role-based access control for Apache Kafka: the broker's " +
                    "StandardAuthorizer plus roles and role bindings."
                url = "https://github.com/kfkit/kfkrbac"
                licenses {
                    license {
                        name = "Apache-2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                        distribution = "repo"
                    }
                }
                developers {
                    developer {
                        id = "iabramov"
                        name = "Ivan Abramov"
                        url = "https://github.com/iabramov"
                    }
                }
                scm {
                    url = "https://github.com/kfkit/kfkrbac"
                    connection = "scm:git:https://github.com/kfkit/kfkrbac.git"
                    developerConnection = "scm:git:ssh://git@github.com/kfkit/kfkrbac.git"
                }
                issueManagement {
                    system = "GitHub"
                    url = "https://github.com/kfkit/kfkrbac/issues"
                }
            }
        }
    }
}

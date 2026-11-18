import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import java.security.MessageDigest

plugins {
    java
    `jvm-test-suite`
    `maven-publish`
    id("me.champeau.jmh") version "0.7.3"
    // Only its task type is used, to relocate kfkwho's classes. Applied, the
    // plugin would build and publish a second jar next to the one that ships.
    id("com.gradleup.shadow") version "9.6.1" apply false
}

group = "dev.kfkit"
// The version is in gradle.properties; a release sets it from the tag with
// -Pversion=X.Y.Z, which only works while this file does not assign it.

// The broker provides the Kafka classes at runtime. Compile against the
// newest supported release.
val kafkaVersion = "4.4.0"

// What a Connect worker of that release has on its class path.
val jakartaRsVersion = "3.1.0"
val jacksonVersion = "2.21.7"
val jerseyVersion = "3.1.10"

// The broker image the integration tests run the jar on: -PbrokerVersion=4.1.2.
val brokerVersion = providers.gradleProperty("brokerVersion").getOrElse(kafkaVersion)

// kfkwho, whose access metrics the authorizer publishes on request, is a
// dependency and never a copy: its released jar, with its classes relocated
// into this project's package, is part of the jar that ships. The version is
// declared here and nowhere else in the build. What is fetched is checked
// against these SHA-256 before anything is built from it, so a new version
// needs them too: of the jar, the content of kfkwho-<version>.jar.sha256 in
// the release; of the exporter rules the integration tests use,
// `shasum -a 256 jmx-exporter/kfkwho.yml` at the tag.
val kfkwhoVersion = "0.1.0"
val kfkwhoSha256 = "6d79dd82f5a2dc4af21305977902b79d37f0c307b7457ea0e53206aaba7de88a"
val kfkwhoRulesSha256 = "e520ea65994a20218901acc20df38678f4df221ce1651b75085712940762cf65"
val kfkwhoRepository = "https://github.com/kfkit/kfkwho"
val kfkwhoPackage = "dev.kfkit.kfkwho"
val relocatedKfkwhoPackage = "dev.kfkit.kfkrbac.shaded.kfkwho"

// Fails the build when a file that was fetched is not the one kfkwho released.
val requireReleased: (File, String, String, String) -> Unit = { file, expected, setting, version ->
    val actual = MessageDigest.getInstance("SHA-256").digest(file.readBytes())
        .joinToString("") { "%02x".format(it) }
    if (actual != expected) {
        throw GradleException(
            "${file.name} is not the file kfkwho $version released: its SHA-256 is $actual and the build "
                + "expects $expected ($setting in build.gradle.kts). It was not used. If the release "
                + "was replaced on purpose, change $setting; if not, look at what was fetched: $file"
        )
    }
}

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
    // kfkwho is released on GitHub and is not on Maven Central yet. Its jar is
    // looked for in its release and its exporter rules at the release's tag,
    // nowhere else, and nothing else is looked for there. There is no
    // metadata: the files are all there is.
    exclusiveContent {
        forRepository {
            ivy {
                name = "kfkwhoReleases"
                url = uri(kfkwhoRepository)
                patternLayout {
                    artifact("releases/download/v[revision]/[module]-[revision].[ext]")
                    artifact("raw/v[revision]/jmx-exporter/[module].[ext]")
                }
                metadataSources {
                    artifact()
                }
            }
        }
        filter {
            includeModule("dev.kfkit", "kfkwho")
        }
    }
}

// The jar of kfkwho as it was released. Only relocatedKfkwho reads it.
val kfkwho = configurations.create("kfkwho") {
    isCanBeConsumed = false
    isTransitive = false
}

// kfkwho's classes in this project's package, so that a broker which also has
// kfkwho's own jar in libs/ loads each from its own. Everything here that
// uses kfkwho, the tests and the benchmarks included, uses this jar: what is
// tested is what ships. The authorizer of kfkwho itself is left out; only
// the recorder and what it needs are bundled.
val relocatedKfkwho = tasks.register<ShadowJar>("relocatedKfkwho") {
    description = "Checks the released kfkwho jar and relocates its classes into $relocatedKfkwhoPackage."
    val released: FileCollection = kfkwho
    val version = kfkwhoVersion
    val expected = kfkwhoSha256
    val check = requireReleased
    configurations = listOf(kfkwho)
    relocate(kfkwhoPackage, relocatedKfkwhoPackage)
    exclude("META-INF/**")
    exclude("${kfkwhoPackage.replace('.', '/')}/MeteredStandardAuthorizer.class")
    archiveFileName = "kfkwho-$kfkwhoVersion-relocated.jar"
    destinationDirectory = layout.buildDirectory.dir("kfkwho")
    // A changed checksum checks the same file again.
    inputs.property("sha256", expected)
    doFirst {
        check(released.singleFile, expected, "kfkwhoSha256", version)
    }
}

dependencies {
    compileOnly("org.apache.kafka:kafka-metadata:$kafkaVersion")
    compileOnly("org.apache.kafka:kafka-clients:$kafkaVersion")
    compileOnly("org.apache.kafka:kafka-server-common:$kafkaVersion")
    // The broker ships an SLF4J; Kafka's own POM has it at runtime scope only.
    compileOnly("org.slf4j:slf4j-api:1.7.36")
    // The extension for Kafka Connect: the worker provides these. None of them
    // goes into the jar; a bundled jakarta.* breaks the worker.
    compileOnly("org.apache.kafka:connect-api:$kafkaVersion")
    compileOnly("jakarta.ws.rs:jakarta.ws.rs-api:$jakartaRsVersion")
    // Jackson is in libs/ of the Kafka distribution too, from 4.1 on: the command reads
    // the files of apply and import with its streaming parser (jackson-core), and with
    // nothing that 2.19, which Kafka 4.1 has, does not have.
    compileOnly("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    // Inside the jar, see tasks.jar: not a dependency of what is published.
    compileOnly(files(relocatedKfkwho))

    testImplementation("org.apache.kafka:kafka-metadata:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-server-common:$kafkaVersion")
    testImplementation("org.apache.kafka:connect-api:$kafkaVersion")
    testImplementation("jakarta.ws.rs:jakarta.ws.rs-api:$jakartaRsVersion")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    // Builds the responses the filter aborts with, as the worker's own JAX-RS does.
    testRuntimeOnly("org.glassfish.jersey.core:jersey-common:$jerseyVersion")
    // The admin client with a mock network underneath: what the command sends and how it reads a refusal.
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion:test-fixtures")
    testImplementation(files(relocatedKfkwho))
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")

    jmh("org.apache.kafka:kafka-metadata:$kafkaVersion")
    jmh("org.apache.kafka:kafka-clients:$kafkaVersion")
    jmh("org.apache.kafka:kafka-server-common:$kafkaVersion")
    jmh(files(relocatedKfkwho))
}

// What the integration tests give a broker besides the jar: kfkwho's own
// exporter rules, as released with the version bundled, and the JMX Exporter
// agent that reads them.
val kfkwhoRules = configurations.create("kfkwhoRules") {
    isCanBeConsumed = false
    isTransitive = false
}
val jmxExporterAgent = configurations.create("jmxExporterAgent") {
    isCanBeConsumed = false
    isTransitive = false
}

dependencies {
    kfkwho("dev.kfkit:kfkwho:$kfkwhoVersion")
    kfkwhoRules("dev.kfkit:kfkwho:$kfkwhoVersion@yml")
    jmxExporterAgent("io.prometheus.jmx:jmx_prometheus_javaagent:1.0.1")
}

testing {
    suites {
        // Real brokers in containers with the built jar as their authorizer:
        // ./gradlew integrationTest. Not part of build or check unless
        // -PintegrationTest is set; CI runs the one-node tests once per
        // supported broker release and the cluster tests on the newest.
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter("6.1.3")
            dependencies {
                implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
                implementation("org.testcontainers:testcontainers:2.0.5")
                runtimeOnly("org.slf4j:slf4j-simple:2.0.20")
            }
            targets.all {
                testTask.configure {
                    val brokerLogs = layout.buildDirectory.dir("integration-logs")
                    dependsOn(tasks.jar)
                    inputs.files(tasks.jar)
                    inputs.property("brokerVersion", brokerVersion)
                    // DocsIT runs the commands of these pages, and ApplyImportBrokerIT imports the
                    // example files: a change to one of them alone reruns the suite.
                    inputs.files(fileTree("docs") { include("*.md", "examples/*") })
                        .withPropertyName("docs")
                        .withPathSensitivity(PathSensitivity.RELATIVE)
                    outputs.dir(brokerLogs)
                    systemProperty("kfkrbac.it.jar", tasks.jar.get().archiveFile.get().asFile.absolutePath)
                    systemProperty("kfkrbac.it.image", "apache/kafka:$brokerVersion")
                    systemProperty("kfkrbac.it.logs", brokerLogs.get().asFile.absolutePath)
                    // For the tests of the metrics: kfkwho's rules and its own jar, both as
                    // released, and the agent. The jar was checked when it was relocated.
                    val kfkwhoJar: FileCollection = kfkwho
                    val rules: FileCollection = kfkwhoRules
                    val agent: FileCollection = jmxExporterAgent
                    val rulesSha256 = kfkwhoRulesSha256
                    val version = kfkwhoVersion
                    val check = requireReleased
                    inputs.files(kfkwhoJar, rules, agent)
                    inputs.property("kfkwhoRulesSha256", rulesSha256)
                    doFirst {
                        check(rules.singleFile, rulesSha256, "kfkwhoRulesSha256", version)
                    }
                    jvmArgumentProviders.add(CommandLineArgumentProvider {
                        listOf(
                            "-Dkfkrbac.it.kfkwho=${kfkwhoJar.singleFile.absolutePath}",
                            "-Dkfkrbac.it.rules=${rules.singleFile.absolutePath}",
                            "-Dkfkrbac.it.agent=${agent.singleFile.absolutePath}",
                        )
                    })
                    // The clients' own chatter; what a broker said is in build/integration-logs.
                    systemProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
                    testLogging {
                        events("passed", "skipped", "failed")
                        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
                        showStandardStreams = false
                    }
                    // A skipped test shows as SKIPPED without its reason; say it.
                    addTestListener(object : TestListener {
                        override fun beforeSuite(suite: TestDescriptor) {}
                        override fun beforeTest(testDescriptor: TestDescriptor) {}
                        override fun afterTest(testDescriptor: TestDescriptor, result: TestResult) {}
                        override fun afterSuite(suite: TestDescriptor, result: TestResult) {
                            if (suite.parent == null && result.skippedTestCount > 0) {
                                logger.warn(
                                    "integrationTest: {} of {} tests skipped. They skip for one reason only: "
                                        + "Docker is not available.",
                                    result.skippedTestCount, result.testCount,
                                )
                            }
                        }
                    })
                }
            }
        }
    }
}

// check compiles the integration tests so that they cannot rot, and runs
// them only when asked to.
tasks.check {
    dependsOn(tasks.named("integrationTestClasses"))
    if (providers.gradleProperty("integrationTest").isPresent) {
        dependsOn(testing.suites.named("integrationTest"))
    }
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
    // RoleTest and SettingsDocTest compare these with the code, DocsTest
    // follows their links and checks that DocsIT has a test for each page
    // that marks commands as run: a change to them alone reruns the tests.
    inputs.files(fileTree("docs"), "README.md", "src/integrationTest/java/dev/kfkit/kfkrbac/DocsIT.java")
        .withPropertyName("docs")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    // MetricsDocTest compares these with the code and with the kfkwho bundled.
    inputs.files("docs/metrics.md", "NOTICE")
        .withPropertyName("metricsDocs")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    systemProperty("kfkrbac.kfkwho.version", kfkwhoVersion)
    // JarContentsTest reads the jar that ships.
    inputs.files(tasks.jar)
        .withPropertyName("jar")
        .withPathSensitivity(PathSensitivity.NONE)
    systemProperty("kfkrbac.jar", tasks.jar.get().archiveFile.get().asFile.absolutePath)
    testLogging {
        events("passed", "skipped", "failed")
    }
}

// The jar that ships is self-contained: our classes and kfkwho's, relocated,
// with the licence and the notice that covers both. It has no dependency and
// no Class-Path; Kafka's classes and SLF4J are the broker's.
tasks.jar {
    from(zipTree(relocatedKfkwho.flatMap { it.archiveFile })) {
        exclude("META-INF/**")
    }
    from(files("LICENSE", "NOTICE")) {
        into("META-INF")
    }
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

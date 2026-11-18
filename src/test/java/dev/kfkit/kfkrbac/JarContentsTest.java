/*
 * Copyright 2026 Ivan Abramov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.kfkit.kfkrbac;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import dev.kfkit.kfkrbac.shaded.kfkwho.AccessRecorder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The jar that ships, as Gradle built it: one file with this project's
 * classes and kfkwho's, relocated, and nothing else. Gradle passes its path
 * as the system property {@code kfkrbac.jar} and builds it before the tests.
 */
class JarContentsTest {

    private static final String OURS = "dev/kfkit/kfkrbac/";
    private static final String RELOCATED = "dev/kfkit/kfkrbac/shaded/kfkwho/";
    private static final String KFKWHO = "dev/kfkit/kfkwho";
    private static final String SERVICES = "META-INF/services/";

    /** The files in the jar, directories left out. */
    private static List<String> files;
    private static Path jar;

    @BeforeAll
    static void list() throws IOException {
        String path = System.getProperty("kfkrbac.jar");
        assertNotNull(path, "kfkrbac.jar is not set; run the tests through ./gradlew test");
        jar = Path.of(path);
        try (JarFile opened = new JarFile(jar.toFile())) {
            files = opened.stream().filter(entry -> !entry.isDirectory()).map(JarEntry::getName).sorted().toList();
        }
    }

    @Test
    void theRecorderIsInsideRelocated() {
        // The class the authorizer was compiled against is the one in the jar.
        assertEquals(RELOCATED.replace('/', '.'), AccessRecorder.class.getPackageName() + ".");
        assertTrue(files.contains(RELOCATED + "AccessRecorder.class"), files.toString());
        assertTrue(files.contains(OURS + "RbacAuthorizer.class"), files.toString());
    }

    @Test
    void nothingIsLeftUnderKfkwhosOwnPackage() {
        assertEquals(List.of(), files.stream().filter(file -> file.startsWith(KFKWHO + "/")).toList());
    }

    /** A class that still named kfkwho's own package would load kfkwho's jar when it is there, and fail when it is not. */
    @Test
    void noClassRefersToKfkwhosOwnPackage() throws IOException {
        try (JarFile opened = new JarFile(jar.toFile())) {
            for (String file : files) {
                if (!file.endsWith(".class")) {
                    continue;
                }
                try (InputStream in = opened.getInputStream(opened.getEntry(file))) {
                    String constants = new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
                    assertFalse(constants.contains(KFKWHO) || constants.contains(KFKWHO.replace('/', '.')),
                            file + " refers to " + KFKWHO);
                }
            }
        }
    }

    @Test
    void kfkwhosOwnAuthorizerIsNotBundled() {
        assertEquals(List.of(), files.stream().filter(file -> file.contains("MeteredStandardAuthorizer")).toList());
    }

    @Test
    void nothingElseIsBundled() {
        assertEquals(List.of(), files.stream()
                .filter(file -> file.startsWith("org/apache/kafka/") || file.startsWith("org/slf4j/")).toList());
        assertEquals(List.of(),
                files.stream().filter(file -> !file.startsWith(OURS) && !file.startsWith("META-INF/")).toList());
        assertEquals(List.of("META-INF/LICENSE", "META-INF/MANIFEST.MF", "META-INF/NOTICE"), files.stream()
                .filter(file -> file.startsWith("META-INF/") && !file.startsWith(SERVICES)).toList());
    }

    /** What the jar registers as a service is a class of this project: a bundled library would bring its own. */
    @Test
    void theServicesRegisteredAreOurs() throws IOException {
        try (JarFile opened = new JarFile(jar.toFile())) {
            for (String file : files) {
                if (!file.startsWith(SERVICES)) {
                    continue;
                }
                try (InputStream in = opened.getInputStream(opened.getEntry(file))) {
                    List<String> registered = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines()
                            .map(String::strip).filter(line -> !line.isEmpty() && !line.startsWith("#")).toList();
                    assertEquals(List.of(), registered.stream()
                            .filter(name -> !name.startsWith(OURS.replace('/', '.'))).toList(), file);
                }
            }
        }
    }

    @Test
    void theJarNeedsNoOtherJar() throws IOException {
        try (JarFile opened = new JarFile(jar.toFile())) {
            Manifest manifest = opened.getManifest();
            assertNull(manifest.getMainAttributes().getValue("Class-Path"));
            assertEquals("kfkrbac", manifest.getMainAttributes().getValue("Implementation-Title"));
        }
    }

    @Test
    void theNoticeAndTheLicenceTravelWithTheJar() throws IOException {
        try (JarFile opened = new JarFile(jar.toFile())) {
            for (String file : List.of("NOTICE", "LICENSE")) {
                try (InputStream in = opened.getInputStream(opened.getEntry("META-INF/" + file))) {
                    assertEquals(Files.readString(Path.of(file), StandardCharsets.UTF_8),
                            new String(in.readAllBytes(), StandardCharsets.UTF_8), "META-INF/" + file);
                }
            }
        }
    }
}

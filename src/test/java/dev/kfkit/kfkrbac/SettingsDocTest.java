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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

/**
 * AC2 of issue 23: the table of {@code docs/settings.md} against the
 * classes that ship. The settings are the ones the compiled classes name,
 * a type is what the code that reads the setting takes and refuses, and a
 * default is what that code does without the setting.
 */
class SettingsDocTest {

    private static final Path SETTINGS_DOC = Path.of("docs/settings.md");
    private static final List<String> HEADER = List.of("Setting", "Read by", "Type", "Default", "Meaning");

    /** A setting of ours, as a whole string constant of a class: {@code kfkrbac.metrics.enabled}. */
    private static final Pattern SETTING = Pattern.compile("kfkrbac(\\.[a-z]+)+");

    /**
     * The code that reads a setting, and what it does without it.
     *
     * @param readBy where it is read, as the table says it
     * @param read reads that value of the setting as the node or the worker would at start-up
     * @param byDefault the default, as the table writes it
     */
    private record Reader(String readBy, Consumer<String> read, Supplier<String> byDefault) {
    }

    private static final Map<String, Reader> READERS = Map.of(
            RbacAuthorizer.METRICS_ENABLED_CONFIG, new Reader("a node",
                    // A node that is only a controller: the value is read, and nothing of kfkwho is started.
                    value -> configure(Map.of(RbacAuthorizer.METRICS_ENABLED_CONFIG, value, "process.roles",
                            "controller")),
                    () -> {
                        configure(Map.of());
                        return String.valueOf(!mbeans("kfkwho:*").isEmpty());
                    }),
            DeniedLog.INTERVAL_CONFIG, new Reader("a node",
                    value -> configure(Map.of(DeniedLog.INTERVAL_CONFIG, value)),
                    () -> String.valueOf(DeniedLog.DEFAULT_INTERVAL_SECONDS)),
            RestAccess.SUPER_USERS, new Reader("a worker",
                    value -> RestAccess.validate(Map.of(RestAccess.SUPER_USERS, value)),
                    () -> RestAccess.superUsers(null).isEmpty() ? "none" : RestAccess.superUsers(null).toString()),
            RestAccess.REFRESH_INTERVAL_MS, new Reader("a worker",
                    value -> RestAccess.validate(Map.of(RestAccess.REFRESH_INTERVAL_MS, value)),
                    () -> String.valueOf(RestAccess.interval(null))));

    /** What a setting of each type takes, and what it must refuse. */
    private static final Map<String, List<List<String>>> TAKES_AND_REFUSES = Map.of(
            "boolean", List.of(List.of("true", "false"), List.of("yes", "1", "")),
            "int", List.of(List.of("0", "7"), List.of("1.5", "seven", "-1", "", "3000000000")),
            "long", List.of(List.of("1", "3000000000"), List.of("1.5", "seven", "0", "-1", "")),
            "string", List.of(List.of("User:admin", "User:admin;User:CN=connect-1,O=example"), List.of("admin")));

    @Test
    void theTableHasTheSettingsTheClassesName() throws IOException {
        assertEquals(settingsOfTheClasses(), new TreeSet<>(table().keySet()),
                "the settings in the string constants of the classes of the jar, and the rows of " + SETTINGS_DOC);
    }

    /** The scan finds a setting where there is one: otherwise the test above could pass by finding nothing. */
    @Test
    void theSettingsOfAClassAreFoundInItsConstants() throws IOException {
        assertEquals(Set.of(RestAccess.SUPER_USERS, RestAccess.REFRESH_INTERVAL_MS),
                settings(Files.readAllBytes(classes().resolve("dev/kfkit/kfkrbac/RestAccess.class"))));
        assertEquals(Set.of(), settings(Files.readAllBytes(classes().resolve("dev/kfkit/kfkrbac/Role.class"))));
    }

    @Test
    void theTableSaysWhereEachSettingIsReadAndItsDefault() throws IOException {
        for (Map.Entry<String, List<String>> row : table().entrySet()) {
            Reader reader = reader(row.getKey());
            assertEquals(reader.readBy(), row.getValue().get(1), "where " + row.getKey() + " is read");
            assertEquals(reader.byDefault().get(), row.getValue().get(3).replace("`", ""),
                    "the default of " + row.getKey());
        }
    }

    @Test
    void theCodeTakesAndRefusesWhatTheTypeOfTheTableSays() throws IOException {
        for (Map.Entry<String, List<String>> row : table().entrySet()) {
            String setting = row.getKey();
            String type = row.getValue().get(2);
            List<List<String>> values = TAKES_AND_REFUSES.get(type);
            assertNotNull(values, "this test knows the type '" + type + "' of " + setting);
            Reader reader = reader(setting);
            for (String taken : values.get(0)) {
                assertDoesNotThrow(() -> reader.read().accept(taken), setting + "=" + taken);
            }
            for (String refused : values.get(1)) {
                ConfigException thrown = assertThrows(ConfigException.class, () -> reader.read().accept(refused),
                        setting + "=" + refused);
                assertTrue(thrown.getMessage().contains(setting), "the message names the setting: "
                        + thrown.getMessage());
            }
        }
    }

    private static Reader reader(String setting) {
        Reader reader = READERS.get(setting);
        assertNotNull(reader, "this test knows the code that reads " + setting);
        return reader;
    }

    /** The authorizer as a node configures it, with these settings, closed again. */
    private static void configure(Map<String, ?> settings) {
        try (RbacAuthorizer authorizer = new RbacAuthorizer()) {
            authorizer.configure(settings);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Set<ObjectName> mbeans(String pattern) {
        try {
            return ManagementFactory.getPlatformMBeanServer().queryNames(new ObjectName(pattern), null);
        } catch (MalformedObjectNameException e) {
            throw new AssertionError(e);
        }
    }

    /** The rows of the table, by setting; cells as written, backticks around the setting dropped. */
    private static Map<String, List<String>> table() throws IOException {
        Map<String, List<String>> rows = new LinkedHashMap<>();
        List<String> lines = Files.readAllLines(SETTINGS_DOC, StandardCharsets.UTF_8);
        int header = lines.indexOf("| " + String.join(" | ", HEADER) + " |");
        assertTrue(header >= 0, SETTINGS_DOC + " has a table with the header " + HEADER);
        for (int i = header + 2; i < lines.size() && lines.get(i).startsWith("|"); i++) {
            String line = lines.get(i).strip();
            List<String> cells = Stream.of(line.substring(1, line.length() - 1).split("\\|", HEADER.size()))
                    .map(String::strip).toList();
            assertEquals(HEADER.size(), cells.size(), "cells in " + line);
            assertEquals(null, rows.put(cells.get(0).replace("`", ""), cells), "a setting has one row");
        }
        assertTrue(!rows.isEmpty(), SETTINGS_DOC + " has rows");
        return rows;
    }

    /** Every setting of ours that a class under test names in a string constant. */
    private static Set<String> settingsOfTheClasses() throws IOException {
        Set<String> settings = new TreeSet<>();
        int classes = 0;
        try (Stream<Path> files = Files.walk(classes())) {
            for (Path file : files.filter(file -> file.toString().endsWith(".class")).toList()) {
                settings.addAll(settings(Files.readAllBytes(file)));
                classes++;
            }
        }
        assertTrue(classes > 20, "the classes of the jar were found under " + classes() + ": " + classes);
        return settings;
    }

    /** The directory the classes that go into the jar were compiled to. */
    private static Path classes() {
        try {
            Path classes = Path.of(RbacAuthorizer.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            assertTrue(Files.isDirectory(classes), classes + " is a directory of classes");
            return classes;
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }

    /** The string constants of a class file that are a setting of ours and nothing else. */
    private static Set<String> settings(byte[] classFile) throws IOException {
        Set<String> settings = new TreeSet<>();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(classFile));
        assertEquals(0xCAFEBABE, in.readInt(), "a class file");
        in.skipBytes(4); // minor and major version
        int count = in.readUnsignedShort();
        for (int i = 1; i < count; i++) {
            int tag = in.readUnsignedByte();
            switch (tag) {
                case 1 -> { // Utf8
                    String text = in.readUTF();
                    if (SETTING.matcher(text).matches()) {
                        settings.add(text);
                    }
                }
                case 7, 8, 16, 19, 20 -> in.skipBytes(2); // Class, String, MethodType, Module, Package
                case 15 -> in.skipBytes(3); // MethodHandle
                case 3, 4, 9, 10, 11, 12, 17, 18 -> in.skipBytes(4);
                case 5, 6 -> { // Long and Double take two entries
                    in.skipBytes(8);
                    i++;
                }
                default -> throw new AssertionError("constant pool tag " + tag);
            }
        }
        return settings;
    }
}

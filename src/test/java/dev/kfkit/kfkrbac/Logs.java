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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * What the code under test logs, read from where the test logger writes:
 * standard error, one line for each message, as
 * {@code [thread] LEVEL logger - message}.
 */
final class Logs {

    /** A message as it was logged. */
    record Line(String level, String logger, String message) {
    }

    private Logs() {
    }

    /** The messages written to the logger while the code runs, in order. */
    static List<Line> of(String logger, Runnable code) {
        List<Line> lines = new ArrayList<>();
        for (String text : captured(code).split("\\R")) {
            int thread = text.indexOf("] ");
            if (!text.startsWith("[") || thread < 0) {
                continue; // a stack trace, or not a message
            }
            String[] parts = text.substring(thread + 2).split(" ", 3);
            if (parts.length == 3 && parts[1].equals(logger) && parts[2].startsWith("- ")) {
                lines.add(new Line(parts[0], parts[1], parts[2].substring(2)));
            }
        }
        return lines;
    }

    /** The messages alone. */
    static List<String> messages(String logger, Runnable code) {
        return of(logger, code).stream().map(Line::message).toList();
    }

    /**
     * Runs the code with the logger at {@code DEBUG}; the test logger is at
     * {@code INFO}, as the broker's is. The level of the test logger is set
     * by reflection, for the run only: it has no other way to change it
     * after start-up, and at {@code DEBUG} for every test the allowed
     * requests of the whole suite would be written out.
     */
    static void atDebug(String logger, Runnable code) {
        try {
            Object log = Class.forName("org.slf4j.LoggerFactory").getMethod("getLogger", String.class)
                    .invoke(null, logger);
            Field level = log.getClass().getDeclaredField("currentLogLevel");
            level.setAccessible(true);
            int before = level.getInt(log);
            level.setInt(log, 10); // DEBUG
            try {
                code.run();
            } finally {
                level.setInt(log, before);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("the test logger is not the one whose level this sets", e);
        }
    }

    /** What the code writes to standard error, which also gets it once the code has run. */
    private static String captured(Runnable code) {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            code.run();
        } finally {
            System.setErr(original);
            original.print(captured.toString(StandardCharsets.UTF_8));
        }
        return captured.toString(StandardCharsets.UTF_8);
    }
}

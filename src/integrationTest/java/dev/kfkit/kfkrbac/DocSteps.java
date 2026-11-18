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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A document of {@code docs/} as what a test does with it: the shell
 * commands to run, in the order of the page, and the output the page shows
 * for them.
 *
 * <p>An HTML comment on the line before a fenced block says what the block
 * is to the tests; a reader of the rendered page does not see it.
 *
 * <pre>
 * &lt;!-- run --&gt;                    the bash block that follows is run and must exit with 0
 * &lt;!-- run: exit 1 --&gt;            ... and must exit with 1
 * &lt;!-- run: on open --&gt;           ... on the node of that name, not on the document's main one
 * &lt;!-- run: session --&gt;           ... and goes in front of every later block of the page, as in
 *                                  one shell session: where the page defines a function
 * &lt;!-- not run: the reason --&gt;    the block that follows is not run: commands that need what a
 *                                  test does not have, or a file for a tool the tests do not run
 * &lt;!-- output --&gt;                 the text block that follows is everything the block before it prints
 * &lt;!-- output: excerpt --&gt;        ... is among what it prints: each line is in a line of the
 *                                  output, in that order
 * &lt;!-- prints nothing --&gt;         the block before it prints nothing
 * &lt;!-- restart --&gt;                the node is stopped and started here
 * &lt;!-- restart: on open --&gt;       ... that node
 * &lt;!-- environment --&gt;            the block that follows is the environment of a container
 * </pre>
 *
 * <p>Options after the colon are separated by commas. A {@code bash} block
 * without one of the first two kinds of comment fails the reading: every
 * shell command of a page is run, or the page says it is not.
 */
final class DocSteps {

    /** The node a block runs on when its comment names none. */
    static final String MAIN = "main";

    /** One thing the test does, at a line of the page. */
    sealed interface Step permits Run, Restart {
        int line();

        String node();
    }

    /**
     * A block of shell commands to run.
     *
     * @param line the line of the page the block opens at
     * @param node the node it runs on
     * @param session whether it also goes in front of every later block of the page
     * @param exit the exit status it must have
     * @param commands the block, as the page has it
     * @param output what it must print, or {@code null} when the page does not say
     */
    record Run(int line, String node, boolean session, int exit, String commands, Output output) implements Step {

        Run with(Output shown) {
            return new Run(line, node, session, exit, commands, shown);
        }
    }

    /** The node is stopped and started. */
    record Restart(int line, String node) implements Step {
    }

    /** How the text the page shows relates to what the block prints. */
    enum Shows {
        EVERYTHING, EXCERPT, NOTHING
    }

    /**
     * What a block must print.
     *
     * @param shows whether {@code text} is all of it or lines among it
     * @param text the text of the page
     */
    record Output(Shows shows, String text) {
    }

    private static final Pattern COMMENT = Pattern.compile("^\\s*<!--\\s*(.*?)\\s*-->\\s*$");
    private static final Pattern FENCE = Pattern.compile("^(\\s*)```\\s*([A-Za-z0-9_-]*)\\s*$");
    private static final Pattern EXIT = Pattern.compile("exit (\\d+)");
    private static final Pattern ON = Pattern.compile("on ([a-z]+)");

    /** Fences a shell command could hide in without being run. */
    private static final Set<String> OTHER_SHELLS = Set.of("sh", "shell", "console", "zsh", "shell-session");

    private final Path document;
    private final List<Step> steps = new ArrayList<>();
    private final List<String> notRun = new ArrayList<>();
    private final Map<String, String> environment = new LinkedHashMap<>();

    private DocSteps(Path document) {
        this.document = document;
    }

    /** Reads the page; fails when a shell block does not say whether it is run. */
    static DocSteps of(Path document) {
        DocSteps read = new DocSteps(document);
        try {
            read.read(Files.readAllLines(document));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return read;
    }

    /** What the test does, in the order of the page. */
    List<Step> steps() {
        return List.copyOf(steps);
    }

    /** The reasons the page gives for the blocks it does not run. */
    List<String> notRun() {
        return List.copyOf(notRun);
    }

    /** The variables of the block the page marks as the environment of a container. */
    Map<String, String> environment() {
        return Map.copyOf(environment);
    }

    private void read(List<String> lines) {
        String marker = null; // the comment that waits for its block
        int markerLine = 0;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            Matcher comment = COMMENT.matcher(line);
            Matcher fence = FENCE.matcher(line);
            if (comment.matches()) {
                if (marker != null) {
                    throw wrong(markerLine, "the comment '" + marker + "' is not followed by a block");
                }
                String text = comment.group(1);
                if (text.equals("restart") || text.startsWith("restart:")) {
                    steps.add(new Restart(i + 1, node(text)));
                } else if (text.equals("prints nothing")) {
                    show(i + 1, new Output(Shows.NOTHING, ""));
                } else if (text.equals("run") || text.startsWith("run:") || text.startsWith("not run")
                        || text.equals("output") || text.startsWith("output:") || text.equals("environment")) {
                    marker = text;
                    markerLine = i + 1;
                }
                continue;
            }
            if (!fence.matches()) {
                if (marker != null) {
                    throw wrong(markerLine, "the comment '" + marker + "' is not followed by a block");
                }
                continue;
            }
            int opens = i + 1;
            String indent = fence.group(1);
            String language = fence.group(2);
            StringBuilder block = new StringBuilder();
            for (i++; i < lines.size() && !lines.get(i).strip().equals("```"); i++) {
                String inside = lines.get(i);
                block.append(inside.startsWith(indent) ? inside.substring(indent.length()) : inside.stripLeading())
                        .append('\n');
            }
            if (i == lines.size()) {
                throw wrong(opens, "the block is not closed");
            }
            block(opens, language, block.toString(), marker);
            marker = null;
        }
        if (marker != null) {
            throw wrong(markerLine, "the comment '" + marker + "' is not followed by a block");
        }
    }

    private void block(int line, String language, String text, String marker) {
        if (OTHER_SHELLS.contains(language)) {
            throw wrong(line, "shell commands go in a bash block, which is run or says it is not; this one is "
                    + language);
        }
        if (language.equals("bash")) {
            if (marker == null || !(marker.startsWith("run") || marker.startsWith("not run"))) {
                throw wrong(line, "a bash block has '<!-- run -->' or '<!-- not run: the reason -->' on the line "
                        + "before it");
            }
            if (marker.startsWith("not run")) {
                notRun(line, marker);
                return;
            }
            Matcher exit = EXIT.matcher(marker);
            steps.add(new Run(line, node(marker), marker.matches(".*\\bsession\\b.*"),
                    exit.find() ? Integer.parseInt(exit.group(1)) : 0, text, null));
            return;
        }
        if (marker == null) {
            return; // a block of something else: YAML, properties, a container file
        }
        if (marker.startsWith("not run")) {
            notRun(line, marker); // nothing would run it; the page says so all the same
        } else if (marker.equals("environment")) {
            for (String variable : text.lines().filter(variable -> !variable.isBlank()).toList()) {
                int equals = variable.indexOf('=');
                if (equals <= 0) {
                    throw wrong(line, "a line of the environment is NAME=value, not '" + variable + "'");
                }
                environment.put(variable.substring(0, equals), variable.substring(equals + 1));
            }
        } else if (marker.startsWith("output")) {
            show(line, new Output(marker.contains("excerpt") ? Shows.EXCERPT : Shows.EVERYTHING, text));
        } else {
            throw wrong(line, "the comment '" + marker + "' goes before a bash block, and this one is "
                    + (language.isEmpty() ? "not" : language));
        }
    }

    private void notRun(int line, String marker) {
        String reason = marker.substring("not run".length()).replaceFirst("^:\\s*", "");
        if (reason.isBlank()) {
            throw wrong(line, "'<!-- not run -->' says why: '<!-- not run: the reason -->'");
        }
        notRun.add(reason);
    }

    /** Gives the last block of commands the output the page shows for it. */
    private void show(int line, Output output) {
        if (steps.isEmpty() || !(steps.get(steps.size() - 1) instanceof Run run) || run.output() != null) {
            throw wrong(line, "output belongs to the block of commands right before it, and to one only");
        }
        steps.set(steps.size() - 1, run.with(output));
    }

    private static String node(String marker) {
        Matcher on = ON.matcher(marker);
        return on.find() ? on.group(1) : MAIN;
    }

    private AssertionError wrong(int line, String what) {
        return new AssertionError(document + ":" + line + ": " + what);
    }
}

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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What can be checked of the README and {@code docs/} without a broker:
 * that a link to a file leads to one, and that the operator guides say of
 * every block of shell commands whether a test runs it. Running them is
 * {@code DocsIT}'s, in the integration suite.
 */
class DocsTest {

    /** {@code [text](target)} and {@code [text](target "title")}, images included. */
    private static final Pattern INLINE_LINK =
            Pattern.compile("\\[[^\\]]*\\]\\(\\s*<?([^)\\s>]+)>?(?:\\s+\"[^\"]*\")?\\s*\\)");
    /** {@code [label]: target}, the definition of a reference link. */
    private static final Pattern REFERENCE = Pattern.compile("(?m)^ {0,3}\\[[^\\]]+\\]:\\s*<?([^\\s>]+)>?");
    /** {@code https:}, {@code mailto:}: a link that leaves the repository. */
    private static final Pattern SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*:.*");
    private static final Pattern FENCED_BLOCK = Pattern.compile("(?ms)^\\s*```.*?^\\s*```\\s*$");
    private static final Pattern CODE_SPAN = Pattern.compile("`[^`\\n]*`");

    private static final Pattern FENCE = Pattern.compile("^\\s*```\\s*([A-Za-z0-9_-]*)\\s*$");
    private static final Pattern RUN_OR_NOT =
            Pattern.compile("^\\s*<!--\\s*(run(:.*)?|not run: \\S.*?)\\s*-->\\s*$");
    private static final List<String> SHELLS = List.of("bash", "sh", "shell", "console", "zsh", "shell-session");

    /** The guides whose every shell command is run by {@code DocsIT}, or says that it is not. */
    private static final List<Path> RUN_BY_A_TEST = List.of(Path.of("docs/install.md"), Path.of("docs/bindings.md"),
            Path.of("docs/delegation.md"));

    /** AC6 of issue 23: no link in the README or {@code docs/} points at a file that does not exist. */
    @Test
    void everyLinkToAFileLeadsToOne() throws IOException {
        List<Path> documents = documents(Path.of(""));
        assertTrue(documents.contains(Path.of("README.md")) && documents.size() > 1,
                "the README and the documents of docs/ were found: " + documents);

        List<String> links = new ArrayList<>();
        assertEquals(List.of(), broken(documents, links));
        assertFalse(links.isEmpty(), "the documents link to files at all");
    }

    /** The check sees a broken link when there is one: otherwise it could pass by finding nothing. */
    @Test
    void aLinkToAMissingFileIsFound(@TempDir Path root) throws IOException {
        Files.createDirectories(root.resolve("docs/deeper"));
        Files.writeString(root.resolve("README.md"), """
                [there](docs/there.md), [gone](docs/gone.md "a title"), [a section](docs/there.md#section),
                [elsewhere](https://example.com/gone.md), [here](#here), `[code](not-a-link.md)`

                ```bash
                echo "[code](not-a-link-either.md)"
                ```

                [by reference][ref]

                [ref]: docs/gone-too.md
                """);
        Files.writeString(root.resolve("docs/there.md"), "[up](../README.md) ![picture](deeper/missing.png)\n");
        Files.writeString(root.resolve("docs/deeper/far.md"), "[a directory](..) [out](../../nowhere/)\n");

        List<String> links = new ArrayList<>();
        assertEquals(List.of(
                "README.md: docs/gone.md",
                "README.md: docs/gone-too.md",
                "docs/deeper/far.md: ../../nowhere/",
                "docs/there.md: deeper/missing.png"), broken(root, documents(root), links));
        assertEquals(8, links.size(), "the links to files that were looked at: " + links);
    }

    /**
     * AC3 of issue 23, as far as it goes without a broker: in the guides
     * {@code DocsIT} runs, every block of shell commands is a {@code bash}
     * block with a comment before it that says it is run, or why not.
     */
    @Test
    void everyShellBlockOfTheGuidesSaysWhetherItIsRun() throws IOException {
        List<String> unmarked = new ArrayList<>();
        int blocks = 0;
        for (Path guide : RUN_BY_A_TEST) {
            List<String> lines = Files.readAllLines(guide);
            String before = "";
            boolean inside = false;
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                Matcher fence = FENCE.matcher(line);
                if (fence.matches()) {
                    inside = !inside;
                    if (inside && SHELLS.contains(fence.group(1))) {
                        blocks++;
                        if (!fence.group(1).equals("bash") || !RUN_OR_NOT.matcher(before).matches()) {
                            unmarked.add(guide + ":" + (i + 1));
                        }
                    }
                }
                if (!line.isBlank()) {
                    before = line;
                }
            }
            assertFalse(inside, guide + " ends inside a block");
        }
        assertEquals(List.of(), unmarked, "blocks of shell commands that are not bash blocks with '<!-- run -->' "
                + "or '<!-- not run: the reason -->' on the line before them");
        assertTrue(blocks > 0, "the guides have blocks of shell commands");
    }

    private static List<String> broken(List<Path> documents, List<String> links) throws IOException {
        return broken(Path.of(""), documents, links);
    }

    /**
     * The links of the documents that point at a file or a directory that
     * is not there, as {@code document: target}. A link that leaves the
     * repository, and one to a section of the same document, is not looked
     * at; of a link to a section of another document, the document is.
     *
     * @param root the directory the documents' paths are relative to
     * @param links collects every link that was looked at
     */
    private static List<String> broken(Path root, List<Path> documents, List<String> links) throws IOException {
        List<String> broken = new ArrayList<>();
        for (Path document : documents) {
            String text = CODE_SPAN.matcher(FENCED_BLOCK.matcher(Files.readString(root.resolve(document)))
                    .replaceAll("")).replaceAll("");
            for (Pattern link : List.of(INLINE_LINK, REFERENCE)) {
                Matcher found = link.matcher(text);
                while (found.find()) {
                    String target = found.group(1);
                    if (SCHEME.matcher(target).matches() || target.startsWith("#")) {
                        continue;
                    }
                    links.add(document + ": " + target);
                    String file = target.contains("#") ? target.substring(0, target.indexOf('#')) : target;
                    Path parent = root.resolve(document).toAbsolutePath().getParent();
                    if (!Files.exists(parent.resolve(file).normalize())) {
                        broken.add(slashes(document) + ": " + target);
                    }
                }
            }
        }
        return broken;
    }

    /** The README and every Markdown file under {@code docs/}, relative to the root, in a fixed order. */
    private static List<Path> documents(Path root) throws IOException {
        List<Path> documents = new ArrayList<>();
        documents.add(Path.of("README.md"));
        try (Stream<Path> docs = Files.walk(root.resolve("docs"))) {
            docs.filter(Files::isRegularFile).filter(file -> file.getFileName().toString().endsWith(".md"))
                    .map(file -> root.toAbsolutePath().relativize(file.toAbsolutePath())).sorted()
                    .forEach(documents::add);
        }
        return documents;
    }

    private static String slashes(Path path) {
        return path.toString().replace('\\', '/');
    }
}

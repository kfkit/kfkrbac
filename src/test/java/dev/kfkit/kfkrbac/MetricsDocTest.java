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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import dev.kfkit.kfkrbac.shaded.kfkwho.AccessRecorder;
import org.junit.jupiter.api.Test;

/**
 * What operators read about the metrics, against the code and the kfkwho
 * that is bundled: {@code docs/metrics.md} and {@code NOTICE}. Gradle passes
 * the version of kfkwho as the system property {@code kfkrbac.kfkwho.version}.
 */
class MetricsDocTest {

    private static final Path METRICS_DOC = Path.of("docs/metrics.md");
    private static final Path NOTICE = Path.of("NOTICE");
    private static final String KFKWHO_DOCS = "https://github.com/kfkit/kfkwho/blob/v%s/docs/%s";

    @Test
    void theDocSaysHowToTurnTheMetricsOn() throws IOException {
        String doc = Files.readString(METRICS_DOC, StandardCharsets.UTF_8);

        assertTrue(doc.contains(RbacAuthorizer.METRICS_ENABLED_CONFIG + "=true"),
                METRICS_DOC + " does not show " + RbacAuthorizer.METRICS_ENABLED_CONFIG + "=true");
    }

    /** The reference is kfkwho's, of the version in the jar: a newer kfkwho may read settings this one does not. */
    @Test
    void theDocPointsToTheReferenceOfTheKfkwhoThatIsBundled() throws IOException {
        String version = System.getProperty("kfkrbac.kfkwho.version");
        assertNotNull(version, "kfkrbac.kfkwho.version is not set; run the tests through ./gradlew test");
        String doc = Files.readString(METRICS_DOC, StandardCharsets.UTF_8);

        for (String page : new String[] {"config.md", "metrics.md"}) {
            String link = KFKWHO_DOCS.formatted(version, page);
            assertTrue(doc.contains(link), METRICS_DOC + " does not link to " + link);
        }
        assertTrue(doc.contains("kfkwho " + version), METRICS_DOC + " does not name kfkwho " + version);
    }

    @Test
    void theNoticeCarriesKfkwhosNoticeAndNamesWhereItsClassesAre() throws IOException {
        String notice = Files.readString(NOTICE, StandardCharsets.UTF_8);
        int kfkwho = notice.indexOf("  kfkwho\n  Copyright ");

        assertTrue(kfkwho > 0, NOTICE + " does not carry kfkwho's notice");
        assertTrue(notice.indexOf("licensed under the Apache License, Version 2.0", kfkwho) > 0,
                NOTICE + " does not carry the licence line of kfkwho's notice");
        assertTrue(notice.contains(AccessRecorder.class.getPackageName()),
                NOTICE + " does not name " + AccessRecorder.class.getPackageName());
    }
}

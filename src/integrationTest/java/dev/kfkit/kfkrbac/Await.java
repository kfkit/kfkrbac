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

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Waiting for a condition with a bound: what is asked is asked again until
 * the answer is the expected one or the time is up, and then the failure
 * says what was expected and what the last answer was.
 */
final class Await {

    /** Between two attempts. */
    private static final Duration PAUSE = Duration.ofMillis(200);

    private Await() {
    }

    /** Asks until the request goes through; fails with the last refusal when it does not in time. */
    static void allowed(Duration within, String why, Supplier<Answer> attempt) {
        until(within, attempt, Answer::allowed,
                last -> "expected " + last.request() + " to be allowed within " + seconds(within) + ", " + why
                        + "; the cluster's last answer: " + last);
    }

    /** Asks until the request is refused; fails when it still goes through after the time is up. */
    static void refused(Duration within, String why, Supplier<Answer> attempt) {
        until(within, attempt, answer -> !answer.allowed(),
                last -> "expected " + last.request() + " to be refused within " + seconds(within) + ", " + why
                        + "; the cluster's last answer: " + last);
    }

    /**
     * Asks until the answer is the expected one and returns it; fails with
     * {@code failure} of the last answer when the time is up.
     */
    static <T> T until(Duration within, Supplier<T> probe, Predicate<T> expected, Function<T, String> failure) {
        long deadline = System.nanoTime() + within.toNanos();
        int attempts = 0;
        while (true) {
            T answer = probe.get();
            attempts++;
            if (expected.test(answer)) {
                return answer;
            }
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(failure.apply(answer) + " (asked " + attempts + " times)");
            }
            pause();
        }
    }

    private static void pause() {
        try {
            Thread.sleep(PAUSE.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", e);
        }
    }

    private static String seconds(Duration duration) {
        return duration.toSeconds() + " s";
    }
}

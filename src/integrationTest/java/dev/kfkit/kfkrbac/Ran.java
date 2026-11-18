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

/**
 * What one run of the jar's command inside a broker's container left
 * behind.
 *
 * @param typed what was typed after the name of the class, for the failure message
 * @param status the exit status
 * @param out what it printed to standard output
 * @param err what it printed to standard error
 */
record Ran(String typed, int status, String out, String err) {

    /** Fails unless the command exited with that status. */
    Ran assertStatus(int expected, String why) {
        if (status != expected) {
            throw new AssertionError("expected the command to exit with " + expected + ", " + why + "; " + this);
        }
        return this;
    }

    /**
     * Fails unless the command exited with 0, printed exactly that to
     * standard output and nothing to standard error.
     */
    Ran assertPrinted(String expected, String why) {
        assertStatus(0, why);
        if (!out.equals(expected) || !err.isEmpty()) {
            throw new AssertionError("expected the command to print\n" + expected
                    + "and nothing to standard error, " + why + "; " + this);
        }
        return this;
    }

    /** Whether the command exited with that status and its standard error has each of the texts in it. */
    boolean failedSaying(int expected, String... texts) {
        if (status != expected) {
            return false;
        }
        for (String text : texts) {
            if (!err.contains(text)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fails unless the command exited with that status, printed nothing to
     * standard output and its standard error has each of the texts in it.
     */
    Ran assertFailedSaying(int expected, String why, String... texts) {
        assertStatus(expected, why);
        if (!out.isEmpty()) {
            throw new AssertionError("expected the command to print nothing to standard output, " + why + "; " + this);
        }
        for (String text : texts) {
            if (!err.contains(text)) {
                throw new AssertionError("expected the command to say \"" + text + "\" on standard error, " + why
                        + "; " + this);
            }
        }
        return this;
    }

    @Override
    public String toString() {
        return "`" + typed + "` exited with " + status + ", standard output:\n" + out + "standard error:\n" + err;
    }
}

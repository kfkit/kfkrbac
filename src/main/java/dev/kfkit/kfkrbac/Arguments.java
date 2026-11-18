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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The options of one verb, as they were typed: {@code --role DeveloperRead},
 * {@code --role=DeveloperRead}, {@code --group-only}.
 *
 * <p>Written by hand so that the jar keeps having no dependency of its own.
 */
final class Arguments {

    /** The command was not typed the way it is used. */
    static final class WrongUsage extends Exception {

        private static final long serialVersionUID = 1L;

        WrongUsage(String message) {
            super(message);
        }
    }

    private final Map<String, String> values;
    private final Set<String> flags;

    private Arguments(Map<String, String> values, Set<String> flags) {
        this.values = values;
        this.flags = flags;
    }

    /**
     * Reads the options.
     *
     * @param typed what follows the verb
     * @param valued the options that take a value
     * @param flags the options that take none
     */
    static Arguments parse(List<String> typed, Set<String> valued, Set<String> flags) throws WrongUsage {
        Map<String, String> values = new LinkedHashMap<>();
        Set<String> given = new HashSet<>();
        for (int i = 0; i < typed.size(); i++) {
            String word = typed.get(i);
            int equals = word.startsWith("--") ? word.indexOf('=') : -1;
            String option = equals < 0 ? word : word.substring(0, equals);
            if (flags.contains(option)) {
                if (equals >= 0) {
                    throw new WrongUsage(option + " takes no value");
                }
                given.add(option);
            } else if (valued.contains(option)) {
                String value;
                if (equals >= 0) {
                    value = word.substring(equals + 1);
                } else if (i + 1 < typed.size()) {
                    value = typed.get(++i);
                } else {
                    throw new WrongUsage(option + " needs a value");
                }
                if (values.put(option, value) != null) {
                    throw new WrongUsage(option + " is given twice");
                }
            } else if (word.startsWith("--")) {
                throw new WrongUsage("Unknown option: " + option);
            } else {
                throw new WrongUsage("Unexpected argument: " + word);
            }
        }
        return new Arguments(values, given);
    }

    /** The value of an option, when it was given. */
    Optional<String> value(String option) {
        return Optional.ofNullable(values.get(option));
    }

    /** The value of an option the verb cannot do without. */
    String required(String option) throws WrongUsage {
        String value = values.get(option);
        if (value == null) {
            throw new WrongUsage(option + " is required");
        }
        return value;
    }

    /** Whether an option that takes no value was given. */
    boolean has(String flag) {
        return flags.contains(flag);
    }

    /** Which of the options were given, with or without a value, in the order asked. */
    List<String> given(String... options) {
        return Stream.of(options).filter(option -> values.containsKey(option) || flags.contains(option)).toList();
    }
}

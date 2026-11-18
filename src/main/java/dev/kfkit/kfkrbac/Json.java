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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;

/**
 * A value of a JSON file that the command reads, with the place it is at:
 * the file, the line and the column, and the way to it from the top
 * ({@code bindings[2].role}). Whatever is wrong with a file is said with
 * that place and the field, as an {@link Invalid}.
 *
 * <p>The parser is the one the Kafka distribution has on its classpath,
 * Jackson's, so that the jar keeps having no dependency of its own. Only
 * its streaming reader is used: it is what knows where a value is. It is
 * given the bytes of the file, and finds out their encoding as JSON has it:
 * UTF-8, with or without a byte order mark, or UTF-16 or UTF-32.
 */
final class Json {

    /** The file is not valid JSON, or not of the shape it must have. */
    static final class Invalid extends Exception {

        private static final long serialVersionUID = 1L;

        Invalid(String message) {
            super(message);
        }
    }

    private static final JsonFactory FACTORY =
            JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();

    private final String file;
    private final int line;
    private final int column;
    private final String path;

    /** A {@code Map<String, Json>}, a {@code List<Json>}, a {@code String}, or {@code null} for anything else. */
    private final Object value;

    /** What the value is, for the message that says it is not what it must be: {@code a number}. */
    private final String kind;

    private Json(String file, int line, int column, String path, Object value, String kind) {
        this.file = file;
        this.line = line;
        this.column = column;
        this.path = path;
        this.value = value;
        this.kind = kind;
    }

    /** Reads a file that holds one JSON value. */
    static Json read(Path file) throws Invalid {
        String name = file.toString();
        byte[] content;
        try {
            content = Files.readAllBytes(file);
        } catch (NoSuchFileException e) {
            throw new Invalid(name + ": there is no such file");
        } catch (IOException e) {
            throw new Invalid(name + ": cannot be read: " + e);
        }
        try (JsonParser parser = FACTORY.createParser(content)) {
            if (parser.nextToken() == null) {
                throw new Invalid(name + ": line 1, column 1: not valid JSON: the file is empty");
            }
            Json root = value(parser, name, "");
            if (parser.nextToken() != null) {
                JsonLocation after = parser.currentTokenLocation();
                throw new Invalid(name + ": line " + after.getLineNr() + ", column " + after.getColumnNr()
                        + ": not valid JSON: something follows the end of the value");
            }
            return root;
        } catch (JsonProcessingException e) {
            JsonLocation at = e.getLocation();
            throw new Invalid(name + ": " + (at == null ? "" : "line " + at.getLineNr() + ", column "
                    + at.getColumnNr() + ": ") + "not valid JSON: " + e.getOriginalMessage());
        } catch (IOException e) {
            throw new Invalid(name + ": cannot be read: " + e);
        }
    }

    private static Json value(JsonParser parser, String file, String path) throws IOException {
        JsonLocation at = parser.currentTokenLocation();
        JsonToken token = parser.currentToken();
        if (token == JsonToken.START_OBJECT) {
            Map<String, Json> fields = new LinkedHashMap<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String name = parser.currentName();
                parser.nextToken();
                fields.put(name, value(parser, file, path.isEmpty() ? name : path + "." + name));
            }
            return new Json(file, at.getLineNr(), at.getColumnNr(), path, fields, "an object");
        }
        if (token == JsonToken.START_ARRAY) {
            List<Json> elements = new ArrayList<>();
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                elements.add(value(parser, file, path + "[" + elements.size() + "]"));
            }
            return new Json(file, at.getLineNr(), at.getColumnNr(), path, elements, "a list");
        }
        if (token == JsonToken.VALUE_STRING) {
            return new Json(file, at.getLineNr(), at.getColumnNr(), path, parser.getText(), "a string");
        }
        String kind = token == JsonToken.VALUE_NULL ? "null"
                : token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE ? "true or false"
                : "a number";
        return new Json(file, at.getLineNr(), at.getColumnNr(), path, null, kind);
    }

    /** The line the value starts at, from 1. */
    int line() {
        return line;
    }

    /** The way to the value from the top of the file: {@code bindings[2].role}; empty for the top itself. */
    String path() {
        return path;
    }

    /** Whether the value is a list. */
    boolean isList() {
        return value instanceof List;
    }

    /** The elements of a list. */
    List<Json> elements() throws Invalid {
        if (!(value instanceof List<?> elements)) {
            throw invalid("is " + kind + ", and must be a list");
        }
        List<Json> list = new ArrayList<>(elements.size());
        for (Object element : elements) {
            list.add((Json) element);
        }
        return list;
    }

    /** The fields of an object, in the order of the file. */
    Map<String, Json> fields() throws Invalid {
        if (!(value instanceof Map<?, ?> fields)) {
            throw invalid("is " + kind + ", and must be an object");
        }
        Map<String, Json> map = new LinkedHashMap<>();
        fields.forEach((name, field) -> map.put((String) name, (Json) field));
        return map;
    }

    /** A field an object must have. */
    Json field(String name) throws Invalid {
        Json field = fields().get(name);
        if (field == null) {
            throw invalid("\"" + name + "\" is required");
        }
        return field;
    }

    /** A field an object may have. */
    Optional<Json> optional(String name) throws Invalid {
        return Optional.ofNullable(fields().get(name));
    }

    /** The text of a string. */
    String text() throws Invalid {
        if (!(value instanceof String text)) {
            throw invalid("is " + kind + ", and must be a string");
        }
        return text;
    }

    /** The text of a field an object must have, which must be a string. */
    String text(String name) throws Invalid {
        return field(name).text();
    }

    /** What is wrong with this value, with its place: {@code file: line 7, column 5: bindings[2]: ...}. */
    Invalid invalid(String what) {
        return new Invalid(file + ": " + place() + ": " + what);
    }

    /** Where the value is: {@code line 7, column 5: bindings[2]}. */
    String place() {
        return "line " + line + ", column " + column + (path.isEmpty() ? "" : ": " + path);
    }
}

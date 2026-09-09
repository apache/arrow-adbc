/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adbc.driver.flightsql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.flight.NoOpSessionOptionValueVisitor;
import org.apache.arrow.flight.SessionOptionValue;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Package-private helpers for Flight SQL session option serialization and type conversion. */
final class FlightSqlSessionUtil {

  static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Extracts the native Java value from a {@link SessionOptionValue}. String arrays are defensively
   * cloned; Void returns {@code null} (callers must handle null before calling {@link #cast}).
   */
  static final NoOpSessionOptionValueVisitor<Object> TO_JAVA =
      new NoOpSessionOptionValueVisitor<Object>() {
        @Override
        public Object visit(String v) {
          return v;
        }

        @Override
        public Object visit(boolean v) {
          return v;
        }

        @Override
        public Object visit(long v) {
          return v;
        }

        @Override
        public Object visit(double v) {
          return v;
        }

        @Override
        public Object visit(String[] v) {
          return v.clone();
        }
      };

  /**
   * Extracts a JSON-safe Java value from a {@link SessionOptionValue}. JSON has no representation
   * for NaN/Infinity, so non-finite doubles are represented by their {@link String} form (for
   * example {@code "NaN"}).
   */
  static final NoOpSessionOptionValueVisitor<Object> TO_JSON_JAVA =
      new NoOpSessionOptionValueVisitor<Object>() {
        @Override
        public Object visit(String v) {
          return v;
        }

        @Override
        public Object visit(boolean v) {
          return v;
        }

        @Override
        public Object visit(long v) {
          return v;
        }

        @Override
        public Object visit(double v) {
          return Double.isFinite(v) ? (Object) v : Double.toString(v);
        }

        @Override
        public Object visit(String[] v) {
          return v.clone();
        }
      };

  /** Serializes all session options to a JSON object string. */
  static String toJson(Map<String, SessionOptionValue> opts) throws AdbcException {
    Map<String, @Nullable Object> map = new LinkedHashMap<>();
    for (Map.Entry<String, SessionOptionValue> e : opts.entrySet()) {
      map.put(e.getKey(), e.getValue().acceptVisitor(TO_JSON_JAVA));
    }
    try {
      return MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw AdbcException.internal("[Flight SQL] Failed to serialize session options").withCause(e);
    }
  }

  /** Parses a strict JSON string array (used when a string-list option is supplied as JSON). */
  static String[] parseJsonArray(String json) throws AdbcException {
    try {
      final JsonNode root = MAPPER.readTree(json);
      if (root == null || !root.isArray()) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] Expected JSON array for string list option, got: " + json);
      }

      final String[] result = new String[root.size()];
      for (int i = 0; i < root.size(); i++) {
        final JsonNode element = root.get(i);
        if (!element.isTextual()) {
          throw AdbcException.invalidArgument(
              "[Flight SQL] Expected string at index "
                  + i
                  + " in string list option, got: "
                  + element);
        }
        result[i] = element.textValue();
      }
      return result;
    } catch (JsonProcessingException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] Expected JSON array for string list option, got: " + json)
          .withCause(e);
    }
  }

  /** Validates and defensively copies a direct string-list option value. */
  static String[] validateStringArray(String[] values) throws AdbcException {
    if (values == null) {
      throw AdbcException.invalidArgument("[Flight SQL] String list option must not be null");
    }
    final String[] result = values.clone();
    for (int i = 0; i < result.length; i++) {
      if (result[i] == null) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] String list option contains null at index " + i);
      }
    }
    return result;
  }

  /** Returns whether the requested key type is supported by the given session option prefix. */
  static boolean supportsType(TypedKey<?> key, String prefix) {
    final Class<?> type = key.getType();
    if (prefix.equals(FlightSqlConnectionProperties.SESSION_OPTION_BOOL_PREFIX)) {
      return type == Boolean.class;
    }
    if (prefix.equals(FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX)) {
      return type == String[].class || type == String.class;
    }
    if (prefix.equals(FlightSqlConnectionProperties.SESSION_OPTION_PREFIX)) {
      return type == String.class || type == Long.class || type == Double.class;
    }
    return false;
  }

  /**
   * Casts a raw Java value extracted via {@link #TO_JAVA} according to the session option prefix
   * and requested {@link TypedKey} type. The Flight value type must match exactly; the only
   * representation conversion is String[] to JSON String for the string-list prefix. Returns {@code
   * null} for unsupported key types so the caller can delegate to the default {@code
   * AdbcConnection} implementation.
   */
  static <T> @Nullable T cast(TypedKey<T> key, Object raw, String optionName) throws AdbcException {
    final String k = key.getKey();
    if (k.startsWith(FlightSqlConnectionProperties.SESSION_OPTION_BOOL_PREFIX)) {
      return cast(key, raw, optionName, FlightSqlConnectionProperties.SESSION_OPTION_BOOL_PREFIX);
    }
    if (k.startsWith(FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX)) {
      return cast(
          key, raw, optionName, FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX);
    }
    if (k.startsWith(FlightSqlConnectionProperties.SESSION_OPTION_PREFIX)) {
      return cast(key, raw, optionName, FlightSqlConnectionProperties.SESSION_OPTION_PREFIX);
    }
    return null;
  }

  private static <T> @Nullable T cast(TypedKey<T> key, Object raw, String optionName, String prefix)
      throws AdbcException {
    final Class<T> type = key.getType();

    if (prefix.equals(FlightSqlConnectionProperties.SESSION_OPTION_BOOL_PREFIX)) {
      if (type != Boolean.class) {
        return null;
      }
      if (raw instanceof Boolean) {
        return key.cast(raw);
      }
      throw typeMismatch(optionName, raw, type);
    }

    if (prefix.equals(FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX)) {
      if (type == String[].class) {
        if (raw instanceof String[]) {
          return key.cast(raw);
        }
        throw typeMismatch(optionName, raw, type);
      }
      if (type == String.class) {
        if (!(raw instanceof String[])) {
          throw typeMismatch(optionName, raw, String[].class);
        }
        try {
          return key.cast(MAPPER.writeValueAsString(raw));
        } catch (JsonProcessingException e) {
          throw AdbcException.internal(
                  "[Flight SQL] Failed to serialize string list option as JSON")
              .withCause(e);
        }
      }
      return null;
    }

    if (prefix.equals(FlightSqlConnectionProperties.SESSION_OPTION_PREFIX)) {
      if (type == String.class) {
        if (raw instanceof String) {
          return key.cast(raw);
        }
        throw typeMismatch(optionName, raw, type);
      }
      if (type == Long.class) {
        if (raw instanceof Long) {
          return key.cast(raw);
        }
        throw typeMismatch(optionName, raw, type);
      }
      if (type == Double.class) {
        if (raw instanceof Double) {
          return key.cast(raw);
        }
        throw typeMismatch(optionName, raw, type);
      }
      return null;
    }

    return null;
  }

  private static AdbcException typeMismatch(String optionName, Object raw, Class<?> expectedType) {
    return new AdbcException(
        "[Flight SQL] Session option '"
            + optionName
            + "' has type "
            + raw.getClass().getSimpleName()
            + ", not "
            + expectedType.getSimpleName(),
        null,
        AdbcStatusCode.NOT_FOUND,
        null,
        0);
  }

  /** Looks up a session option by name, throwing {@code NOT_FOUND} if absent. */
  static SessionOptionValue require(Map<String, SessionOptionValue> opts, String name)
      throws AdbcException {
    SessionOptionValue val = opts.get(name);
    if (val == null) {
      throw new AdbcException(
          "[Flight SQL] Session option not found: " + name,
          null,
          AdbcStatusCode.NOT_FOUND,
          null,
          0);
    }
    return val;
  }

  /** Strictly parses "true"/"false" (case-insensitive); rejects anything else. */
  static boolean parseStrictBoolean(String s, String optionName) throws AdbcException {
    if ("true".equalsIgnoreCase(s)) return true;
    if ("false".equalsIgnoreCase(s)) return false;
    throw AdbcException.invalidArgument(
        "[Flight SQL] Session option '" + optionName + "' cannot be parsed as Boolean: " + s);
  }

  private FlightSqlSessionUtil() {}
}

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
   * Extracts a native Java value from a {@link SessionOptionValue}. JSON has no representation for
   * NaN/Infinity, so non-finite doubles are converted to their {@link String} form (e.g. {@code
   * "NaN"}) here rather than special-cased later; String[] is defensively cloned; Void returns
   * {@code null} (callers must handle null before calling {@link #cast}).
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
      map.put(e.getKey(), e.getValue().acceptVisitor(TO_JAVA));
    }
    try {
      return MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw AdbcException.internal("[Flight SQL] Failed to serialize session options").withCause(e);
    }
  }

  /** Parses a JSON string array (used when a string-list option is supplied as JSON). */
  static String[] parseJsonArray(String json) throws AdbcException {
    try {
      return MAPPER.readValue(json, String[].class);
    } catch (JsonProcessingException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] Expected JSON array for string list option, got: " + json)
          .withCause(e);
    }
  }

  /**
   * Casts a raw Java value extracted via {@link #TO_JAVA} to the type requested by a {@link
   * TypedKey}. {@code raw} must not be {@code null} (Void options must be rejected before calling
   * this). Returns {@code null} for unsupported types so the caller can delegate to the default
   * {@code AdbcConnection} implementation.
   */
  @SuppressWarnings("unchecked")
  static <T> @Nullable T cast(TypedKey<T> key, Object raw, String optionName) throws AdbcException {
    final Class<T> type = key.getType();
    if (type == String.class) {
      if (raw instanceof String[]) {
        try {
          return (T) MAPPER.writeValueAsString(raw);
        } catch (JsonProcessingException e) {
          throw new AdbcException(
              "[Flight SQL] Failed to serialize string list option as JSON",
              e,
              AdbcStatusCode.INTERNAL,
              null,
              0);
        }
      }
      return (T) String.valueOf(raw);
    }
    if (type == Boolean.class) {
      if (raw instanceof Boolean) return (T) raw;
      return (T) Boolean.valueOf(parseStrictBoolean(String.valueOf(raw), optionName));
    }
    if (type == String[].class) {
      if (raw instanceof String[]) return (T) raw;
      return (T) new String[] {String.valueOf(raw)};
    }
    try {
      if (type == Long.class) {
        if (raw instanceof Long) return (T) raw;
        if (raw instanceof Number) return (T) Long.valueOf(((Number) raw).longValue());
        return (T) Long.valueOf(Long.parseLong(String.valueOf(raw)));
      }
      if (type == Double.class) {
        if (raw instanceof Double) return (T) raw;
        if (raw instanceof Number) return (T) Double.valueOf(((Number) raw).doubleValue());
        return (T) Double.valueOf(Double.parseDouble(String.valueOf(raw)));
      }
    } catch (NumberFormatException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] Session option '"
                  + optionName
                  + "' cannot be parsed as "
                  + type.getSimpleName()
                  + ": "
                  + raw)
          .withCause(e);
    }
    return null;
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

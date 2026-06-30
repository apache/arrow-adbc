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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SessionOptionValueVisitor;

/** Helpers for Flight SQL session option serialization and type conversion. */
final class FlightSqlSessionUtil {

  private FlightSqlSessionUtil() {}

  // -- JSON helpers --

  static String sessionOptionsToJson(Map<String, SessionOptionValue> opts) {
    final StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, SessionOptionValue> e : opts.entrySet()) {
      if (!first) sb.append(',');
      sb.append('"').append(escapeJson(e.getKey())).append("\":").append(sessionValueToJson(e.getValue()));
      first = false;
    }
    return sb.append('}').toString();
  }

  static String sessionValueToJson(SessionOptionValue val) {
    return val.acceptVisitor(JSON_VALUE_VISITOR);
  }

  static String toJsonArray(String[] values) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < values.length; i++) {
      if (i > 0) sb.append(',');
      sb.append('"').append(escapeJson(values[i])).append('"');
    }
    return sb.append(']').toString();
  }

  static String[] parseJsonArray(String json) throws AdbcException {
    final String trimmed = json.trim();
    if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
      throw AdbcException.invalidArgument(
          "[Flight SQL] Expected JSON array for string list option, got: " + json);
    }
    final String inner = trimmed.substring(1, trimmed.length() - 1).trim();
    if (inner.isEmpty()) {
      return new String[0];
    }
    final List<String> result = new ArrayList<>();
    int i = 0;
    while (i < inner.length()) {
      while (i < inner.length() && Character.isWhitespace(inner.charAt(i))) i++;
      if (i >= inner.length()) break;
      if (inner.charAt(i) != '"') {
        throw AdbcException.invalidArgument(
            "[Flight SQL] Invalid JSON string array element at position " + i + " in: " + json);
      }
      i++; // skip opening quote
      final StringBuilder elem = new StringBuilder();
      while (i < inner.length() && inner.charAt(i) != '"') {
        if (inner.charAt(i) == '\\' && i + 1 < inner.length()) {
          i++;
          switch (inner.charAt(i)) {
            case '"': elem.append('"'); break;
            case '\\': elem.append('\\'); break;
            case '/': elem.append('/'); break;
            case 'n': elem.append('\n'); break;
            case 'r': elem.append('\r'); break;
            case 't': elem.append('\t'); break;
            case 'b': elem.append('\b'); break;
            case 'f': elem.append('\f'); break;
            case 'u':
              if (i + 4 < inner.length()) {
                final String hex = inner.substring(i + 1, i + 5);
                try {
                  elem.append((char) Integer.parseInt(hex, 16));
                  i += 4;
                } catch (NumberFormatException e) {
                  elem.append('u');
                }
              } else {
                elem.append('u');
              }
              break;
            default: elem.append(inner.charAt(i));
          }
        } else {
          elem.append(inner.charAt(i));
        }
        i++;
      }
      if (i >= inner.length()) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] Unterminated string in JSON array: " + json);
      }
      i++; // skip closing quote
      result.add(elem.toString());
      while (i < inner.length() && Character.isWhitespace(inner.charAt(i))) i++;
      if (i < inner.length() && inner.charAt(i) == ',') i++;
    }
    return result.toArray(new String[0]);
  }

  static String escapeJson(String s) {
    final StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"': sb.append("\\\""); break;
        case '\\': sb.append("\\\\"); break;
        case '\n': sb.append("\\n"); break;
        case '\r': sb.append("\\r"); break;
        case '\t': sb.append("\\t"); break;
        case '\b': sb.append("\\b"); break;
        case '\f': sb.append("\\f"); break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.toString();
  }

  // -- SessionOptionValue visitors --

  static final SessionOptionValueVisitor<Boolean> BOOL_VISITOR =
      new SessionOptionValueVisitor<Boolean>() {
        @Override public Boolean visit(String v) { return Boolean.parseBoolean(v); }
        @Override public Boolean visit(boolean v) { return v; }
        @Override public Boolean visit(long v) { return v != 0; }
        @Override public Boolean visit(double v) { return v != 0; }
        @Override public Boolean visit(String[] v) { return false; }
        @Override public Boolean visit(Void v) { return false; }
      };

  static final SessionOptionValueVisitor<String[]> STRING_ARRAY_VISITOR =
      new SessionOptionValueVisitor<String[]>() {
        @Override public String[] visit(String v) { return new String[]{v}; }
        @Override public String[] visit(boolean v) { return new String[]{String.valueOf(v)}; }
        @Override public String[] visit(long v) { return new String[]{String.valueOf(v)}; }
        @Override public String[] visit(double v) { return new String[]{String.valueOf(v)}; }
        @Override public String[] visit(String[] v) { return v.clone(); }
        @Override public String[] visit(Void v) { return new String[0]; }
      };

  static final SessionOptionValueVisitor<String> STRING_VISITOR =
      new SessionOptionValueVisitor<String>() {
        @Override public String visit(String v) { return v; }
        @Override public String visit(boolean v) { return String.valueOf(v); }
        @Override public String visit(long v) { return String.valueOf(v); }
        @Override public String visit(double v) { return String.valueOf(v); }
        @Override public String visit(String[] v) { return toJsonArray(v); }
        @Override public String visit(Void v) { return ""; }
      };

  static final SessionOptionValueVisitor<Long> LONG_VISITOR =
      new SessionOptionValueVisitor<Long>() {
        @Override public Long visit(String v) { return Long.parseLong(v); }
        @Override public Long visit(boolean v) { return v ? 1L : 0L; }
        @Override public Long visit(long v) { return v; }
        @Override public Long visit(double v) { return (long) v; }
        @Override public Long visit(String[] v) { return 0L; }
        @Override public Long visit(Void v) { return 0L; }
      };

  static final SessionOptionValueVisitor<Double> DOUBLE_VISITOR =
      new SessionOptionValueVisitor<Double>() {
        @Override public Double visit(String v) { return Double.parseDouble(v); }
        @Override public Double visit(boolean v) { return v ? 1.0 : 0.0; }
        @Override public Double visit(long v) { return (double) v; }
        @Override public Double visit(double v) { return v; }
        @Override public Double visit(String[] v) { return 0.0; }
        @Override public Double visit(Void v) { return 0.0; }
      };

  static final SessionOptionValueVisitor<String> JSON_VALUE_VISITOR =
      new SessionOptionValueVisitor<String>() {
        @Override public String visit(String v) { return '"' + escapeJson(v) + '"'; }
        @Override public String visit(boolean v) { return String.valueOf(v); }
        @Override public String visit(long v) { return String.valueOf(v); }
        @Override public String visit(double v) {
          if (Double.isNaN(v) || Double.isInfinite(v)) {
            return "null";
          }
          return String.valueOf(v);
        }
        @Override public String visit(String[] v) { return toJsonArray(v); }
        @Override public String visit(Void v) { return "null"; }
      };
}

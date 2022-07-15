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
package org.apache.arrow.adbc.driver.jdbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;

final class JdbcDriverUtil {
  // Do our best to properly map database-specific errors to NOT_FOUND status.
  private static final Set<String> SQLSTATE_TABLE_NOT_FOUND =
      new HashSet<>(
          Arrays.asList(
              // Apache Derby https://db.apache.org/derby/docs/10.4/ref/rrefexcept71493.html
              "42X05",
              // MySQL
              // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
              "42S02",
              // Postgres https://www.postgresql.org/docs/current/errcodes-appendix.html
              "42P01"));

  private JdbcDriverUtil() {
    throw new AssertionError("Do not instantiate this class");
  }

  static String prefixExceptionMessage(final String s) {
    return "[JDBC] " + s;
  }

  static AdbcStatusCode guessStatusCode(String sqlState) {
    if (sqlState == null) {
      return AdbcStatusCode.UNKNOWN;
    } else if (SQLSTATE_TABLE_NOT_FOUND.contains(sqlState)) {
      return AdbcStatusCode.NOT_FOUND;
    }
    return AdbcStatusCode.UNKNOWN;
  }

  static AdbcException fromSqlException(SQLException e) {
    return new AdbcException(
        prefixExceptionMessage(e.getMessage()),
        e.getCause(),
        guessStatusCode(e.getSQLState()),
        e.getSQLState(),
        e.getErrorCode());
  }

  static AdbcException fromSqlException(String format, SQLException e, Object... values) {
    return fromSqlException(guessStatusCode(e.getSQLState()), format, e, values);
  }

  static AdbcException fromSqlException(
      AdbcStatusCode status, String format, SQLException e, Object... values) {
    final String message = "[JDBC] " + String.format(format, values) + e.getMessage();
    return new AdbcException(message, e.getCause(), status, e.getSQLState(), e.getErrorCode());
  }
}

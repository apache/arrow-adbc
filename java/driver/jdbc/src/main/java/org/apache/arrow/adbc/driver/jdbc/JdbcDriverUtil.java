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
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;

final class JdbcDriverUtil {
  private JdbcDriverUtil() {
    throw new AssertionError("Do not instantiate this class");
  }

  static String prefixExceptionMessage(final String s) {
    return "[JDBC] " + s;
  }

  static AdbcException fromSqlException(final SQLException e) {
    return new AdbcException(
        prefixExceptionMessage(e.getMessage()),
        e.getCause(),
        AdbcStatusCode.UNKNOWN,
        e.getSQLState(),
        e.getErrorCode());
  }
}

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

package org.apache.arrow.adbc.driver.jdbc.db2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.adbc.driver.testsuite.SqlTestUtil;
import org.apache.arrow.adbc.driver.testsuite.SqlValidationQuirks;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.TimeUnit;
import org.junit.jupiter.api.Assumptions;

public class Db2Quirks extends SqlValidationQuirks {
  static final String DB2_URL_ENV_VAR = "ADBC_JDBC_DB2_URL";
  static final String DB2_USER_ENV_VAR = "ADBC_JDBC_DB2_USER";
  static final String DB2_PASSWORD_ENV_VAR = "ADBC_JDBC_DB2_PASSWORD";
  static final String DB2_DATABASE_ENV_VAR = "ADBC_JDBC_DB2_DATABASE";

  String catalog = "";

  static String makeJdbcUrl() {
    final String db2Url = System.getenv(DB2_URL_ENV_VAR);
    final String user = System.getenv(DB2_USER_ENV_VAR);
    final String password = System.getenv(DB2_PASSWORD_ENV_VAR);

    if (SqlTestUtil.isCI() && (db2Url == null || db2Url.isEmpty())) {
      throw new RuntimeException("DB2 not found, set " + DB2_URL_ENV_VAR);
    }

    Assumptions.assumeFalse(db2Url == null, "DB2 not found, set " + DB2_URL_ENV_VAR);
    Assumptions.assumeFalse(db2Url.isEmpty(), "DB2 not found, set " + DB2_URL_ENV_VAR);

    StringBuilder url = new StringBuilder("jdbc:db2://").append(db2Url);
    if (user != null && !user.isEmpty()) {
      url.append(url.toString().contains("?") ? "&" : "?");
      url.append("user=").append(user);
      if (password != null && !password.isEmpty()) {
        url.append("&password=").append(password);
      }
    }
    return url.toString();
  }

  public Connection getJdbcConnection() throws SQLException {
    return DriverManager.getConnection(makeJdbcUrl());
  }

  @Override
  public AdbcDatabase initDatabase(BufferAllocator allocator) throws AdbcException {
    String url = makeJdbcUrl();

    final String database = System.getenv(DB2_DATABASE_ENV_VAR);
    if (SqlTestUtil.isCI() && database == null) {
      throw new RuntimeException("DB2 database not found, set " + DB2_DATABASE_ENV_VAR);
    }
    Assumptions.assumeFalse(
        database == null, "DB2 database not found, set " + DB2_DATABASE_ENV_VAR);
    this.catalog = database;

    final Map<String, Object> parameters = new HashMap<>();
    AdbcDriver.PARAM_URI.set(parameters, url);
    return new JdbcDriver(allocator).open(parameters);
  }

  @Override
  public void cleanupTable(String name) throws Exception {
    try (final Connection connection = DriverManager.getConnection(makeJdbcUrl())) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE " + name);
      } catch (SQLException ignored) {
      }
    }
  }

  @Override
  public String defaultCatalog() {
    return catalog;
  }

  @Override
  public String defaultDbSchema() {
    return null;
  }

  @Override
  public String caseFoldTableName(String name) {
    return name.toUpperCase();
  }

  @Override
  public String caseFoldColumnName(String name) {
    return name.toUpperCase();
  }

  @Override
  public TimeUnit defaultTimeUnit() {
    return TimeUnit.MICROSECOND;
  }

  @Override
  public TimeUnit defaultTimestampUnit() {
    return TimeUnit.MICROSECOND;
  }

  @Override
  public boolean supportsCurrentCatalog() {
    return false;
  }
}

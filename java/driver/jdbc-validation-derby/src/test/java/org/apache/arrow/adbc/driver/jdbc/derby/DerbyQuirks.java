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

package org.apache.arrow.adbc.driver.jdbc.derby;

import java.nio.file.Path;
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
import org.apache.arrow.adbc.driver.testsuite.SqlValidationQuirks;

public class DerbyQuirks extends SqlValidationQuirks {
  private final String jdbcUrl;

  public DerbyQuirks(Path databaseRoot) {
    this.jdbcUrl = "jdbc:derby:" + databaseRoot.toString() + "/db;create=true";
  }

  @Override
  public AdbcDatabase initDatabase() throws AdbcException {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put(AdbcDriver.PARAM_URL, jdbcUrl);
    return JdbcDriver.INSTANCE.open(parameters);
  }

  @Override
  public void cleanupTable(String name) throws Exception {
    try (final Connection connection1 = DriverManager.getConnection(jdbcUrl)) {
      try (Statement statement = connection1.createStatement()) {
        statement.execute("DROP TABLE " + name);
      } catch (SQLException ignored) {
      }
    }
  }

  @Override
  public String caseFoldTableName(String name) {
    return name.toUpperCase();
  }

  @Override
  public String caseFoldColumnName(String name) {
    return name.toUpperCase();
  }
}

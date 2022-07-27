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

import java.util.Map;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;

/** An ADBC driver wrapping the JDBC API. */
public enum JdbcDriver implements AdbcDriver {
  INSTANCE;

  private final BufferAllocator allocator;

  JdbcDriver() {
    allocator = new RootAllocator();
    AdbcDriverManager.getInstance().registerDriver("org.apache.arrow.adbc.driver.jdbc", this);
  }

  @Override
  public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
    Object target = parameters.get(PARAM_URL);
    if (!(target instanceof String)) {
      throw AdbcException.invalidArgument("[JDBC] Must provide String " + PARAM_URL + " parameter");
    }
    Object quirks = parameters.get(PARAM_SQL_QUIRKS);
    if (quirks != null) {
      Preconditions.checkArgument(
          quirks instanceof SqlQuirks,
          String.format(
              "[JDBC] %s must be a SqlQuirks instance, not %s",
              PARAM_SQL_QUIRKS, quirks.getClass().getName()));
    } else {
      quirks = new SqlQuirks();
    }
    return new JdbcDatabase(allocator, (String) target, (SqlQuirks) quirks);
  }
}

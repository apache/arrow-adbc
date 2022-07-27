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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.memory.BufferAllocator;

/** An instance of a database (e.g. a handle to an in-memory database). */
public final class JdbcDatabase implements AdbcDatabase {
  private final BufferAllocator allocator;
  private final String target;
  private final SqlQuirks quirks;
  private final Connection connection;
  private final AtomicInteger counter;

  JdbcDatabase(BufferAllocator allocator, final String target, SqlQuirks quirks)
      throws AdbcException {
    this.allocator = allocator;
    this.target = target;
    this.quirks = quirks;
    try {
      this.connection = DriverManager.getConnection(target);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    this.counter = new AtomicInteger();
  }

  @Override
  public AdbcConnection connect() throws AdbcException {
    final Connection connection;
    try {
      connection = DriverManager.getConnection(target);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    final int count = counter.getAndIncrement();
    return new JdbcConnection(
        allocator.newChildAllocator("adbc-jdbc-connection-" + count, 0, allocator.getLimit()),
        connection,
        quirks);
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  @Override
  public String toString() {
    return "JdbcDatabase{" + "target='" + target + '\'' + '}';
  }
}

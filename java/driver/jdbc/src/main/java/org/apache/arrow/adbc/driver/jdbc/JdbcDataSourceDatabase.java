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
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An instance of a database based on a {@link DataSource}. */
public final class JdbcDataSourceDatabase implements AdbcDatabase {
  private final BufferAllocator allocator;
  private final DataSource dataSource;
  private final @Nullable String username;
  private final @Nullable String password;
  private final JdbcQuirks quirks;
  private final AtomicInteger counter;
  private @Nullable Connection connection;

  JdbcDataSourceDatabase(
      BufferAllocator allocator,
      DataSource dataSource,
      @Nullable String username,
      @Nullable String password,
      JdbcQuirks quirks)
      throws AdbcException {
    this.allocator = Objects.requireNonNull(allocator);
    this.dataSource = Objects.requireNonNull(dataSource);
    this.username = username;
    this.password = password;
    this.quirks = Objects.requireNonNull(quirks);
    this.connection = null;
    this.counter = new AtomicInteger();
  }

  @Override
  public AdbcConnection connect() throws AdbcException {
    @Nullable Connection conn = this.connection;
    try {
      if (conn == null) {
        if (username != null && password != null) {
          conn = this.connection = dataSource.getConnection(username, password);
        } else {
          conn = this.connection = dataSource.getConnection();
        }
      }
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
    final int count = counter.getAndIncrement();
    return new JdbcConnection(
        allocator.newChildAllocator(
            "adbc-jdbc-datasource-connection-" + count, 0, allocator.getLimit()),
        conn,
        quirks);
  }

  @Override
  public void close() throws AdbcException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw AdbcException.io(e);
      }
    }
    connection = null;
  }

  @Override
  public String toString() {
    return "JdbcDatabase{" + "dataSource='" + dataSource + '\'' + '}';
  }
}

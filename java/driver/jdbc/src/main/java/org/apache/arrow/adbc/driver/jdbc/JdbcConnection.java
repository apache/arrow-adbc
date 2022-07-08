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
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;

public class JdbcConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final Connection connection;

  JdbcConnection(BufferAllocator allocator, Connection connection) {
    this.allocator = allocator;
    this.connection = connection;
  }

  @Override
  public void commit() throws AdbcException {
    try {
      checkAutoCommit();
      connection.commit();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public AdbcStatement createStatement() throws AdbcException {
    return new JdbcStatement(allocator, connection);
  }

  @Override
  public AdbcStatement bulkIngest(String targetTableName) throws AdbcException {
    return JdbcStatement.ingestRoot(allocator, connection, targetTableName);
  }

  @Override
  public void rollback() throws AdbcException {
    try {
      checkAutoCommit();
      connection.rollback();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public boolean getAutoCommit() throws AdbcException {
    try {
      return connection.getAutoCommit();
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void setAutoCommit(boolean enableAutoCommit) throws AdbcException {
    try {
      connection.setAutoCommit(enableAutoCommit);
    } catch (SQLException e) {
      throw JdbcDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  private void checkAutoCommit() throws SQLException {
    if (connection.getAutoCommit()) {
      throw new IllegalStateException("Cannot perform operation in autocommit mode");
    }
  }

  @Override
  public String toString() {
    return "JdbcConnection{" + "connection=" + connection + '}';
  }
}

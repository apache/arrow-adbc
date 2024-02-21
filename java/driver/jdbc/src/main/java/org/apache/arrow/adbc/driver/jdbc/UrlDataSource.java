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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Adapt a JDBC URL to the DataSource interface. */
class UrlDataSource implements DataSource {
  final String target;
  @Nullable PrintWriter logWriter;
  int loginTimeout;

  UrlDataSource(String target) {
    this.target = Objects.requireNonNull(target);
    this.logWriter = null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return DriverManager.getConnection(target);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return DriverManager.getConnection(target, username, password);
  }

  @Override
  @SuppressWarnings("override.return")
  public @Nullable PrintWriter getLogWriter() throws SQLException {
    return logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    logWriter = out;
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return loginTimeout;
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    loginTimeout = seconds;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("UrlDataSource does not support getParentLogger");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(this);
    } catch (ClassCastException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}

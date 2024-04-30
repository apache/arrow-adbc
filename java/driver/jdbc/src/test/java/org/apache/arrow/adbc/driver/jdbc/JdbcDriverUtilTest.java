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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.ConnectException;
import java.sql.SQLException;
import org.apache.arrow.adbc.core.AdbcException;
import org.junit.jupiter.api.Test;

public class JdbcDriverUtilTest {

  @Test
  public void fromKnownSQLException() {
    SQLException knownException = new SQLException("Known SQL exception", "S1000", 100, null);
    AdbcException adbcException = JdbcDriverUtil.fromSqlException(knownException);
    assertEquals(
        JdbcDriverUtil.prefixExceptionMessage(knownException.getMessage()),
        adbcException.getMessage());
    assertEquals(adbcException.getVendorCode(), knownException.getErrorCode());
    assertEquals(adbcException.getSqlState(), knownException.getSQLState());
  }

  @Test
  public void fromUnknownSQLException() {
    SQLException unknownSqlException = new SQLException("Unknown SQL exception");
    AdbcException adbcException = JdbcDriverUtil.fromSqlException(unknownSqlException);
    assertEquals(
        JdbcDriverUtil.prefixExceptionMessage(unknownSqlException.getMessage()),
        adbcException.getMessage());
    assertEquals(adbcException.getVendorCode(), 0);
    assertNull(adbcException.getSqlState());
  }

  @Test
  public void fromWrappedKnownSQLException() {
    // Wrap a known exception in an unknown exception.
    SQLException knownException = new SQLException("Known SQL exception", "S1000", 100, null);
    SQLException unknownSqlException = new SQLException("Unknown SQL exception", knownException);

    AdbcException adbcException = JdbcDriverUtil.fromSqlException(unknownSqlException);
    assertEquals(
        JdbcDriverUtil.prefixExceptionMessage(knownException.getMessage()),
        adbcException.getMessage());
    assertEquals(adbcException.getVendorCode(), knownException.getErrorCode());
    assertEquals(adbcException.getSqlState(), knownException.getSQLState());
  }

  @Test
  public void fromSQLExceptionCausedByConnectException() {
    // Wrap a known exception in an unknown exception.
    ConnectException connectException = new ConnectException();
    SQLException knownException =
        new SQLException("Known SQL exception", "S1000", 100, connectException);

    AdbcException adbcException = JdbcDriverUtil.fromSqlException(knownException);
    assertEquals(knownException.getMessage(), adbcException.getMessage());
    assertEquals(adbcException.getVendorCode(), knownException.getErrorCode());
    assertEquals(adbcException.getSqlState(), knownException.getSQLState());
    assertEquals(adbcException.getSqlState(), knownException.getSQLState());
  }
}

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
package org.apache.arrow.adbc.driver.jni;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.testsuite.ArrowToJava;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration test that uses multiple drivers simultaneously. */
public class MultiDriverIntegrationTest {
  BufferAllocator allocator;
  JniDriver driver;

  AdbcDatabase pgDb, mssqlDb, fsqlDb;
  AdbcConnection pgConn, mssqlConn, fsqlConn;

  @BeforeAll
  static void beforeAll() {
    Assumptions.assumeFalse(
        FlightSqlSqliteIntegrationTest.URI == null || FlightSqlSqliteIntegrationTest.URI.isEmpty(),
        String.format("Must set %s", FlightSqlSqliteIntegrationTest.URI_ENV));
    Assumptions.assumeFalse(
        PostgresIntegrationTest.URI == null || PostgresIntegrationTest.URI.isEmpty(),
        String.format("Must set %s", PostgresIntegrationTest.URI_ENV));
    Assumptions.assumeFalse(
        SqlServerIntegrationTest.URI == null || SqlServerIntegrationTest.URI.isEmpty(),
        String.format("Must set %s", SqlServerIntegrationTest.URI_ENV));
  }

  @BeforeEach
  void beforeEach() throws Exception {
    allocator = new RootAllocator();
    driver = new JniDriver(allocator);

    {
      System.err.println(
          "Connecting to Flight SQL with URI: " + FlightSqlSqliteIntegrationTest.URI);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_flightsql");
      AdbcDriver.PARAM_URI.set(parameters, FlightSqlSqliteIntegrationTest.URI);
      fsqlDb = driver.open(parameters);
      fsqlConn = fsqlDb.connect();
    }
    {
      System.err.println("Connecting to MSSQL with URI: " + SqlServerIntegrationTest.URI);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "mssql");
      AdbcDriver.PARAM_URI.set(parameters, SqlServerIntegrationTest.URI);
      mssqlDb = driver.open(parameters);
      mssqlConn = mssqlDb.connect();
    }
    {
      System.err.println("Connecting to PostgreSQL with URI: " + PostgresIntegrationTest.URI);
      Map<String, Object> parameters = new HashMap<>();
      JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_postgresql");
      AdbcDriver.PARAM_URI.set(parameters, PostgresIntegrationTest.URI);
      pgDb = driver.open(parameters);
      pgConn = pgDb.connect();
    }
  }

  @AfterEach
  void afterEach() throws Exception {
    AutoCloseables.close(pgConn, mssqlConn, fsqlConn, pgDb, mssqlDb, fsqlDb, allocator);
  }

  @Test
  void queryAll() throws Exception {
    try (var pgStmt = pgConn.createStatement();
        var mssqlStmt = mssqlConn.createStatement();
        var fsqlStmt = fsqlConn.createStatement()) {
      pgStmt.setSqlQuery("SELECT 1 as foo");
      mssqlStmt.setSqlQuery("SELECT 1 as bar");
      fsqlStmt.setSqlQuery("SELECT 1 as baz");

      try (var pgRes = pgStmt.executeQuery();
          var mssqlRes = mssqlStmt.executeQuery();
          var fsqlRes = fsqlStmt.executeQuery()) {
        assertThat(ArrowToJava.toIntegers(pgRes.getReader(), "foo")).containsExactly(1);
        assertThat(ArrowToJava.toIntegers(mssqlRes.getReader(), "bar")).containsExactly(1);
        assertThat(ArrowToJava.toLongs(fsqlRes.getReader(), "baz")).containsExactly(1L);
      }
    }
  }

  @Test
  void errorDriverDoesNotExist() {
    Map<String, Object> parameters = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(parameters, "thisdriverdoesnotexist");
    assertThatThrownBy(
            () -> {
              try (var db = driver.open(parameters)) {}
            })
        .isInstanceOf(AdbcException.class);
  }

  @Test
  void errorFailedConnection() throws Exception {
    Map<String, Object> parameters = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(parameters, "mssql");
    AdbcDriver.PARAM_URI.set(parameters, "mssql://localhost:9999");
    try (var db = driver.open(parameters)) {
      assertThatThrownBy(db::connect).hasMessageContaining("Could not get connection");
    }
  }

  @Test
  void errorBadConnectionParameter() throws Exception {
    Map<String, Object> parameters = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(parameters, "mssql");
    parameters.put("this parameter does not exist", "");
    AdbcDriver.PARAM_URI.set(parameters, "mssql://localhost:9999");
    assertThatThrownBy(() -> driver.open(parameters))
        .hasMessageContaining("Unknown database option");
  }
}

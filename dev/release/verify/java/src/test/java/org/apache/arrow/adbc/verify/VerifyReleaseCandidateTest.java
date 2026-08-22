// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.arrow.adbc.verify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Briefly test that shipped JARs actually work. */
class VerifyReleaseCandidateTest {
  BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  @Test
  void testDriverFlightSql() throws Exception {
    var driver = new FlightSqlDriver(allocator);
    Map<String, Object> params = new HashMap<>();
    FlightSqlDriver.PARAM_URI.set(params, "grpc://localhost:1212");
    try (AdbcDatabase database = driver.open(params)) {
      assertThrows(
          AdbcException.class,
          () -> {
            //noinspection EmptyTryBlock
            try (AdbcConnection ignored = database.connect()) {}
          });
    }
  }

  @Test
  void testDriverJdbcAdapter() throws Exception {
    var driver = new JdbcDriver(allocator);
    Map<String, Object> params = new HashMap<>();
    params.put(JdbcDriver.PARAM_URI, "jdbc:driverdoesnotexist://");
    try (AdbcDatabase database = driver.open(params)) {
      assertThrows(
          AdbcException.class,
          () -> {
            //noinspection EmptyTryBlock
            try (AdbcConnection ignored = database.connect()) {}
          });
    }
  }

  @Test
  void testDriverJni() {
    // smoke test: try to load a driver that does not exist; should fail with a proper error
    // (not something like a linker error, which would imply the JNI shim doesn't work)
    var driver = new JniDriver(allocator);
    AdbcException exception =
        assertThrows(AdbcException.class, () -> driver.load().driver("nonexistent").open());
    assertThat(exception).hasMessageContaining("Could not load `nonexistent`");
  }

  @Test
  void testManagedDriverManager() {
    // Not the JNI/C driver manager.
    var manager = AdbcDriverManager.getInstance();
    assertThat(manager).isNotNull();

    for (var factory :
        List.of(
            "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory",
            "org.apache.arrow.adbc.driver.jdbc.JdbcDriverFactory",
            "org.apache.arrow.adbc.driver.jni.JniDriverFactory")) {
      assertThat(
              assertThrows(
                  IllegalStateException.class,
                  () -> manager.registerDriver(factory, (allocator) -> null)))
          .hasMessageContaining("Driver factory already registered");
    }
  }
}

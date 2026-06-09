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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.testsuite.ArrowAssertions;
import org.apache.arrow.adbc.driver.testsuite.ArrowToJava;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests against the mock test server. */
public class FlightSqlIntegrationTest {
  public static final String URI_ENV = "ADBC_TEST_FLIGHTSQL_URI";
  static String URI = System.getenv(URI_ENV);

  BufferAllocator allocator;
  JniDriver driver;
  AdbcDatabase db;
  AdbcConnection conn;

  @BeforeAll
  static void beforeAll() {
    Assumptions.assumeFalse(
        URI == null || URI.isEmpty(),
        String.format("Must set %s to run Flight SQL integration tests", URI_ENV));
  }

  @BeforeEach
  void beforeEach() throws Exception {
    allocator = new RootAllocator();
    driver = new JniDriver(allocator);
    Map<String, Object> parameters = new HashMap<>();
    JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_flightsql");
    AdbcDriver.PARAM_URI.set(parameters, URI);
    parameters.put("adbc.flight.sql.client_option.tls_skip_verify", "true");
    parameters.put("adbc.flight.sql.authorization_header", "Basic dXNlcjpwYXNzd29yZA==");
    db = driver.open(parameters);
    conn = db.connect();
  }

  @AfterEach
  void afterEach() throws Exception {
    conn.close();
    db.close();
    allocator.close();
  }

  @Test
  void simple() throws Exception {
    try (var stmt = conn.createStatement()) {
      // smoke test; the server doesn't actually run any query and returns a fixed result
      stmt.setSqlQuery("SELECT 1 + 1 AS sum");
      try (var reader = stmt.executeQuery()) {
        assertThat(ArrowToJava.toIntegers(reader.getReader(), "ints")).containsExactly(5);
      }
    }
  }

  @Test
  void substrait() throws Exception {
    try (var stmt = conn.createStatement()) {
      stmt.setSubstraitPlan(ByteBuffer.wrap(new byte[] {42, 0, (byte) 129, (byte) 255}));
      try (var reader = stmt.executeQuery()) {
        assertThat(ArrowToJava.toIntegers(reader.getReader(), "substrait")).containsExactly(5);
      }
    }
  }

  @Test
  void poll() throws Exception {
    var expectedSchema = new Schema(List.of(Field.nullable("ints", Types.MinorType.INT.getType())));
    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("poll");
      stmt.prepare();
      var iter = stmt.pollPartitioned();
      var elements = new ArrayList<AdbcStatement.PartitionResult>();
      iter.forEachRemaining(elements::add);
      assertThat(elements).size().isEqualTo(5);
      assertThat(elements.get(0).getSchema()).isNull();
      assertThat(elements.get(1).getSchema()).isNull();
      ArrowAssertions.assertSchema(elements.get(2).getSchema()).isEqualTo(expectedSchema);
      ArrowAssertions.assertSchema(elements.get(3).getSchema()).isEqualTo(expectedSchema);
      ArrowAssertions.assertSchema(elements.get(4).getSchema()).isEqualTo(expectedSchema);

      elements.forEach(
          partitionResult -> {
            assertThat(partitionResult.getPartitionDescriptors()).size().isEqualTo(1);
          });
    }
  }

  @Test
  void progress() throws Exception {
    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("poll");
      stmt.prepare();
      var iter = stmt.pollPartitioned();
      assertThat(stmt.getProgress()).isEqualTo(0.0);
      assertThat(stmt.getMaxProgress()).isEqualTo(1.0);

      for (int i = 0; i < 5; i++) {
        assertThat(iter.hasNext()).isTrue();
        assertThat(stmt.getProgress()).isCloseTo(0.2 * (i + 1), Assertions.offset(0.05));
        assertThat(stmt.getMaxProgress()).isEqualTo(1.0);
        assertThat(iter.next()).isNotNull();
      }

      assertThat(iter.hasNext()).isFalse();
    }
  }

  @Test
  void pollError() throws Exception {
    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("error_poll_later");
      stmt.prepare();
      var iter = stmt.pollPartitioned();
      assertThat(iter.hasNext()).isTrue();
      assertThat(iter.next()).isNotNull();
      assertThatThrownBy(iter::hasNext).hasMessageContaining("expected error");
    }
  }

  @Test
  void pollForever() throws Exception {
    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("forever");
      stmt.prepare();
      var iter = stmt.pollPartitioned();
      assertThat(iter.hasNext()).isTrue();
      assertThat(iter.next()).isNotNull();
      // XXX: rather janky. must cancel on the background; cancelling in between statements has no
      // effect. we should perhaps reconsider this behavior
      var t =
          new Thread(
              () -> {
                try {
                  while (stmt.getProgress() < 0.05) {
                    Thread.sleep(100);
                  }
                  System.out.println("progress: " + stmt.getProgress());
                  stmt.cancel();
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });
      t.start();

      assertThatThrownBy(iter::hasNext).hasMessageContaining("context canceled");
      t.join(10000);
    }
  }
}

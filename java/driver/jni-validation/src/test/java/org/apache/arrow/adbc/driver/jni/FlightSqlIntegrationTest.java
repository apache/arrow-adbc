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

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FlightSqlIntegrationTest {
  public static final String URI_ENV = "ADBC_SQLITE_FLIGHTSQL_URI";
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
      stmt.setSqlQuery("SELECT 1 + 1 AS sum");
      try (var reader = stmt.executeQuery()) {
        assertThat(reader.getReader().loadNextBatch()).isTrue();
        assertThat(reader.getReader().getVectorSchemaRoot().getVector("sum").getObject(0))
            .isEqualTo(2L);
      }
    }
  }

  @Test
  void partitioned() throws Exception {
    try (var stmt = conn.createStatement()) {
      stmt.setSqlQuery("SELECT 1 + 1 AS sum");
      var partitions = stmt.executePartitioned();
      assertThat(partitions.getPartitionDescriptors().size()).isEqualTo(1);
      assertThat(partitions.getAffectedRows()).isEqualTo(-1);
      // The test server doesn't give a schema.
      assertThat(partitions.getSchema()).isNull();

      try (var reader =
          conn.readPartition(partitions.getPartitionDescriptors().get(0).getDescriptor())) {
        assertThat(reader.loadNextBatch()).isTrue();
        assertThat(reader.getVectorSchemaRoot().getVector("sum").getObject(0)).isEqualTo(2L);
      }
    }
  }
}

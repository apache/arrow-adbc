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
package org.apache.arrow.adbc.driver.flightsql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link FlightSqlConnection} lifecycle (open, close, resource cleanup). */
class FlightSqlConnectionTest {
  static BufferAllocator allocator;
  static FlightServer server;
  static AdbcDriver driver;
  static AdbcDatabase database;
  AdbcConnection connection;
  long baselineMemory;

  @BeforeAll
  static void beforeAll() throws Exception {
    allocator = new RootAllocator();
    server =
        FlightServer.builder()
            .allocator(allocator)
            .producer(new DetailsTest.Producer())
            .location(Location.forGrpcInsecure("localhost", 0))
            .build();
    server.start();
    driver = new FlightSqlDriver(allocator);
    Map<String, Object> parameters = new HashMap<>();
    AdbcDriver.PARAM_URI.set(
        parameters, Location.forGrpcInsecure("localhost", server.getPort()).getUri().toString());
    database = driver.open(parameters);
  }

  @BeforeEach
  void beforeEach() throws Exception {
    baselineMemory = allocator.getAllocatedMemory();
    connection = database.connect();
  }

  @AfterEach
  void afterEach() throws Exception {
    AutoCloseables.close(connection);
  }

  @AfterAll
  static void afterAll() throws Exception {
    AutoCloseables.close(database, server, allocator);
  }

  @Test
  void testCloseDoesNotThrow() throws Exception {
    connection.close();
    connection = null;
  }

  @Test
  void testCloseFreesChildAllocator() throws Exception {
    connection.close();
    connection = null;
    assertThat(allocator.getAllocatedMemory()).isEqualTo(baselineMemory);
  }
}

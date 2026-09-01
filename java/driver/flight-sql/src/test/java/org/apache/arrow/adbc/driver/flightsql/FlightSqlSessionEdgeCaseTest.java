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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.TypedKey;
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

/** Regression tests for session-option validation and local type dispatch. */
class FlightSqlSessionEdgeCaseTest {
  static BufferAllocator allocator;
  static FlightSqlSessionTest.SessionProducer producer;
  static FlightServer server;
  static AdbcDatabase database;
  AdbcConnection connection;

  @BeforeAll
  static void beforeAll() throws Exception {
    allocator = new RootAllocator();
    producer = new FlightSqlSessionTest.SessionProducer();
    server =
        FlightServer.builder()
            .allocator(allocator)
            .producer(producer)
            .location(Location.forGrpcInsecure("localhost", 0))
            .build();
    server.start();

    AdbcDriver driver = new FlightSqlDriver(allocator);
    Map<String, Object> parameters = new HashMap<>();
    AdbcDriver.PARAM_URI.set(
        parameters, Location.forGrpcInsecure("localhost", server.getPort()).getUri().toString());
    database = driver.open(parameters);
  }

  @BeforeEach
  void beforeEach() throws Exception {
    producer.reset();
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
  void testStringListJsonRejectsNonStringValues() {
    TypedKey<String> key =
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX + "tags", String.class);

    String[] invalidValues =
        new String[] {"null", "[null]", "[1]", "[true]", "[1.5]", "[{}]", "[[]]"};
    for (String json : invalidValues) {
      AdbcException ex = assertThrows(AdbcException.class, () -> connection.setOption(key, json));
      assertThat(ex.getStatus()).as(json).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
    }
  }

  @Test
  void testStringListArrayRejectsNullElement() {
    TypedKey<String[]> key =
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX + "tags",
            String[].class);

    AdbcException ex =
        assertThrows(
            AdbcException.class, () -> connection.setOption(key, new String[] {"valid", null}));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void testUnsupportedGetterTypeDoesNotFetchSessionOptions() {
    producer.rejectGetSession.set(true);
    TypedKey<Integer> key =
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_BOOL_PREFIX + "flag", Integer.class);

    AdbcException ex = assertThrows(AdbcException.class, () -> connection.getOption(key));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
  }
}

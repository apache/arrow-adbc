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
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for the {@code flightsql://} URI scheme handled by {@link FlightSqlDriver}. */
class FlightSqlDriverUriTest {
  static BufferAllocator allocator;
  static FlightServer server;
  static FlightSqlDriver driver;

  @BeforeAll
  static void beforeAll() throws Exception {
    allocator = new RootAllocator();
    server =
        FlightServer.builder()
            .allocator(allocator)
            .producer(new NoOpFlightSqlProducer())
            .location(Location.forGrpcInsecure("localhost", 0))
            .build();
    server.start();
    driver = new FlightSqlDriver(allocator);
  }

  @AfterAll
  static void afterAll() throws Exception {
    AutoCloseables.close(server, allocator);
  }

  @Test
  void defaultTransportIsTls() throws Exception {
    Location location = FlightSqlDriver.parseLocation("flightsql://localhost:1234");
    assertThat(location.getUri().getScheme()).isEqualTo("grpc+tls");
    assertThat(location.getUri().getHost()).isEqualTo("localhost");
    assertThat(location.getUri().getPort()).isEqualTo(1234);
  }

  @Test
  void explicitTlsTransport() throws Exception {
    Location location = FlightSqlDriver.parseLocation("flightsql://localhost:1234?transport=tls");
    assertThat(location.getUri().getScheme()).isEqualTo("grpc+tls");
  }

  @Test
  void tcpTransportIsCaseInsensitive() throws Exception {
    Location location = FlightSqlDriver.parseLocation("flightsql://localhost:1234?transport=TCP");
    assertThat(location.getUri().getScheme()).isEqualTo("grpc+tcp");
    assertThat(location.getUri().getPort()).isEqualTo(1234);
  }

  @Test
  void unixTransport() throws Exception {
    Location location = FlightSqlDriver.parseLocation("flightsql:///tmp/adbc.sock?transport=unix");
    assertThat(location.getUri().getScheme()).isEqualTo("grpc+unix");
    assertThat(location.getUri().getPath()).isEqualTo("/tmp/adbc.sock");
  }

  @Test
  void schemeIsCaseInsensitive() throws Exception {
    Location location = FlightSqlDriver.parseLocation("FLIGHTSQL://localhost:1234");
    assertThat(location.getUri().getScheme()).isEqualTo("grpc+tls");
  }

  @Test
  void legacySchemesStillWork() throws Exception {
    Location location = FlightSqlDriver.parseLocation("grpc+tls://localhost:1234");
    assertThat(location.getUri().getScheme()).isEqualTo("grpc+tls");
  }

  @Test
  void connectsOverTcpTransport() throws Exception {
    Map<String, Object> parameters = new HashMap<>();
    AdbcDriver.PARAM_URI.set(
        parameters, "flightsql://localhost:" + server.getPort() + "?transport=tcp");

    try (AdbcDatabase database = driver.open(parameters);
        AdbcConnection connection = database.connect()) {
      assertThat(connection).isNotNull();
    }
  }

  @Test
  void rejectsUnrecognizedTransport() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () -> FlightSqlDriver.parseLocation("flightsql://localhost:1234?transport=bogus"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsHostWithUnixTransport() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () ->
                FlightSqlDriver.parseLocation(
                    "flightsql://localhost:1234/socket?transport=unix"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsPathWithTcpTransport() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () ->
                FlightSqlDriver.parseLocation("flightsql://localhost:1234/socket?transport=tcp"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsPathWithDefaultTransport() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () -> FlightSqlDriver.parseLocation("flightsql://localhost:1234/socket"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsUnixTransportWithoutPath() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () -> FlightSqlDriver.parseLocation("flightsql://?transport=unix"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsHostWithUnderscoreAsAdbcException() {
    // java.net.URI treats an underscore as invalid in a "server-based" authority and silently
    // parses the host/port as absent instead of throwing, so this must be caught explicitly
    // rather than letting Location.forGrpcTls's IllegalArgumentException escape uncaught.
    AdbcException ex =
        assertThrows(
            AdbcException.class, () -> FlightSqlDriver.parseLocation("flightsql://my_host:1234"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsMissingHostAsAdbcException() {
    AdbcException ex =
        assertThrows(AdbcException.class, () -> FlightSqlDriver.parseLocation("flightsql://:1234"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void transportKeyIsCaseInsensitive() throws Exception {
    Location location = FlightSqlDriver.parseLocation("flightsql://localhost:1234?TRANSPORT=tcp");
    assertThat(location.getUri().getScheme()).isEqualTo("grpc+tcp");
  }

  @Test
  void rejectsMalformedTransportPercentEncoding() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () -> FlightSqlDriver.parseLocation("flightsql://localhost:1234?transport=%zz"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsUserInfo() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () -> FlightSqlDriver.parseLocation("flightsql://alice:secret@localhost:1234"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

  @Test
  void rejectsFragment() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () -> FlightSqlDriver.parseLocation("flightsql://localhost:1234#frag"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.INVALID_ARGUMENT);
  }

}

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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HeaderTest {

  private FlightServer.Builder builder;
  private FlightServer server;
  private Map<String, Object> params;
  private AdbcConnection connection;
  private BufferAllocator allocator;
  private HeaderValidator.Factory headerValidatorFactory;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    headerValidatorFactory = new HeaderValidator.Factory();
    builder =
        FlightServer.builder()
            .middleware(HeaderValidator.KEY, headerValidatorFactory)
            .location(Location.forGrpcInsecure("localhost", 0))
            .producer(new MockFlightSqlProducer());
    params = new HashMap<>();
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(connection, server, allocator);
    connection = null;
    server = null;
    allocator = null;
  }

  @Test
  public void testArbitraryHeader() throws Exception {
    final String dummyValue = "dummy";
    final String dummyHeaderName = "test-header";
    params.put(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + dummyHeaderName, dummyValue);
    server = builder.build();
    server.start();
    connect();

    CallHeaders headers = headerValidatorFactory.getHeadersReceivedAtRequest(0);
    assertEquals(dummyValue, headers.get(dummyHeaderName));
  }

  @Test
  public void testCookies() throws Exception {
    builder.middleware(CookieMiddleware.KEY, new CookieMiddleware.Factory());
    server = builder.build();
    server.start();

    params.put(FlightSqlConnectionProperties.WITH_COOKIE_MIDDLEWARE.getKey(), true);
    connect();
    // Use GetInfo but with a type that requires connecting to the server.
    try (ArrowReader reader = connection.getInfo(new int[] {AdbcInfoCode.VENDOR_NAME.getValue()})) {
      while (reader.loadNextBatch()) {
        ; // Just iterate through the result.
      }
    } catch (Exception ex) {
      // Swallow exceptions from the RPC call. Only interested in tracking metadata.
    }
    CallHeaders secondHeaders = headerValidatorFactory.getHeadersReceivedAtRequest(1);
    assertTrue(secondHeaders.containsKey("cookie"));
  }

  @Test
  public void testBearerToken() throws Exception {
    builder.headerAuthenticator(
        new GeneratedBearerTokenAuthenticator(
            new BasicCallHeaderAuthenticator((username, password) -> () -> username)));
    server = builder.build();
    server.start();

    params.put(AdbcDriver.PARAM_USERNAME.getKey(), "dummy_user");
    params.put(AdbcDriver.PARAM_PASSWORD.getKey(), "dummy_password");
    connect();
    // Use GetInfo but with a type that requires connecting to the server.
    try (ArrowReader reader = connection.getInfo(new int[] {AdbcInfoCode.VENDOR_NAME.getValue()})) {
      while (reader.loadNextBatch()) {
        ; // Just iterate through the result.
      }
    } catch (Exception ex) {
      // Swallow exceptions from the RPC call. Only interested in tracking metadata.
      // This is expected to fail since GetSqlInfo isn't implemented on this test FlightSqlProducer.
    }
    CallHeaders secondHeaders = headerValidatorFactory.getHeadersReceivedAtRequest(1);
    assertTrue(secondHeaders.get("authorization").contains("Bearer"));
  }

  @Test
  public void testUnauthenticated() throws Exception {
    builder.headerAuthenticator(
        new GeneratedBearerTokenAuthenticator(
            new BasicCallHeaderAuthenticator((username, password) -> () -> username)));
    server = builder.build();
    server.start();

    AdbcException adbcException = assertThrows(AdbcException.class, this::connect);
    assertEquals(AdbcStatusCode.UNAUTHENTICATED, adbcException.getStatus());
  }

  static class CookieMiddleware implements FlightServerMiddleware {

    public static final Key<CookieMiddleware> KEY = Key.of("CookieMiddleware");

    @Override
    public void onBeforeSendingHeaders(CallHeaders callHeaders) {
      callHeaders.insert("set-cookie", "test=test_val");
    }

    @Override
    public void onCallCompleted(CallStatus callStatus) {}

    @Override
    public void onCallErrored(Throwable throwable) {}

    public static class Factory implements FlightServerMiddleware.Factory<CookieMiddleware> {

      @Override
      public CookieMiddleware onCallStarted(
          CallInfo callInfo, CallHeaders callHeaders, RequestContext requestContext) {
        return new CookieMiddleware();
      }
    }
  }

  private void connect() throws Exception {
    int port = server.getPort();
    String uri = String.format("grpc+tcp://%s:%d", "localhost", port);
    params.put(AdbcDriver.PARAM_URI.getKey(), uri);
    AdbcDatabase db =
        AdbcDriverManager.getInstance()
            .connect(FlightSqlDriverFactory.class.getCanonicalName(), allocator, params);
    connection = db.connect();
  }
}

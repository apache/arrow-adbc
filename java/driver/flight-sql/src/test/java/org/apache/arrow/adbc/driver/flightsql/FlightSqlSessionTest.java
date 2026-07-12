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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.GetSessionOptionsResult;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for Flight SQL session management (get/set/erase options, CloseSession). */
class FlightSqlSessionTest {
  static BufferAllocator allocator;
  static SessionProducer producer;
  static FlightServer server;
  static AdbcDriver driver;
  static AdbcDatabase database;
  AdbcConnection connection;

  @BeforeAll
  static void beforeAll() throws Exception {
    allocator = new RootAllocator();
    producer = new SessionProducer();
    server =
        FlightServer.builder()
            .allocator(allocator)
            .producer(producer)
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
  void testSetGetStringOption() throws Exception {
    connection.setOption(
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_PREFIX + "catalog", String.class),
        "my_catalog");

    String value =
        connection.getOption(
            new TypedKey<>(
                FlightSqlConnectionProperties.SESSION_OPTION_PREFIX + "catalog", String.class));
    assertThat(value).isEqualTo("my_catalog");
  }

  @Test
  void testSetGetBoolOption() throws Exception {
    connection.setOption(
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_BOOL_PREFIX + "flag", Boolean.class),
        true);

    Boolean value =
        connection.getOption(
            new TypedKey<>(
                FlightSqlConnectionProperties.SESSION_OPTION_BOOL_PREFIX + "flag", Boolean.class));
    assertThat(value).isTrue();
  }

  @Test
  void testSetGetStringListOptionAsArray() throws Exception {
    connection.setOption(
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX + "tags",
            String[].class),
        new String[] {"a", "b", "c"});

    String[] value =
        connection.getOption(
            new TypedKey<>(
                FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX + "tags",
                String[].class));
    assertThat(value).containsExactly("a", "b", "c");
  }

  @Test
  void testSetGetStringListOptionAsJson() throws Exception {
    connection.setOption(
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX + "tags", String.class),
        "[\"x\",\"y\"]");

    String[] value =
        connection.getOption(
            new TypedKey<>(
                FlightSqlConnectionProperties.SESSION_OPTION_STRING_LIST_PREFIX + "tags",
                String[].class));
    assertThat(value).containsExactly("x", "y");
  }

  @Test
  void testEraseOption() throws Exception {
    connection.setOption(
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_PREFIX + "toErase", String.class),
        "value");

    connection.setOption(
        new TypedKey<>(
            FlightSqlConnectionProperties.SESSION_OPTION_ERASE_PREFIX + "toErase", String.class),
        "");

    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () ->
                connection.getOption(
                    new TypedKey<>(
                        FlightSqlConnectionProperties.SESSION_OPTION_PREFIX + "toErase",
                        String.class)));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.NOT_FOUND);
  }

  @Test
  void testGetSessionOptionsBlob() throws Exception {
    connection.setOption(
        new TypedKey<>(FlightSqlConnectionProperties.SESSION_OPTION_PREFIX + "k1", String.class),
        "v1");

    String blob =
        connection.getOption(
            new TypedKey<>(FlightSqlConnectionProperties.SESSION_OPTIONS, String.class));
    assertThat(blob).contains("\"k1\"");
    assertThat(blob).contains("\"v1\"");
  }

  @Test
  void testCloseSessionCalledOnClose() throws Exception {
    producer.closeSessionCalled.set(false);
    connection.close();
    connection = null; // prevent double-close in afterEach
    assertThat(producer.closeSessionCalled.get()).isTrue();
  }

  @Test
  void testCloseDoesNotThrowWhenServerReturnsUnimplemented() throws Exception {
    producer.rejectClose.set(true);
    connection.close(); // must not throw
    connection = null;
  }

  @Test
  void testGetSessionOptionsBlobEmptyWhenServerUnsupported() throws Exception {
    producer.rejectGetSession.set(true);
    String blob =
        connection.getOption(
            new TypedKey<>(FlightSqlConnectionProperties.SESSION_OPTIONS, String.class));
    assertThat(blob).isEqualTo("{}");
  }

  @Test
  void testSetOptionReadOnlyBlobThrows() {
    AdbcException ex =
        assertThrows(
            AdbcException.class,
            () ->
                connection.setOption(
                    new TypedKey<>(FlightSqlConnectionProperties.SESSION_OPTIONS, String.class),
                    "{}"));
    assertThat(ex.getStatus()).isEqualTo(AdbcStatusCode.NOT_IMPLEMENTED);
  }

  // ----- Server-side producer -----

  static class SessionProducer implements FlightSqlProducer {
    private final Map<String, SessionOptionValue> sessionOptions = new HashMap<>();
    final AtomicBoolean closeSessionCalled = new AtomicBoolean(false);
    final AtomicBoolean rejectClose = new AtomicBoolean(false);
    final AtomicBoolean rejectGetSession = new AtomicBoolean(false);

    void reset() {
      sessionOptions.clear();
      closeSessionCalled.set(false);
      rejectClose.set(false);
      rejectGetSession.set(false);
    }

    @Override
    public void setSessionOptions(
        SetSessionOptionsRequest request,
        CallContext context,
        StreamListener<SetSessionOptionsResult> listener) {
      for (Map.Entry<String, SessionOptionValue> e : request.getSessionOptions().entrySet()) {
        if (e.getValue().isEmpty()) {
          sessionOptions.remove(e.getKey());
        } else {
          sessionOptions.put(e.getKey(), e.getValue());
        }
      }
      listener.onNext(new SetSessionOptionsResult(Collections.emptyMap()));
      listener.onCompleted();
    }

    @Override
    public void getSessionOptions(
        GetSessionOptionsRequest request,
        CallContext context,
        StreamListener<GetSessionOptionsResult> listener) {
      if (rejectGetSession.get()) {
        listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException());
        return;
      }
      listener.onNext(new GetSessionOptionsResult(new HashMap<>(sessionOptions)));
      listener.onCompleted();
    }

    @Override
    public void closeSession(
        CloseSessionRequest request,
        CallContext context,
        StreamListener<CloseSessionResult> listener) {
      if (rejectClose.get()) {
        listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException());
        return;
      }
      closeSessionCalled.set(true);
      listener.onNext(new CloseSessionResult(CloseSessionResult.Status.CLOSED));
      listener.onCompleted();
    }

    // Required abstract method implementations (no-op)

    @Override
    public void createPreparedStatement(
        FlightSql.ActionCreatePreparedStatementRequest req,
        CallContext ctx,
        StreamListener<Result> listener) {
      listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public void closePreparedStatement(
        FlightSql.ActionClosePreparedStatementRequest req,
        CallContext ctx,
        StreamListener<Result> listener) {
      listener.onError(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoStatement(
        FlightSql.CommandStatementQuery cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
        FlightSql.CommandPreparedStatementQuery cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public SchemaResult getSchemaStatement(
        FlightSql.CommandStatementQuery cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamStatement(
        FlightSql.TicketStatementQuery ticket, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public void getStreamPreparedStatement(
        FlightSql.CommandPreparedStatementQuery cmd,
        CallContext ctx,
        ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public Runnable acceptPutStatement(
        FlightSql.CommandStatementUpdate cmd,
        CallContext ctx,
        FlightStream stream,
        StreamListener<PutResult> listener) {
      return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(
        FlightSql.CommandPreparedStatementUpdate cmd,
        CallContext ctx,
        FlightStream stream,
        StreamListener<PutResult> listener) {
      return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(
        FlightSql.CommandPreparedStatementQuery cmd,
        CallContext ctx,
        FlightStream stream,
        StreamListener<PutResult> listener) {
      return null;
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(
        FlightSql.CommandGetSqlInfo cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamSqlInfo(
        FlightSql.CommandGetSqlInfo cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(
        FlightSql.CommandGetXdbcTypeInfo cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamTypeInfo(
        FlightSql.CommandGetXdbcTypeInfo cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(
        FlightSql.CommandGetCatalogs cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamCatalogs(CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoSchemas(
        FlightSql.CommandGetDbSchemas cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamSchemas(
        FlightSql.CommandGetDbSchemas cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoTables(
        FlightSql.CommandGetTables cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamTables(
        FlightSql.CommandGetTables cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(
        FlightSql.CommandGetTableTypes cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamTableTypes(CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(
        FlightSql.CommandGetPrimaryKeys cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamPrimaryKeys(
        FlightSql.CommandGetPrimaryKeys cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(
        FlightSql.CommandGetExportedKeys cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(
        FlightSql.CommandGetImportedKeys cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(
        FlightSql.CommandGetCrossReference cmd, CallContext ctx, FlightDescriptor descriptor) {
      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    @Override
    public void getStreamExportedKeys(
        FlightSql.CommandGetExportedKeys cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public void getStreamImportedKeys(
        FlightSql.CommandGetImportedKeys cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public void getStreamCrossReference(
        FlightSql.CommandGetCrossReference cmd, CallContext ctx, ServerStreamListener listener) {
      listener.error(CallStatus.UNIMPLEMENTED.toRuntimeException());
    }

    @Override
    public void listFlights(
        CallContext ctx, Criteria criteria, StreamListener<FlightInfo> listener) {}

    @Override
    public void close() {}
  }
}

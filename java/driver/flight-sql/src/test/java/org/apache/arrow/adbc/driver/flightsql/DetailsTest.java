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

import io.grpc.Metadata;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.ErrorDetail;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.ErrorFlightMetadata;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
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

/** Test that gRPC error details make it through. */
class DetailsTest {
  static BufferAllocator allocator;
  static Producer producer;
  static FlightServer server;
  static AdbcDriver driver;
  static AdbcDatabase database;
  AdbcConnection connection;
  AdbcStatement statement;

  @BeforeAll
  static void beforeAll() throws Exception {
    allocator = new RootAllocator();
    producer = new Producer();
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
    connection = database.connect();
    statement = connection.createStatement();
  }

  @AfterEach
  void afterEach() throws Exception {
    AutoCloseables.close(statement, connection);
  }

  @AfterAll
  static void afterAll() throws Exception {
    AutoCloseables.close(database, server, allocator);
  }

  @Test
  void flightDetails() throws Exception {
    statement.setSqlQuery("flight");

    AdbcException exception =
        assertThrows(
            AdbcException.class,
            () -> {
              try (AdbcStatement.QueryResult result = statement.executeQuery()) {}
            });

    assertThat(exception.getDetails()).contains(new ErrorDetail("x-foo", "text"));
    Optional<ErrorDetail> binaryKey =
        exception.getDetails().stream().filter(x -> x.getKey().equals("x-foo-bin")).findAny();
    assertThat(binaryKey)
        .get()
        .extracting(ErrorDetail::getValue)
        .isEqualTo("text".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void grpcDetails() throws Exception {
    statement.setSqlQuery("grpc");

    AdbcException exception =
        assertThrows(
            AdbcException.class,
            () -> {
              try (AdbcStatement.QueryResult result = statement.executeQuery()) {}
            });

    assertThat(exception.getDetails()).contains(new ErrorDetail("x-foo", "text"));
    Optional<ErrorDetail> binaryKey =
        exception.getDetails().stream().filter(x -> x.getKey().equals("x-foo-bin")).findAny();
    assertThat(binaryKey)
        .get()
        .extracting(ErrorDetail::getValue)
        .isEqualTo("text".getBytes(StandardCharsets.UTF_8));
  }

  static class Producer implements FlightSqlProducer {
    Metadata.Key<byte[]> BINARY_KEY = Metadata.Key.of("x-foo-bin", Metadata.BINARY_BYTE_MARSHALLER);
    Metadata.Key<String> TEXT_KEY = Metadata.Key.of("x-foo", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public FlightInfo getFlightInfoStatement(
        FlightSql.CommandStatementQuery commandStatementQuery,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      if (commandStatementQuery.getQuery().equals("flight")) {
        // Using Flight path
        ErrorFlightMetadata metadata = new ErrorFlightMetadata();
        metadata.insert("x-foo", "text");
        metadata.insert("x-foo-bin", "text".getBytes(StandardCharsets.UTF_8));
        throw CallStatus.UNKNOWN
            .withDescription("Expected")
            .withMetadata(metadata)
            .toRuntimeException();
      } else if (commandStatementQuery.getQuery().equals("grpc")) {
        // Using gRPC path
        Metadata trailers = new Metadata();
        trailers.put(TEXT_KEY, "text");
        trailers.put(BINARY_KEY, "text".getBytes(StandardCharsets.UTF_8));
        throw Status.UNKNOWN.asRuntimeException(trailers);
      }

      throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    // No-op implementations

    @Override
    public void createPreparedStatement(
        FlightSql.ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
        CallContext callContext,
        StreamListener<Result> streamListener) {}

    @Override
    public void closePreparedStatement(
        FlightSql.ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
        CallContext callContext,
        StreamListener<Result> streamListener) {}

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
        FlightSql.CommandPreparedStatementQuery commandPreparedStatementQuery,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public SchemaResult getSchemaStatement(
        FlightSql.CommandStatementQuery commandStatementQuery,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamStatement(
        FlightSql.TicketStatementQuery ticketStatementQuery,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public void getStreamPreparedStatement(
        FlightSql.CommandPreparedStatementQuery commandPreparedStatementQuery,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public Runnable acceptPutStatement(
        FlightSql.CommandStatementUpdate commandStatementUpdate,
        CallContext callContext,
        FlightStream flightStream,
        StreamListener<PutResult> streamListener) {
      return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(
        FlightSql.CommandPreparedStatementUpdate commandPreparedStatementUpdate,
        CallContext callContext,
        FlightStream flightStream,
        StreamListener<PutResult> streamListener) {
      return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(
        FlightSql.CommandPreparedStatementQuery commandPreparedStatementQuery,
        CallContext callContext,
        FlightStream flightStream,
        StreamListener<PutResult> streamListener) {
      return null;
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(
        FlightSql.CommandGetSqlInfo commandGetSqlInfo,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamSqlInfo(
        FlightSql.CommandGetSqlInfo commandGetSqlInfo,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public FlightInfo getFlightInfoTypeInfo(
        FlightSql.CommandGetXdbcTypeInfo commandGetXdbcTypeInfo,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamTypeInfo(
        FlightSql.CommandGetXdbcTypeInfo commandGetXdbcTypeInfo,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public FlightInfo getFlightInfoCatalogs(
        FlightSql.CommandGetCatalogs commandGetCatalogs,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamCatalogs(
        CallContext callContext, ServerStreamListener serverStreamListener) {}

    @Override
    public FlightInfo getFlightInfoSchemas(
        FlightSql.CommandGetDbSchemas commandGetDbSchemas,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamSchemas(
        FlightSql.CommandGetDbSchemas commandGetDbSchemas,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public FlightInfo getFlightInfoTables(
        FlightSql.CommandGetTables commandGetTables,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamTables(
        FlightSql.CommandGetTables commandGetTables,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public FlightInfo getFlightInfoTableTypes(
        FlightSql.CommandGetTableTypes commandGetTableTypes,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamTableTypes(
        CallContext callContext, ServerStreamListener serverStreamListener) {}

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(
        FlightSql.CommandGetPrimaryKeys commandGetPrimaryKeys,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamPrimaryKeys(
        FlightSql.CommandGetPrimaryKeys commandGetPrimaryKeys,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public FlightInfo getFlightInfoExportedKeys(
        FlightSql.CommandGetExportedKeys commandGetExportedKeys,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(
        FlightSql.CommandGetImportedKeys commandGetImportedKeys,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(
        FlightSql.CommandGetCrossReference commandGetCrossReference,
        CallContext callContext,
        FlightDescriptor flightDescriptor) {
      return null;
    }

    @Override
    public void getStreamExportedKeys(
        FlightSql.CommandGetExportedKeys commandGetExportedKeys,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public void getStreamImportedKeys(
        FlightSql.CommandGetImportedKeys commandGetImportedKeys,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public void getStreamCrossReference(
        FlightSql.CommandGetCrossReference commandGetCrossReference,
        CallContext callContext,
        ServerStreamListener serverStreamListener) {}

    @Override
    public void close() throws Exception {}

    @Override
    public void listFlights(
        CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {}
  }
}

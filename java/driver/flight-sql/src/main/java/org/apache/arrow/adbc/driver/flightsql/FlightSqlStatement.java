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

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

public class FlightSqlStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final FlightSqlClient client;
  private final LoadingCache<Location, FlightClient> clientCache;
  private final SqlQuirks quirks;

  // State for SQL queries
  private String sqlQuery;
  private FlightSqlClient.PreparedStatement preparedStatement;
  // State for bulk ingest
  private BulkState bulkOperation;
  private VectorSchemaRoot bindRoot;

  FlightSqlStatement(
      BufferAllocator allocator,
      FlightSqlClient client,
      LoadingCache<Location, FlightClient> clientCache,
      SqlQuirks quirks) {
    this.allocator = allocator;
    this.client = client;
    this.clientCache = clientCache;
    this.quirks = quirks;
    this.sqlQuery = null;
  }

  static FlightSqlStatement ingestRoot(
      BufferAllocator allocator,
      FlightSqlClient client,
      LoadingCache<Location, FlightClient> clientCache,
      SqlQuirks quirks,
      String targetTableName,
      BulkIngestMode mode) {
    Objects.requireNonNull(targetTableName);
    final FlightSqlStatement statement =
        new FlightSqlStatement(allocator, client, clientCache, quirks);
    statement.bulkOperation = new BulkState();
    statement.bulkOperation.mode = mode;
    statement.bulkOperation.targetTable = targetTableName;
    return statement;
  }

  @Override
  public void setSqlQuery(String query) throws AdbcException {
    if (bulkOperation != null) {
      throw AdbcException.invalidState(
          "[Flight SQL] Statement is configured for a bulk ingest/append operation");
    }
    sqlQuery = query;
  }

  @Override
  public void bind(VectorSchemaRoot root) {
    bindRoot = root;
  }

  private void createBulkTable() throws AdbcException {
    final StringBuilder create = new StringBuilder("CREATE TABLE ");
    create.append(bulkOperation.targetTable);
    create.append(" (");
    for (int col = 0; col < bindRoot.getFieldVectors().size(); col++) {
      if (col > 0) {
        create.append(", ");
      }
      final Field field = bindRoot.getVector(col).getField();
      create.append(field.getName());
      create.append(' ');
      String typeName = quirks.getArrowToSqlTypeNameMapping().apply(field.getType());
      if (typeName == null) {
        throw AdbcException.notImplemented(
            "[Flight SQL] Cannot generate CREATE TABLE statement for field " + field);
      }
      create.append(typeName);
    }
    create.append(")");

    try {
      client.executeUpdate(create.toString());
    } catch (FlightRuntimeException e) {
      throw new AdbcException(
          "[Flight SQL] Could not create table for bulk ingestion: " + bulkOperation.targetTable,
          e,
          AdbcStatusCode.ALREADY_EXISTS,
          null,
          0);
    }
  }

  private UpdateResult executeBulk() throws AdbcException {
    if (bindRoot == null) {
      throw AdbcException.invalidState("[Flight SQL] Must call bind() before bulk insert");
    }

    if (bulkOperation.mode == BulkIngestMode.CREATE) {
      createBulkTable();
    }

    // XXX: potential injection
    final StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(bulkOperation.targetTable);
    insert.append(" VALUES (");
    for (int col = 0; col < bindRoot.getFieldVectors().size(); col++) {
      if (col > 0) {
        insert.append(", ");
      }
      insert.append("?");
    }
    insert.append(")");

    final FlightSqlClient.PreparedStatement statement;
    try {
      statement = client.prepare(insert.toString());
    } catch (FlightRuntimeException e) {
      throw new AdbcException(
          "[Flight SQL] Could not prepare statement for bulk ingestion into "
              + bulkOperation.targetTable,
          e,
          AdbcStatusCode.NOT_FOUND,
          null,
          0);
    }
    try {
      try {
        statement.setParameters(new NonOwningRoot(bindRoot));
        statement.executeUpdate();
      } finally {
        statement.close();
      }
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    }
    return new UpdateResult(bindRoot.getRowCount());
  }

  @FunctionalInterface
  interface Execute<T, R> {
    R execute(T t) throws AdbcException;
  }

  private <R> R execute(
      Execute<FlightSqlClient.PreparedStatement, R> doPrepared,
      Execute<FlightSqlClient, R> doRegular)
      throws AdbcException {
    try {
      if (preparedStatement != null) {
        // TODO: This binds only the LAST row
        // See https://lists.apache.org/thread/47zfk3xooojckvfjq2h6ldlqkjrqnsjt
        // "[DISC] Flight SQL: clarifying prepared statements with parameters and result sets"
        if (bindRoot != null) {
          preparedStatement.setParameters(new NonOwningRoot(bindRoot));
        }
        return doPrepared.execute(preparedStatement);
      } else {
        return doRegular.execute(client);
      }
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    }
  }

  private FlightInfo executeFlightInfo() throws AdbcException {
    if (bulkOperation != null) {
      throw AdbcException.invalidState("[Flight SQL] Must executeUpdate() for bulk ingestion");
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[Flight SQL] Must setSqlQuery() before execute");
    }
    return execute(
        FlightSqlClient.PreparedStatement::execute, (client) -> client.execute(sqlQuery));
  }

  @Override
  public PartitionResult executePartitioned() throws AdbcException {
    final FlightInfo info = executeFlightInfo();

    final List<PartitionDescriptor> descriptors = new ArrayList<>();
    for (final FlightEndpoint endpoint : info.getEndpoints()) {
      // FlightEndpoint doesn't expose its serializer, so do it manually
      Flight.FlightEndpoint.Builder protoEndpoint =
          Flight.FlightEndpoint.newBuilder()
              .setTicket(
                  Flight.Ticket.newBuilder()
                      .setTicket(ByteString.copyFrom(endpoint.getTicket().getBytes())));
      for (final Location location : endpoint.getLocations()) {
        protoEndpoint.addLocation(
            Flight.Location.newBuilder().setUri(location.getUri().toString()).build());
      }
      descriptors.add(
          new PartitionDescriptor(protoEndpoint.build().toByteString().asReadOnlyByteBuffer()));
    }
    return new PartitionResult(info.getSchema(), info.getRecords(), descriptors);
  }

  @Override
  public QueryResult executeQuery() throws AdbcException {
    final FlightInfo info = executeFlightInfo();
    return new QueryResult(
        info.getRecords(),
        new FlightInfoReader(allocator, client, clientCache, info.getEndpoints()));
  }

  @Override
  public UpdateResult executeUpdate() throws AdbcException {
    if (bulkOperation != null) {
      return executeBulk();
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[Flight SQL] Must setSqlQuery() before executeUpdate");
    }
    long updatedRows =
        execute(
            (preparedStatement) -> {
              // XXX(ARROW-17199): why does this throw SQLException?
              try {
                return preparedStatement.executeUpdate();
              } catch (FlightRuntimeException e) {
                throw FlightSqlDriverUtil.fromFlightException(e);
              }
            },
            (client) -> client.executeUpdate(sqlQuery));
    return new UpdateResult(updatedRows);
  }

  @Override
  public Schema getParameterSchema() throws AdbcException {
    if (preparedStatement == null) {
      throw AdbcException.invalidState(
          "[Flight SQL] Must call prepare() before getParameterSchema()");
    }
    return preparedStatement.getParameterSchema();
  }

  @Override
  public void prepare() throws AdbcException {
    try {
      if (sqlQuery == null) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] Must call setSqlQuery(String) before prepare()");
      }
      preparedStatement = client.prepare(sqlQuery);
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(preparedStatement);
  }

  private static final class BulkState {
    public BulkIngestMode mode;
    String targetTable;
  }

  /** A VectorSchemaRoot which does not own its data. */
  private static final class NonOwningRoot extends VectorSchemaRoot {
    public NonOwningRoot(VectorSchemaRoot parent) {
      super(parent.getSchema(), parent.getFieldVectors(), parent.getRowCount());
    }

    @Override
    public void close() {}
  }
}

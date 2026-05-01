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
import java.util.concurrent.ExecutionException;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.arrow.adbc.sql.SqlQuirks;
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
import org.checkerframework.checker.nullness.qual.Nullable;

public class FlightSqlStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final FlightSqlClientWithCallOptions client;
  private final LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache;
  private final SqlQuirks quirks;

  // State for SQL queries
  private @Nullable String sqlQuery;
  private FlightSqlClient.@Nullable PreparedStatement preparedStatement;
  // State for bulk ingest
  private @Nullable BulkState bulkOperation;
  private @Nullable VectorSchemaRoot bindRoot;

  FlightSqlStatement(
      BufferAllocator allocator,
      FlightSqlClientWithCallOptions client,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
      SqlQuirks quirks) {
    this.allocator = allocator;
    this.client = client;
    this.clientCache = clientCache;
    this.quirks = quirks;
    this.sqlQuery = null;
    this.preparedStatement = null;
    this.bulkOperation = null;
    this.bindRoot = null;
  }

  static FlightSqlStatement ingestRoot(
      BufferAllocator allocator,
      FlightSqlClientWithCallOptions client,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
      SqlQuirks quirks,
      String targetTableName,
      BulkIngestMode mode) {
    Objects.requireNonNull(targetTableName);
    final FlightSqlStatement statement =
        new FlightSqlStatement(allocator, client, clientCache, quirks);
    statement.bulkOperation = new BulkState(mode, targetTableName);
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

  private void createBulkTable(BulkState bulkOperation, VectorSchemaRoot bindRoot)
      throws AdbcException {
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

  private UpdateResult executeBulk(BulkState bulkOperation) throws AdbcException {
    if (bindRoot == null) {
      throw AdbcException.invalidState("[Flight SQL] Must call bind() before bulk insert");
    }
    final VectorSchemaRoot bindParams = bindRoot;

    if (bulkOperation.mode == BulkIngestMode.CREATE) {
      createBulkTable(bulkOperation, bindParams);
    }

    // XXX: potential injection
    final StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(bulkOperation.targetTable);
    insert.append(" VALUES (");
    for (int col = 0; col < bindParams.getFieldVectors().size(); col++) {
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
        statement.setParameters(new NonOwningRoot(bindParams));
        client.executePreparedUpdate(statement);
      } finally {
        statement.close();
      }
    } catch (FlightRuntimeException e) {
      // XXX: FlightSqlClient.executeUpdate does some extra wrapping that we need to undo
      if (e.getCause() instanceof ExecutionException
          && e.getCause().getCause() instanceof FlightRuntimeException) {
        throw FlightSqlDriverUtil.fromFlightException(
            (FlightRuntimeException) e.getCause().getCause());
      }
      throw FlightSqlDriverUtil.fromFlightException(e);
    }
    return new UpdateResult(bindParams.getRowCount());
  }

  @FunctionalInterface
  interface Execute<T, R> {
    R execute(T t) throws AdbcException;
  }

  private <R> R execute(
      Execute<FlightSqlClient.PreparedStatement, R> doPrepared,
      Execute<FlightSqlClientWithCallOptions, R> doRegular)
      throws AdbcException {
    try {
      if (preparedStatement != null) {
        FlightSqlClient.PreparedStatement prepared = preparedStatement;
        // TODO: This binds only the LAST row
        // See https://lists.apache.org/thread/47zfk3xooojckvfjq2h6ldlqkjrqnsjt
        // "[DISC] Flight SQL: clarifying prepared statements with parameters and result sets"
        if (bindRoot != null) {
          prepared.setParameters(new NonOwningRoot(bindRoot));
        }
        return doPrepared.execute(prepared);
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
    final String query = sqlQuery;
    return execute(client::executePrepared, (client) -> client.execute(query));
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
  public Schema executeSchema() throws AdbcException {
    if (bulkOperation != null) {
      throw AdbcException.invalidState("[Flight SQL] Must executeUpdate() for bulk ingestion");
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[Flight SQL] Must setSqlQuery() before execute");
    }
    final String query = sqlQuery;
    return execute(
        FlightSqlClient.PreparedStatement::getResultSetSchema,
        (client) -> client.getExecuteSchema(query).getSchema());
  }

  @Override
  public UpdateResult executeUpdate() throws AdbcException {
    if (bulkOperation != null) {
      return executeBulk(bulkOperation);
    } else if (sqlQuery == null) {
      throw AdbcException.invalidState("[Flight SQL] Must setSqlQuery() before executeUpdate");
    }
    final String query = sqlQuery;
    long updatedRows =
        execute(
            (preparedStatement) -> {
              // XXX(ARROW-17199): why does this throw SQLException?
              try {
                return client.executePreparedUpdate(preparedStatement);
              } catch (FlightRuntimeException e) {
                throw FlightSqlDriverUtil.fromFlightException(e);
              }
            },
            (client) -> client.executeUpdate(query));
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
  public void close() throws AdbcException {
    // TODO(https://github.com/apache/arrow/issues/39814): this is annotated wrongly upstream
    if (preparedStatement != null) {
      preparedStatement.close();
    }
  }

  private static final class BulkState {
    BulkIngestMode mode;
    String targetTable;

    public BulkState(BulkIngestMode mode, String targetTableName) {
      this.mode = mode;
      this.targetTable = targetTableName;
    }
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

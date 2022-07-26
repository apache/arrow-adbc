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
import java.io.IOException;
import java.sql.SQLException;
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
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;

public class FlightSqlStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final FlightSqlClient client;
  private final LoadingCache<Location, FlightClient> clientCache;
  private final SqlQuirks quirks;

  // State for SQL queries
  private String sqlQuery;
  private FlightSqlClient.PreparedStatement preparedStatement;
  private List<FlightEndpoint> flightEndpoints;
  private ArrowReader reader;
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

  public static AdbcStatement fromDescriptor(
      BufferAllocator allocator,
      FlightSqlClient client,
      LoadingCache<Location, FlightClient> clientCache,
      SqlQuirks quirks,
      List<FlightEndpoint> flightEndpoints) {
    final FlightSqlStatement statement =
        new FlightSqlStatement(allocator, client, clientCache, quirks);
    statement.flightEndpoints = flightEndpoints;
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

  @Override
  public void execute() throws AdbcException {
    if (bulkOperation != null) {
      executeBulk();
    } else if (sqlQuery != null) {
      executeSqlQuery();
    } else {
      throw AdbcException.invalidState("[Flight SQL] Must setSqlQuery() first");
    }
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

  private void executeBulk() throws AdbcException {
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
  }

  private void executeSqlQuery() throws AdbcException {
    try {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          throw new AdbcException(
              "[Flight SQL] Failed to close unread result set",
              e,
              AdbcStatusCode.IO,
              null, /*vendorCode*/
              0);
        }
      }

      if (preparedStatement != null) {
        // TODO: This binds only the LAST row
        if (bindRoot != null) {
          preparedStatement.setParameters(new NonOwningRoot(bindRoot));
        }
        // XXX(ARROW-17199): why does this throw SQLException?
        flightEndpoints = preparedStatement.execute().getEndpoints();
      } else {
        flightEndpoints = client.execute(sqlQuery).getEndpoints();
      }
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    } catch (SQLException e) {
      throw FlightSqlDriverUtil.fromSqlException(e);
    }
  }

  @Override
  public ArrowReader getArrowReader() throws AdbcException {
    if (reader != null) {
      ArrowReader result = reader;
      reader = null;
      return result;
    }
    if (flightEndpoints == null) {
      throw AdbcException.invalidState("[Flight SQL] Must call execute() before getArrowReader()");
    }
    final ArrowReader reader =
        new FlightInfoReader(allocator, client, clientCache, flightEndpoints);
    flightEndpoints = null;
    return reader;
  }

  @Override
  public List<PartitionDescriptor> getPartitionDescriptors() throws AdbcException {
    if (flightEndpoints == null) {
      throw AdbcException.invalidState(
          "[Flight SQL] Must call execute() before getPartitionDescriptors()");
    }
    final List<PartitionDescriptor> result = new ArrayList<>();
    for (final FlightEndpoint endpoint : flightEndpoints) {
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
      result.add(
          new PartitionDescriptor(protoEndpoint.build().toByteString().asReadOnlyByteBuffer()));
    }
    return result;
  }

  @Override
  public void prepare() throws AdbcException {
    try {
      if (sqlQuery == null) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] Must call setSqlQuery(String) before prepare()");
      }
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          throw new AdbcException(
              "[Flight SQL] Failed to close unread result set",
              e,
              AdbcStatusCode.IO,
              null, /*vendorCode*/
              0);
        }
      }

      preparedStatement = client.prepare(sqlQuery);
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(reader, preparedStatement);
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

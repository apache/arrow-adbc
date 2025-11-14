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

import static org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import static org.apache.arrow.flight.sql.FlightSqlClient.Savepoint;
import static org.apache.arrow.flight.sql.FlightSqlClient.SubstraitPlan;
import static org.apache.arrow.flight.sql.FlightSqlClient.Transaction;

import java.util.List;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.RenewFlightEndpointRequest;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.CancelResult;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.util.AutoCloseables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A wrapper around FlightSqlClient which automatically adds CallOptions to each RPC call. */
public class FlightSqlClientWithCallOptions implements AutoCloseable {
  private final FlightSqlClient client;
  private final CallOption[] connectionOptions;

  public FlightSqlClientWithCallOptions(FlightSqlClient client, CallOption... options) {
    this.client = client;
    this.connectionOptions = options;
  }

  public FlightInfo execute(String query, CallOption... options) {
    return client.execute(query, combine(options));
  }

  public FlightInfo execute(String query, Transaction transaction, CallOption... options) {
    return client.execute(query, transaction, combine(options));
  }

  public FlightInfo executePrepared(PreparedStatement statement, CallOption... options) {
    return statement.execute(combine(options));
  }

  public FlightInfo executeSubstrait(SubstraitPlan plan, CallOption... options) {
    return client.executeSubstrait(plan, combine(options));
  }

  public FlightInfo executeSubstrait(
      SubstraitPlan plan, Transaction transaction, CallOption... options) {
    return client.executeSubstrait(plan, transaction, combine(options));
  }

  public SchemaResult getExecuteSchema(
      String query, Transaction transaction, CallOption... options) {
    return client.getExecuteSchema(query, transaction, combine(options));
  }

  public SchemaResult getExecuteSchema(String query, CallOption... options) {
    return client.getExecuteSchema(query, combine(options));
  }

  public SchemaResult getExecuteSubstraitSchema(
      SubstraitPlan plan, Transaction transaction, CallOption... options) {
    return client.getExecuteSubstraitSchema(plan, transaction, combine(options));
  }

  public SchemaResult getExecuteSubstraitSchema(
      SubstraitPlan substraitPlan, CallOption... options) {
    return client.getExecuteSubstraitSchema(substraitPlan, combine(options));
  }

  public long executeUpdate(String query, CallOption... options) {
    return client.executeUpdate(query, combine(options));
  }

  public long executeUpdate(String query, Transaction transaction, CallOption... options) {
    return client.executeUpdate(query, transaction, combine(options));
  }

  public long executePreparedUpdate(PreparedStatement statement, CallOption... options) {
    return statement.executeUpdate(combine(options));
  }

  public long executeSubstraitUpdate(SubstraitPlan plan, CallOption... options) {
    return client.executeSubstraitUpdate(plan, combine(options));
  }

  public long executeSubstraitUpdate(
      SubstraitPlan plan, Transaction transaction, CallOption... options) {
    return client.executeSubstraitUpdate(plan, transaction, combine(options));
  }

  public FlightInfo getCatalogs(CallOption... options) {
    return client.getCatalogs(combine(options));
  }

  public SchemaResult getCatalogsSchema(CallOption... options) {
    return client.getCatalogsSchema(combine(options));
  }

  public FlightInfo getSchemas(
      String catalog, String dbSchemaFilterPattern, CallOption... options) {
    return client.getSchemas(catalog, dbSchemaFilterPattern, combine(options));
  }

  public SchemaResult getSchemasSchema(CallOption... options) {
    return client.getSchemasSchema(combine(options));
  }

  public SchemaResult getSchema(FlightDescriptor descriptor, CallOption... options) {
    return client.getSchema(descriptor, combine(options));
  }

  public FlightStream getStream(Ticket ticket, CallOption... options) {
    return client.getStream(ticket, combine(options));
  }

  public FlightInfo getSqlInfo(FlightSql.SqlInfo... info) {
    return client.getSqlInfo(info, combine());
  }

  public FlightInfo getSqlInfo(FlightSql.SqlInfo[] info, CallOption... options) {
    return client.getSqlInfo(info, combine(options));
  }

  public FlightInfo getSqlInfo(int[] info, CallOption... options) {
    return client.getSqlInfo(info, combine(options));
  }

  public FlightInfo getSqlInfo(Iterable<Integer> info, CallOption... options) {
    return client.getSqlInfo(info, combine(options));
  }

  public SchemaResult getSqlInfoSchema(CallOption... options) {
    return client.getSqlInfoSchema(combine(options));
  }

  public FlightInfo getXdbcTypeInfo(int dataType, CallOption... options) {
    return client.getXdbcTypeInfo(dataType, combine(options));
  }

  public FlightInfo getXdbcTypeInfo(CallOption... options) {
    return client.getXdbcTypeInfo(combine(options));
  }

  public SchemaResult getXdbcTypeInfoSchema(CallOption... options) {
    return client.getXdbcTypeInfoSchema(combine(options));
  }

  @SuppressWarnings("argument")
  // FlightSqlClient.getTableTyoes() allows for null tableTypes but isn't annotated correctly.
  public FlightInfo getTables(
      String catalog,
      String dbSchemaFilterPattern,
      String tableFilterPattern,
      @Nullable List<String> tableTypes,
      boolean includeSchema,
      CallOption... options) {
    return client.getTables(
        catalog,
        dbSchemaFilterPattern,
        tableFilterPattern,
        tableTypes,
        includeSchema,
        combine(options));
  }

  public SchemaResult getTablesSchema(boolean includeSchema, CallOption... options) {
    return client.getTablesSchema(includeSchema, combine(options));
  }

  public FlightInfo getPrimaryKeys(TableRef tableRef, CallOption... options) {
    return client.getPrimaryKeys(tableRef, combine(options));
  }

  public SchemaResult getPrimaryKeysSchema(CallOption... options) {
    return client.getPrimaryKeysSchema(combine(options));
  }

  public FlightInfo getExportedKeys(TableRef tableRef, CallOption... options) {
    return client.getExportedKeys(tableRef, combine(options));
  }

  public SchemaResult getExportedKeysSchema(CallOption... options) {
    return client.getExportedKeysSchema(combine(options));
  }

  public FlightInfo getImportedKeys(TableRef tableRef, CallOption... options) {
    return client.getImportedKeys(tableRef, combine(options));
  }

  public SchemaResult getImportedKeysSchema(CallOption... options) {
    return client.getImportedKeysSchema(combine(options));
  }

  public FlightInfo getCrossReference(
      TableRef pkTableRef, TableRef fkTableRef, CallOption... options) {
    return client.getCrossReference(pkTableRef, fkTableRef, combine(options));
  }

  public SchemaResult getCrossReferenceSchema(CallOption... options) {
    return client.getCrossReferenceSchema(combine(options));
  }

  public FlightInfo getTableTypes(CallOption... options) {
    return client.getTableTypes(combine(options));
  }

  public SchemaResult getTableTypesSchema(CallOption... options) {
    return client.getTableTypesSchema(combine(options));
  }

  public PreparedStatement prepare(String query, CallOption... options) {
    return client.prepare(query, combine(options));
  }

  public PreparedStatement prepare(
      String query, FlightSqlClient.Transaction transaction, CallOption... options) {
    return client.prepare(query, transaction, combine(options));
  }

  public PreparedStatement prepare(SubstraitPlan plan, CallOption... options) {
    return client.prepare(plan, combine(options));
  }

  public PreparedStatement prepare(
      SubstraitPlan plan, Transaction transaction, CallOption... options) {
    return client.prepare(plan, transaction, combine(options));
  }

  public Transaction beginTransaction(CallOption... options) {
    return client.beginTransaction(combine(options));
  }

  public Savepoint beginSavepoint(Transaction transaction, String name, CallOption... options) {
    return client.beginSavepoint(transaction, name, combine(options));
  }

  public void commit(Transaction transaction, CallOption... options) {
    client.commit(transaction, combine(options));
  }

  public void release(Savepoint savepoint, CallOption... options) {
    client.release(savepoint, combine(options));
  }

  public void rollback(Transaction transaction, CallOption... options) {
    client.rollback(transaction, combine(options));
  }

  public void rollback(Savepoint savepoint, CallOption... options) {
    client.rollback(savepoint, combine(options));
  }

  public CancelFlightInfoResult cancelFlightInfo(
      CancelFlightInfoRequest request, CallOption... options) {
    return client.cancelFlightInfo(request, combine(options));
  }

  public CancelResult cancelQuery(FlightInfo info, CallOption... options) {
    return client.cancelQuery(info, combine(options));
  }

  public FlightEndpoint renewFlightEndpoint(
      RenewFlightEndpointRequest request, CallOption... options) {
    return client.renewFlightEndpoint(request, combine(options));
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(client);
  }

  private CallOption[] combine(CallOption... options) {
    final CallOption[] result = new CallOption[connectionOptions.length + options.length];
    System.arraycopy(connectionOptions, 0, result, 0, connectionOptions.length);
    System.arraycopy(options, 0, result, connectionOptions.length, options.length);
    return result;
  }
}

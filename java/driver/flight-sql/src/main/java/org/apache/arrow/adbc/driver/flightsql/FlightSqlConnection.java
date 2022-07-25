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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FlightSqlConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final FlightSqlClient client;
  private final SqlQuirks quirks;

  FlightSqlConnection(BufferAllocator allocator, FlightClient client, SqlQuirks quirks) {
    this.allocator = allocator;
    this.client = new FlightSqlClient(client);
    this.quirks = quirks;
  }

  @Override
  public void commit() throws AdbcException {
    throw AdbcException.notImplemented("[Flight SQL] Transaction methods are not supported");
  }

  @Override
  public AdbcStatement createStatement() throws AdbcException {
    return new FlightSqlStatement(allocator, client, quirks);
  }

  @Override
  public AdbcStatement bulkIngest(String targetTableName, BulkIngestMode mode)
      throws AdbcException {
    return FlightSqlStatement.ingestRoot(allocator, client, quirks, targetTableName, mode);
  }

  @Override
  public AdbcStatement getInfo(int[] infoCodes) throws AdbcException {
    final VectorSchemaRoot root = new InfoMetadataBuilder(allocator, client, infoCodes).build();
    return new FixedRootStatement(allocator, root);
  }

  @Override
  public void rollback() throws AdbcException {
    throw AdbcException.notImplemented("[Flight SQL] Transaction methods are not supported");
  }

  @Override
  public boolean getAutoCommit() throws AdbcException {
    return true;
  }

  @Override
  public void setAutoCommit(boolean enableAutoCommit) throws AdbcException {
    if (!enableAutoCommit) {
      throw AdbcException.notImplemented("[Flight SQL] Transaction methods are not supported");
    }
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  public String toString() {
    return "FlightSqlConnection{" + "client=" + client + '}';
  }
}

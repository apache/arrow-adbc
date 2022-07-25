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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;

/** An instance of a database (e.g. a handle to an in-memory database). */
public final class FlightSqlDatabase implements AdbcDatabase {
  private final BufferAllocator allocator;
  private final Location location;
  private final SqlQuirks quirks;
  private final FlightClient client;
  private final AtomicInteger counter;

  FlightSqlDatabase(BufferAllocator allocator, Location location, SqlQuirks quirks)
      throws AdbcException {
    this.allocator = allocator;
    this.location = location;
    this.quirks = quirks;
    try {
      this.client = FlightClient.builder(allocator, location).build();
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    }
    this.counter = new AtomicInteger();
  }

  @Override
  public AdbcConnection connect() throws AdbcException {
    final FlightClient client;
    try {
      client = FlightClient.builder(allocator, location).build();
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    }
    final int count = counter.getAndIncrement();
    return new FlightSqlConnection(
        allocator.newChildAllocator("adbc-jdbc-connection-" + count, 0, allocator.getLimit()),
        client,
        quirks);
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  public String toString() {
    return "FlightSqlDatabase{" + "target='" + location + '\'' + '}';
  }
}

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
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/** An ArrowReader that wraps a FlightInfo. */
public class FlightInfoReader extends ArrowReader {
  private final Schema schema;
  private final FlightSqlClient client;
  private final LoadingCache<Location, FlightClient> clientCache;
  private final List<FlightEndpoint> flightEndpoints;
  private int nextEndpointIndex;
  private FlightStream currentStream;
  private long bytesRead;

  FlightInfoReader(
      BufferAllocator allocator,
      FlightSqlClient client,
      LoadingCache<Location, FlightClient> clientCache,
      List<FlightEndpoint> flightEndpoints)
      throws AdbcException {
    super(allocator);
    this.client = client;
    this.clientCache = clientCache;
    this.flightEndpoints = flightEndpoints;
    this.nextEndpointIndex = 0;
    this.bytesRead = 0;

    try {
      this.currentStream =
          client.getStream(flightEndpoints.get(this.nextEndpointIndex++).getTicket());
      this.schema = this.currentStream.getSchema();
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    }

    try {
      this.ensureInitialized();
    } catch (IOException e) {
      throw new AdbcException(
          FlightSqlDriverUtil.prefixExceptionMessage(e.getMessage()),
          e,
          AdbcStatusCode.IO,
          null,
          0);
    }
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (!currentStream.next()) {
      if (nextEndpointIndex >= flightEndpoints.size()) {
        return false;
      } else {
        try {
          currentStream.close();
          FlightEndpoint endpoint = flightEndpoints.get(nextEndpointIndex++);
          if (endpoint.getLocations().isEmpty()) {
            currentStream = client.getStream(endpoint.getTicket());
          } else {
            // TODO: this could also retry/loop over locations
            // TODO: filter out non-gRPC locations
            final int index = ThreadLocalRandom.current().nextInt(endpoint.getLocations().size());
            final Location location = endpoint.getLocations().get(index);
            currentStream =
                Objects.requireNonNull(clientCache.get(location)).getStream(endpoint.getTicket());
          }
          if (!schema.equals(currentStream.getSchema())) {
            throw new IOException(
                "Stream has inconsistent schema. Expected: "
                    + schema
                    + "\nFound: "
                    + currentStream.getSchema());
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
    final VectorSchemaRoot root = currentStream.getRoot();
    final VectorUnloader unloader = new VectorUnloader(root);
    final ArrowRecordBatch recordBatch = unloader.getRecordBatch();
    bytesRead += recordBatch.computeBodyLength();
    loadRecordBatch(recordBatch);
    return true;
  }

  @Override
  public long bytesRead() {
    return bytesRead;
  }

  @Override
  protected void closeReadSource() throws IOException {
    try {
      currentStream.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Schema readSchema() {
    return schema;
  }
}

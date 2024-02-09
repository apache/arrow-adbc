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

import static org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverUtil.tryLoadNextStream;

import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/** Base class for ArrowReaders based on consuming data from FlightEndpoints. */
public abstract class BaseFlightReader extends ArrowReader {

  private final List<FlightEndpoint> flightEndpoints;
  private final Supplier<List<FlightEndpoint>> rpcCall;
  private int nextEndpointIndex = 0;
  private FlightStream currentStream;
  private Schema schema;
  private long bytesRead = 0;
  protected final FlightSqlClientWithCallOptions client;
  protected final LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache;

  protected BaseFlightReader(
      BufferAllocator allocator,
      FlightSqlClientWithCallOptions client,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
      Supplier<List<FlightEndpoint>> rpcCall) {
    super(allocator);
    this.client = client;
    this.clientCache = clientCache;
    this.flightEndpoints = new ArrayList<>();
    this.rpcCall = rpcCall;
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
          currentStream = tryLoadNextStream(endpoint, client, clientCache);
          if (!schema.equals(currentStream.getSchema())) {
            throw new IOException(
                "Stream has inconsistent schema. Expected: "
                    + schema
                    + "\nFound: "
                    + currentStream.getSchema());
          }
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
    processRootFromStream(currentStream.getRoot());
    return true;
  }

  @Override
  protected Schema readSchema() throws IOException {
    return schema;
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

  protected abstract void processRootFromStream(VectorSchemaRoot root);

  protected void addBytesRead(long bytes) {
    this.bytesRead += bytes;
  }

  protected void populateEndpointData() throws AdbcException {
    try {
      this.flightEndpoints.addAll(rpcCall.get());
      this.currentStream =
          tryLoadNextStream(flightEndpoints.get(this.nextEndpointIndex++), client, clientCache);
      this.schema = this.currentStream.getSchema();
    } catch (FlightRuntimeException e) {
      throw FlightSqlDriverUtil.fromFlightException(e);
    } catch (IOException e) {
      throw new AdbcException(e.getMessage(), e, AdbcStatusCode.IO, null, 0);
    }
  }

  protected void loadRoot(VectorSchemaRoot root) {
    final VectorUnloader unloader = new VectorUnloader(root);
    final ArrowRecordBatch recordBatch = unloader.getRecordBatch();
    addBytesRead(recordBatch.computeBodyLength());
    loadRecordBatch(recordBatch);
  }
}

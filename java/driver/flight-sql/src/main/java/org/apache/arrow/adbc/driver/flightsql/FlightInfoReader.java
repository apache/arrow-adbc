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
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/** An ArrowReader that wraps a FlightInfo. */
public class FlightInfoReader extends BaseFlightReader {
  @SuppressWarnings(
      "method.invocation") // Checker Framework does not like the ensureInitialized call
  FlightInfoReader(
      BufferAllocator allocator,
      FlightSqlClientWithCallOptions client,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache,
      List<FlightEndpoint> flightEndpoints)
      throws AdbcException {
    super(allocator, client, clientCache, () -> flightEndpoints);

    populateEndpointData();
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
  protected void processRootFromStream(VectorSchemaRoot root) {
    loadRoot(root);
  }
}

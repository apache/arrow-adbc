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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.protobuf.InvalidProtocolBufferException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

public class FlightSqlConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final FlightSqlClient client;
  private final SqlQuirks quirks;
  private final LoadingCache<Location, FlightClient> clientCache;

  FlightSqlConnection(
      BufferAllocator allocator,
      SqlQuirks quirks,
      Location location,
      String username,
      String password) {
    this.allocator = allocator;
    this.quirks = quirks;
    this.clientCache =
        Caffeine.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .removalListener(
                (Location key, FlightClient value, RemovalCause cause) -> {
                  if (value == null) return;
                  try {
                    value.close();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                })
            .build(
                loc -> {
                  FlightClient flightClient =
                      FlightClient.builder(allocator, loc)
                          .intercept(
                              new FlightClientMiddleware.Factory() {
                                final String[] bearerValue = {null};

                                @Override
                                public FlightClientMiddleware onCallStarted(CallInfo info) {
                                  return new FlightClientMiddleware() {
                                    @Override
                                    public void onBeforeSendingHeaders(
                                        CallHeaders outgoingHeaders) {
                                      if (bearerValue[0] != null) {
                                        outgoingHeaders.insert(
                                            Auth2Constants.AUTHORIZATION_HEADER, bearerValue[0]);
                                      }
                                    }

                                    @Override
                                    public void onHeadersReceived(CallHeaders incomingHeaders) {
                                      if (bearerValue[0] == null) {
                                        bearerValue[0] =
                                            incomingHeaders.get(
                                                Auth2Constants.AUTHORIZATION_HEADER);
                                      }
                                    }

                                    @Override
                                    public void onCallCompleted(CallStatus status) {}
                                  };
                                }
                              })
                          .build();

                  if (username != null) {
                    flightClient.handshake(
                        new CredentialCallOption(
                            new BasicAuthCredentialWriter(username, password)));
                  }
                  return flightClient;
                });

    this.client = new FlightSqlClient(Objects.requireNonNull(clientCache.get(location)));
  }

  @Override
  public void commit() throws AdbcException {
    throw AdbcException.notImplemented("[Flight SQL] Transaction methods are not supported");
  }

  @Override
  public AdbcStatement createStatement() throws AdbcException {
    return new FlightSqlStatement(allocator, client, clientCache, quirks);
  }

  @Override
  public ArrowReader readPartition(ByteBuffer descriptor) throws AdbcException {
    final FlightEndpoint endpoint;
    try {
      final Flight.FlightEndpoint protoEndpoint = Flight.FlightEndpoint.parseFrom(descriptor);
      Location[] locations = new Location[protoEndpoint.getLocationCount()];
      int index = 0;
      for (Flight.Location protoLocation : protoEndpoint.getLocationList()) {
        Location location = new Location(protoLocation.getUri());
        locations[index++] = location;
      }

      endpoint =
          new FlightEndpoint(
              new Ticket(protoEndpoint.getTicket().getTicket().toByteArray()), locations);
    } catch (InvalidProtocolBufferException | URISyntaxException e) {
      throw AdbcException.invalidArgument(
              "[Flight SQL] Partition descriptor is invalid: " + e.getMessage())
          .withCause(e);
    }
    return new FlightInfoReader(
        allocator, client, clientCache, Collections.singletonList(endpoint));
  }

  @Override
  public AdbcStatement bulkIngest(String targetTableName, BulkIngestMode mode)
      throws AdbcException {
    return FlightSqlStatement.ingestRoot(
        allocator, client, clientCache, quirks, targetTableName, mode);
  }

  @Override
  public ArrowReader getInfo(int[] infoCodes) throws AdbcException {
    try (final VectorSchemaRoot root =
        new InfoMetadataBuilder(allocator, client, infoCodes).build()) {
      return RootArrowReader.fromRoot(allocator, root);
    }
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

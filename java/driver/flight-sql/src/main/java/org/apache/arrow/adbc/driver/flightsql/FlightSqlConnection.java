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
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FlightSqlConnection implements AdbcConnection {
  private final BufferAllocator allocator;
  private final AtomicInteger counter = new AtomicInteger(0);
  private final FlightSqlClientWithCallOptions client;
  private final SqlQuirks quirks;
  private final Map<String, Object> parameters;
  private final LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache;

  // Cached data to use across additional connections.
  private ClientCookieMiddleware.@Nullable Factory cookieMiddlewareFactory;
  private CallOption[] callOptions;

  // Used to cache the InputStream content as a byte array since
  // subsequent connections may need to use it but it is supplied as a stream.
  private byte @Nullable [] mtlsCertChainBytes;
  private byte @Nullable [] mtlsPrivateKeyBytes;
  private byte @Nullable [] tlsRootCertsBytes;

  FlightSqlConnection(
      BufferAllocator allocator,
      SqlQuirks quirks,
      Location location,
      Map<String, Object> parameters)
      throws AdbcException {
    this.allocator = allocator;
    this.quirks = quirks;
    this.parameters = parameters;
    this.callOptions = new CallOption[0];
    FlightSqlClient flightSqlClient = new FlightSqlClient(createInitialConnection(location));
    this.client = new FlightSqlClientWithCallOptions(flightSqlClient, callOptions);
    this.clientCache =
        Caffeine.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .removalListener(
                (@Nullable Location key,
                    @Nullable FlightSqlClientWithCallOptions value,
                    RemovalCause cause) -> {
                  if (value == null) return;
                  try {
                    value.close();
                  } catch (Exception ex) {
                    if (ex instanceof InterruptedException) {
                      Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException(ex);
                  }
                })
            .build(
                loc -> {
                  FlightClient client = buildClient(loc);
                  client.handshake(callOptions);
                  return new FlightSqlClientWithCallOptions(
                      new FlightSqlClient(client), callOptions);
                });
    this.clientCache.put(location, this.client);
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
  public ArrowReader getObjects(
      GetObjectsDepth depth,
      String catalogPattern,
      String dbSchemaPattern,
      String tableNamePattern,
      String[] tableTypes,
      String columnNamePattern)
      throws AdbcException {
    return GetObjectsMetadataReaders.CreateGetObjectsReader(
        allocator,
        client,
        clientCache,
        depth,
        catalogPattern,
        dbSchemaPattern,
        tableNamePattern,
        tableTypes,
        columnNamePattern);
  }

  @Override
  public ArrowReader getInfo(int @Nullable [] infoCodes) throws AdbcException {
    try {
      return GetInfoMetadataReader.CreateGetInfoMetadataReader(
          allocator, client, clientCache, infoCodes);
    } catch (Exception e) {
      throw AdbcException.invalidState("[Flight SQL] Failed to get info").withCause(e);
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
  public void close() throws AdbcException {
    clientCache.invalidateAll();
    try {
      AutoCloseables.close(client, allocator);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw AdbcException.io(e);
    }
  }

  @Override
  public String toString() {
    return "FlightSqlConnection{" + "client=" + client + '}';
  }

  /**
   * Initialize cached data to share between connections and create, test, and authenticate the
   * first connection.
   */
  private FlightClient createInitialConnection(
      @UnknownInitialization FlightSqlConnection this, Location location) throws AdbcException {
    // Setup cached pre-connection properties.
    try {
      if (parameters != null) {
        final InputStream mtlsCertChain =
            FlightSqlConnectionProperties.MTLS_CERT_CHAIN.get(parameters);
        if (mtlsCertChain != null) {
          this.mtlsCertChainBytes = inputStreamToBytes(mtlsCertChain);
        }

        final InputStream mtlsPrivateKey =
            FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.get(parameters);
        if (mtlsPrivateKey != null) {
          this.mtlsPrivateKeyBytes = inputStreamToBytes(mtlsPrivateKey);
        }

        final InputStream tlsRootCerts =
            FlightSqlConnectionProperties.TLS_ROOT_CERTS.get(parameters);
        if (tlsRootCerts != null) {
          this.tlsRootCertsBytes = inputStreamToBytes(tlsRootCerts);
        }
      }
    } catch (IOException ex) {
      throw new AdbcException(
          String.format(
              "Error reading stream for one of the options %s, %s, %s.",
              FlightSqlConnectionProperties.MTLS_CERT_CHAIN.getKey(),
              FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.getKey(),
              FlightSqlConnectionProperties.TLS_ROOT_CERTS.getKey()),
          ex,
          AdbcStatusCode.IO,
          null,
          0);
    }

    if (parameters != null) {
      final boolean useCookieMiddleware =
          Boolean.TRUE.equals(FlightSqlConnectionProperties.WITH_COOKIE_MIDDLEWARE.get(parameters));
      if (useCookieMiddleware) {
        this.cookieMiddlewareFactory = new ClientCookieMiddleware.Factory();
      }
    }

    // Build the client using the above properties.
    final FlightClient client = buildClient(location);

    // Add user-specified headers.
    ArrayList<CallOption> options = new ArrayList<>();
    final FlightCallHeaders callHeaders = new FlightCallHeaders();
    for (Map.Entry<String, Object> parameter : parameters.entrySet()) {
      if (parameter.getKey().startsWith(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX)) {
        String userHeaderName =
            parameter
                .getKey()
                .substring(FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX.length());

        if (parameter.getValue() instanceof String) {
          callHeaders.insert(userHeaderName, (String) parameter.getValue());
        } else if (parameter.getValue() instanceof byte[]) {
          callHeaders.insert(userHeaderName, (byte[]) parameter.getValue());
        } else {
          throw new AdbcException(
              String.format(
                  "Header values must be String or byte[]. The header failing was %s.",
                  parameter.getKey()),
              null,
              AdbcStatusCode.INVALID_ARGUMENT,
              null,
              0);
        }
      }
    }

    options.add(new HeaderCallOption(callHeaders));

    // Test the connection.
    String username = AdbcDriver.PARAM_USERNAME.get(parameters);
    String password = AdbcDriver.PARAM_PASSWORD.get(parameters);
    if (username != null && password != null) {
      Optional<CredentialCallOption> bearerToken =
          client.authenticateBasicToken(username, password);
      options.add(
          bearerToken.orElse(
              new CredentialCallOption(new BasicAuthCredentialWriter(username, password))));
      this.callOptions = options.toArray(new CallOption[0]);
    } else {
      this.callOptions = options.toArray(new CallOption[0]);
      client.handshake(this.callOptions);
    }

    return client;
  }

  /** Returns a yet-to-be authenticated FlightClient */
  private FlightClient buildClient(
      @UnknownInitialization FlightSqlConnection this, Location location) throws AdbcException {
    if (allocator == null) {
      throw new IllegalStateException("Internal error: allocator was not initialized");
    }
    final FlightClient.Builder builder =
        FlightClient.builder()
            .allocator(
                allocator.newChildAllocator(
                    "adbc-flightclient-connection-" + counter.getAndIncrement(),
                    0,
                    allocator.getLimit()))
            .location(location);

    // Configure TLS options.
    if (mtlsCertChainBytes != null && mtlsPrivateKeyBytes != null) {
      builder.clientCertificate(
          new ByteArrayInputStream(mtlsCertChainBytes),
          new ByteArrayInputStream(mtlsPrivateKeyBytes));
    } else if (mtlsCertChainBytes != null) {
      throw new AdbcException(
          String.format(
              "Must provide both %s and %s or neither. %s provided only.",
              FlightSqlConnectionProperties.MTLS_CERT_CHAIN.getKey(),
              FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.getKey(),
              FlightSqlConnectionProperties.MTLS_CERT_CHAIN.getKey()),
          null,
          AdbcStatusCode.INVALID_ARGUMENT,
          null,
          0);
    } else if (mtlsPrivateKeyBytes != null) {
      throw new AdbcException(
          String.format(
              "Must provide both %s and %s or neither. %s provided only.",
              FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.getKey(),
              FlightSqlConnectionProperties.MTLS_CERT_CHAIN.getKey(),
              FlightSqlConnectionProperties.MTLS_PRIVATE_KEY.getKey()),
          null,
          AdbcStatusCode.INVALID_ARGUMENT,
          null,
          0);
    }

    if (tlsRootCertsBytes != null) {
      builder.trustedCertificates(new ByteArrayInputStream(tlsRootCertsBytes));
    }

    if (parameters != null) {
      if (Boolean.TRUE.equals(FlightSqlConnectionProperties.TLS_SKIP_VERIFY.get(parameters))) {
        builder.verifyServer(false);
      }

      String hostnameOverride = FlightSqlConnectionProperties.TLS_OVERRIDE_HOSTNAME.get(parameters);
      if (hostnameOverride != null) {
        builder.overrideHostname(hostnameOverride);
      }
    }

    // Setup cookies if needed.
    if (cookieMiddlewareFactory != null) {
      builder.intercept(cookieMiddlewareFactory);
    }

    return builder.build();
  }

  private static byte[] inputStreamToBytes(InputStream stream) throws IOException {
    byte[] bytes = new byte[stream.available()];
    DataInputStream dataInputStream = new DataInputStream(stream);
    dataInputStream.readFully(bytes);
    return bytes;
  }
}

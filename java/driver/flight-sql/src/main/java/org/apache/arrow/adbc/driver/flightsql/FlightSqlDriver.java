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

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * An ADBC driver wrapping Arrow Flight SQL.
 *
 * <p>The "uri" option accepts the {@code flightsql://} scheme: secure TLS by default, or add {@code
 * ?transport=tcp} for plaintext, or {@code ?transport=unix} with a socket path for a Unix domain
 * socket. The {@code transport} value is matched case-insensitively, and an unrecognized value is
 * rejected rather than silently falling back to a default. The legacy {@code grpc://}, {@code
 * grpc+tcp://}, {@code grpc+tls://}, and {@code grpc+unix://} schemes are also still accepted.
 */
public class FlightSqlDriver implements AdbcDriver {
  private static final String FLIGHTSQL_SCHEME = "flightsql";
  private static final String TRANSPORT_PARAM = "transport";

  private final BufferAllocator allocator;

  public FlightSqlDriver(BufferAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator);
  }

  @Override
  public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
    String uri = PARAM_URI.get(parameters);
    if (uri == null) {
      Object target = parameters.get(AdbcDriver.PARAM_URL);
      if (!(target instanceof String)) {
        throw AdbcException.invalidArgument(
            "[Flight SQL] Must provide String " + PARAM_URI + " parameter");
      }
      uri = (String) target;
    }

    Location location = parseLocation(uri);

    Object quirks = parameters.get(PARAM_SQL_QUIRKS);
    if (quirks != null) {
      Preconditions.checkArgument(
          quirks instanceof SqlQuirks,
          String.format(
              "[Flight SQL] %s must be a SqlQuirks instance, not %s",
              PARAM_SQL_QUIRKS, quirks.getClass().getName()));
    } else {
      quirks = new SqlQuirks();
    }
    return new FlightSqlDatabase(allocator, location, (SqlQuirks) quirks, parameters);
  }

  /**
   * Parses the "uri" option into a {@link Location}, translating a {@code flightsql://} scheme (and
   * its {@code transport} query parameter) into the legacy {@code grpc*://} scheme that {@link
   * Location} understands natively. Any other scheme is passed through unchanged.
   */
  static Location parseLocation(String uri) throws AdbcException {
    final URI parsed;
    try {
      parsed = new URI(uri);
    } catch (URISyntaxException e) {
      throw AdbcException.invalidArgument(
              String.format("[Flight SQL] Location %s is invalid: %s", uri, e))
          .withCause(e);
    }

    if (!FLIGHTSQL_SCHEME.equalsIgnoreCase(parsed.getScheme())) {
      return new Location(parsed);
    }

    if (parsed.getUserInfo() != null || parsed.getFragment() != null) {
      throw AdbcException.invalidArgument(
          String.format(
              "[Flight SQL] Invalid URI '%s': userinfo and fragment are not supported in %s://"
                  + " URIs",
              uri, FLIGHTSQL_SCHEME));
    }

    final String transport = transportOf(uri, parsed);
    final String host = parsed.getHost();
    final String path = parsed.getPath();
    switch (transport) {
      case "":
      case "tls":
        requireNoPath(uri, path);
        requireHost(uri, host);
        return buildLocation(uri, () -> Location.forGrpcTls(host, parsed.getPort()));
      case "tcp":
        requireNoPath(uri, path);
        requireHost(uri, host);
        return buildLocation(uri, () -> Location.forGrpcInsecure(host, parsed.getPort()));
      case "unix":
        if (host != null && !host.isEmpty()) {
          throw AdbcException.invalidArgument(
              String.format(
                  "[Flight SQL] Invalid URI '%s': a host is not valid with transport=unix", uri));
        }
        if (path == null || path.isEmpty()) {
          throw AdbcException.invalidArgument(
              String.format(
                  "[Flight SQL] Invalid URI '%s': transport=unix requires a socket path", uri));
        }
        return buildLocation(uri, () -> Location.forGrpcDomainSocket(path));
      default:
        throw AdbcException.invalidArgument(
            String.format(
                "[Flight SQL] Invalid URI '%s': unrecognized transport '%s' (expected 'tls',"
                    + " 'tcp', or 'unix')",
                uri, transport));
    }
  }

  private static void requireNoPath(String uri, String path) throws AdbcException {
    if (path != null && !path.isEmpty()) {
      throw AdbcException.invalidArgument(
          String.format(
              "[Flight SQL] Invalid URI '%s': a socket path is only valid with transport=unix",
              uri));
    }
  }

  private static void requireHost(String uri, String host) throws AdbcException {
    if (host == null || host.isEmpty()) {
      throw AdbcException.invalidArgument(
          String.format("[Flight SQL] Invalid URI '%s': a host is required", uri));
    }
  }

  /**
   * Builds a {@link Location} via the given factory, wrapping the {@link IllegalArgumentException}
   * that {@link Location}'s {@code forGrpc*} factories document throwing on an invalid URI into an
   * {@link AdbcException} instead of letting it escape as an unchecked exception.
   */
  private static Location buildLocation(String uri, Supplier<Location> factory)
      throws AdbcException {
    try {
      return factory.get();
    } catch (IllegalArgumentException e) {
      throw AdbcException.invalidArgument(
              String.format("[Flight SQL] Location %s is invalid: %s", uri, e))
          .withCause(e);
    }
  }

  /** Extracts and lowercases the {@code transport} query parameter, defaulting to "". */
  private static String transportOf(String uri, URI parsed) throws AdbcException {
    final String query = parsed.getRawQuery();
    if (query == null) {
      return "";
    }
    int start = 0;
    while (start <= query.length()) {
      int amp = query.indexOf('&', start);
      int end = amp >= 0 ? amp : query.length();
      String pair = query.substring(start, end);
      int eq = pair.indexOf('=');
      String key = eq >= 0 ? pair.substring(0, eq) : pair;
      if (TRANSPORT_PARAM.equalsIgnoreCase(key)) {
        String value = eq >= 0 ? pair.substring(eq + 1) : "";
        try {
          return URLDecoder.decode(value, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        } catch (IllegalArgumentException e) {
          throw AdbcException.invalidArgument(
                  String.format(
                      "[Flight SQL] Invalid URI '%s': malformed transport value: %s", uri, e))
              .withCause(e);
        }
      }
      if (amp < 0) {
        break;
      }
      start = amp + 1;
    }
    return "";
  }
}

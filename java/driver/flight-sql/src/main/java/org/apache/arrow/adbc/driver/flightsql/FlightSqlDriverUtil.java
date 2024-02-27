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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.ErrorDetail;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.checkerframework.checker.nullness.qual.Nullable;

final class FlightSqlDriverUtil {
  private FlightSqlDriverUtil() {
    throw new AssertionError("Do not instantiate this class");
  }

  static String prefixExceptionMessage(final @Nullable String s) {
    // Allow null since Throwable#getMessage may be null
    if (s == null) {
      return "[Flight SQL] (No or unknown error)";
    }
    return "[Flight SQL] " + s;
  }

  static AdbcException fromSqlException(SQLException e) {
    return new AdbcException(
        prefixExceptionMessage(e.getMessage()),
        e.getCause(),
        AdbcStatusCode.UNKNOWN,
        e.getSQLState(),
        e.getErrorCode());
  }

  static AdbcException fromGeneralException(Exception ex) {
    return new AdbcException(ex.getMessage(), ex, AdbcStatusCode.UNKNOWN, null, 0);
  }

  static AdbcStatusCode fromFlightStatusCode(FlightStatusCode code) {
    switch (code) {
      case OK:
        throw new IllegalArgumentException("Cannot convert OK status");
      case UNKNOWN:
        return AdbcStatusCode.UNKNOWN;
      case INTERNAL:
        return AdbcStatusCode.INTERNAL;
      case INVALID_ARGUMENT:
        return AdbcStatusCode.INVALID_ARGUMENT;
      case TIMED_OUT:
        return AdbcStatusCode.TIMEOUT;
      case NOT_FOUND:
        return AdbcStatusCode.NOT_FOUND;
      case ALREADY_EXISTS:
        return AdbcStatusCode.ALREADY_EXISTS;
      case CANCELLED:
        return AdbcStatusCode.CANCELLED;
      case UNAUTHENTICATED:
        return AdbcStatusCode.UNAUTHENTICATED;
      case UNAUTHORIZED:
        return AdbcStatusCode.UNAUTHORIZED;
      case UNIMPLEMENTED:
        return AdbcStatusCode.NOT_IMPLEMENTED;
      case UNAVAILABLE:
        return AdbcStatusCode.IO;
      default:
        return AdbcStatusCode.UNKNOWN;
    }
  }

  static AdbcException fromFlightException(FlightRuntimeException e) {
    List<ErrorDetail> errorDetails = new ArrayList<>();
    for (String key : e.status().metadata().keys()) {
      if (key.endsWith("-bin")) {
        for (byte[] value : e.status().metadata().getAllByte(key)) {
          errorDetails.add(new ErrorDetail(key, value));
        }
      } else {
        for (String value : e.status().metadata().getAll(key)) {
          errorDetails.add(new ErrorDetail(key, value));
        }
      }
    }
    return new AdbcException(
        e.getMessage(),
        e.getCause(),
        fromFlightStatusCode(e.status().code()),
        null,
        0,
        errorDetails);
  }

  static FlightStream tryLoadNextStream(
      FlightEndpoint endpoint,
      FlightSqlClientWithCallOptions rootClient,
      LoadingCache<Location, FlightSqlClientWithCallOptions> clientCache)
      throws IOException {
    if (endpoint.getLocations().isEmpty()) {
      return rootClient.getStream(endpoint.getTicket());
    } else {
      List<Location> locations = new ArrayList<>(endpoint.getLocations());
      Collections.shuffle(locations);
      IOException failure = null;
      for (final Location location : locations) {
        final @Nullable FlightSqlClientWithCallOptions client = clientCache.get(location);
        if (client == null) {
          throw new IllegalStateException("Could not connect to " + location);
        }
        try {
          return client.getStream(endpoint.getTicket());
        } catch (RuntimeException e) {
          // Also handles CompletionException (from clientCache#get), FlightRuntimeException
          if (failure == null) {
            failure =
                new IOException("Failed to get stream from location " + location + ": " + e, e);
          } else {
            failure.addSuppressed(
                new IOException("Failed to get stream from location " + location + ": " + e, e));
          }
        }
      }
      if (failure == null) {
        throw new IllegalStateException("FlightEndpoint had no locations");
      }
      throw Objects.requireNonNull(failure);
    }
  }
}

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

import java.sql.SQLException;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

final class FlightSqlDriverUtil {
  private FlightSqlDriverUtil() {
    throw new AssertionError("Do not instantiate this class");
  }

  static String prefixExceptionMessage(final String s) {
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
    return new AdbcException(
        e.getMessage(), e.getCause(), fromFlightStatusCode(e.status().code()), null, 0);
  }
}

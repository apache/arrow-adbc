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

import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;

public final class ErrorConverter {
  public static AdbcStatusCode toAdbcStatusCode(FlightStatusCode code) {
    switch (code) {
      case OK:
        assert (false);
        return null;
      case INTERNAL:
        return AdbcStatusCode.INTEGRITY;
      case INVALID_ARGUMENT:
        return AdbcStatusCode.INVALID_ARGUMENT;
      case TIMED_OUT:
        return AdbcStatusCode.TIMEOUT;
      case NOT_FOUND:
        return AdbcStatusCode.TIMEOUT;
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
      case UNKNOWN:
      default:
        return AdbcStatusCode.UNKNOWN;
    }
  }

  public static AdbcException toAdbcException(FlightRuntimeException flightException) {
    return new AdbcException(
        flightException.getMessage(),
        flightException,
        toAdbcStatusCode(flightException.status().code()),
        null,
        flightException.status().code().ordinal());
  }

  public static AdbcException fromGeneralException(Exception ex) {
    return new AdbcException(ex.getMessage(), ex, AdbcStatusCode.UNKNOWN, null, 0);
  }
}

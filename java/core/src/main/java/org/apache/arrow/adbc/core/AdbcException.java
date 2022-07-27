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
package org.apache.arrow.adbc.core;

/**
 * An error in the database or ADBC driver.
 *
 * <p>The exception contains up to five types of information about the error:
 *
 * <ul>
 *   <li>An error message
 *   <li>An exception cause
 *   <li>An ADBC status code
 *   <li>A SQLSTATE string
 *   <li>A vendor-specific status code
 * </ul>
 */
public class AdbcException extends Exception {
  private final AdbcStatusCode status;
  private final String sqlState;
  private final int vendorCode;

  // TODO: do we also want to support a multi-exception akin to SQLException#setNextException
  public AdbcException(
      String message, Throwable cause, AdbcStatusCode status, String sqlState, int vendorCode) {
    super(message, cause);
    this.status = status;
    this.sqlState = sqlState;
    this.vendorCode = vendorCode;
  }

  /** Create a new exception with code {@link AdbcStatusCode#INVALID_ARGUMENT}. */
  public static AdbcException invalidArgument(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.INVALID_ARGUMENT, null, 0);
  }

  /** Create a new exception with code {@link AdbcStatusCode#IO}. */
  public static AdbcException io(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.IO, null, 0);
  }

  /** Create a new exception with code {@link AdbcStatusCode#INVALID_STATE}. */
  public static AdbcException invalidState(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.INVALID_STATE, null, 0);
  }

  /** Create a new exception with code {@link AdbcStatusCode#NOT_IMPLEMENTED}. */
  public static AdbcException notImplemented(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.NOT_IMPLEMENTED, null, 0);
  }

  public AdbcStatusCode getStatus() {
    return status;
  }

  public String getSqlState() {
    return sqlState;
  }

  public int getVendorCode() {
    return vendorCode;
  }

  /**
   * Copy this exception with a different cause (a convenience for use with the static factories).
   */
  public AdbcException withCause(Throwable cause) {
    return new AdbcException(this.getMessage(), cause, status, sqlState, vendorCode);
  }

  @Override
  public String toString() {
    return "AdbcException{"
        + "message="
        + getMessage()
        + ", status="
        + status
        + ", sqlState='"
        + sqlState
        + '\''
        + ", vendorCode="
        + vendorCode
        + ", cause="
        + getCause()
        + '}';
  }
}

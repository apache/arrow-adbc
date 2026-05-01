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

import java.util.Collection;
import java.util.Collections;
import org.checkerframework.checker.nullness.qual.Nullable;

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
  private final @Nullable String sqlState;
  private final int vendorCode;
  private Collection<ErrorDetail> details;

  public AdbcException(
      @Nullable String message,
      @Nullable Throwable cause,
      AdbcStatusCode status,
      @Nullable String sqlState,
      int vendorCode) {
    this(message, cause, status, sqlState, vendorCode, Collections.emptyList());
  }

  public AdbcException(
      @Nullable String message,
      @Nullable Throwable cause,
      AdbcStatusCode status,
      @Nullable String sqlState,
      int vendorCode,
      Collection<ErrorDetail> details) {
    super(message, cause);
    this.status = status;
    this.sqlState = sqlState;
    this.vendorCode = vendorCode;
    this.details = details;
  }

  /** Create a new exception with code {@link AdbcStatusCode#INVALID_ARGUMENT}. */
  public static AdbcException invalidArgument(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.INVALID_ARGUMENT, null, 0);
  }

  /** Create a new exception with code {@link AdbcStatusCode#IO}. */
  public static AdbcException io(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.IO, null, 0);
  }

  /** Create a new exception with code {@link AdbcStatusCode#IO} from an existing exception. */
  public static AdbcException io(Throwable cause) {
    return new AdbcException(cause.getMessage(), cause, AdbcStatusCode.IO, null, 0);
  }

  /** Create a new exception with code {@link AdbcStatusCode#INVALID_STATE}. */
  public static AdbcException invalidState(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.INVALID_STATE, null, 0);
  }

  /** Create a new exception with code {@link AdbcStatusCode#NOT_IMPLEMENTED}. */
  public static AdbcException notImplemented(String message) {
    return new AdbcException(message, /*cause*/ null, AdbcStatusCode.NOT_IMPLEMENTED, null, 0);
  }

  /** The ADBC status code. */
  public AdbcStatusCode getStatus() {
    return status;
  }

  /** A SQLSTATE error code, if provided, as defined by the SQL:2003 standard. */
  public @Nullable String getSqlState() {
    return sqlState;
  }

  /** A vendor-specific error code, if applicable. */
  public int getVendorCode() {
    return vendorCode;
  }

  /**
   * Get extra driver-specific error details.
   *
   * <p>This allows drivers to return custom, structured error information (for example, JSON or
   * Protocol Buffers) that can be optionally parsed by clients, beyond the standard AdbcError
   * fields, without having to encode it in the error message. The encoding of the data is
   * driver-defined.
   */
  public Collection<ErrorDetail> getDetails() {
    return details;
  }

  /**
   * Copy this exception with a different cause (a convenience for use with the static factories).
   */
  public AdbcException withCause(Throwable cause) {
    return new AdbcException(getMessage(), cause, status, sqlState, vendorCode, details);
  }

  /**
   * Copy this exception with different details (a convenience for use with the static factories).
   */
  public AdbcException withDetails(Collection<ErrorDetail> details) {
    return new AdbcException(getMessage(), getCause(), status, sqlState, vendorCode, details);
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
        + ", details="
        + getDetails().size()
        + '}';
  }
}

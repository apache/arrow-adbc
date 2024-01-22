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
 * A status code indicating the general category of error that occurred.
 *
 * <p>Also see the ADBC C API definition, which has similar status codes.
 */
public enum AdbcStatusCode {
  /**
   * An unknown error occurred.
   *
   * <p>May indicate client-side or database-side error.
   */
  UNKNOWN,
  /**
   * The operation is not supported.
   *
   * <p>May indicate client-side or database-side error.
   */
  NOT_IMPLEMENTED,
  /**
   * \brief A requested resource already exists.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  NOT_FOUND,
  /**
   * A requested resource already exists.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  ALREADY_EXISTS,
  /**
   * The arguments are invalid, likely a programming error.
   *
   * <p>For instance, they may be of the wrong format, or out of range.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  INVALID_ARGUMENT,
  /**
   * The preconditions for the operation are not met, likely a programming error.
   *
   * <p>For instance, the object may be uninitialized, or may have not been fully configured.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  INVALID_STATE,
  /**
   * Invalid data was processed (not a programming error).
   *
   * <p>For instance, a division by zero may have occurred during query execution.
   *
   * <p>May indicate a database-side error only.
   */
  INVALID_DATA,
  /**
   * The database's integrity was affected.
   *
   * <p>For instance, a foreign key check may have failed, or a uniqueness constraint may have been
   * violated.
   *
   * <p>May indicate a database-side error only.
   */
  INTEGRITY,
  /**
   * An error internal to the driver or database occurred.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  INTERNAL,
  /**
   * An I/O error occurred.
   *
   * <p>For instance, a remote service may be unavailable.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  IO,
  /**
   * The operation was cancelled, not due to a timeout.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  CANCELLED,
  /**
   * The operation was cancelled due to a timeout.
   *
   * <p>May indicate a driver-side or database-side error.
   */
  TIMEOUT,
  /**
   * Authentication failed.
   *
   * <p>May indicate a database-side error only.
   */
  UNAUTHENTICATED,
  /**
   * The client is not authorized to perform the given operation.
   *
   * <p>May indicate a database-side error only.
   */
  UNAUTHORIZED,
  ;
}

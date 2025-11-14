/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <glib-object.h>

#include <adbc-glib/version.h>

G_BEGIN_DECLS

/**
 * GADBCError:
 * @GADBC_ERROR_OK: No error.
 * @GADBC_ERROR_UNKNOWN: An unknown error occurred.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_NOT_IMPLEMENTED: The operation is not implemented or supported.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_NOT_FOUND: A requested resource was not found.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_ALREADY_EXISTS: A requested resource already exists.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_INVALID_ARGUMENT: The arguments are invalid,
 *   likely a programming error.
 *   For instance, they may be of the wrong format, or out of range.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_INVALID_STATE: The preconditions for the operation are not met,
 *   likely a programming error.
 *   For instance, the object may be uninitialized, or may have not
 *   been fully configured.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_INVALID_DATA: Invalid data was processed
 *   (not a programming error).
 *   For instance, a division by zero may have occurred during query
 *   execution.
 *   May indicate a database-side error only.
 * @GADBC_ERROR_INTEGRITY: The database's integrity was affected.
 *   For instance, a foreign key check may have failed, or a uniqueness
 *   constraint may have been violated.
 *   May indicate a database-side error only.
 * @GADBC_ERROR_INTERNAL: An error internal to the driver or database occurred.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_IO: An I/O error occurred.
 *   For instance, a remote service may be unavailable.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_CANCELLED: The operation was cancelled, not due to a timeout.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_TIMEOUT: The operation was cancelled due to a timeout.
 *   May indicate a driver-side or database-side error.
 * @GADBC_ERROR_UNAUTHENTICATED: Authentication failed.
 *   May indicate a database-side error only.
 * @GADBC_ERROR_UNAUTHORIZED: The client is not authorized to perform the
 *   given operation.
 *   May indicate a database-side error only.
 *
 * The error codes are used by all adbc-glib functions.
 *
 * They are corresponding to `ADBC_STATUS_*` values.
 */
typedef enum {
  GADBC_ERROR_OK = 0,
  GADBC_ERROR_UNKNOWN = 1,
  GADBC_ERROR_NOT_IMPLEMENTED = 2,
  GADBC_ERROR_NOT_FOUND = 3,
  GADBC_ERROR_ALREADY_EXISTS = 4,
  GADBC_ERROR_INVALID_ARGUMENT = 5,
  GADBC_ERROR_INVALID_STATE = 6,
  GADBC_ERROR_INVALID_DATA = 7,
  GADBC_ERROR_INTEGRITY = 8,
  GADBC_ERROR_INTERNAL = 9,
  GADBC_ERROR_IO = 10,
  GADBC_ERROR_CANCELLED = 11,
  GADBC_ERROR_TIMEOUT = 12,
  GADBC_ERROR_UNAUTHENTICATED = 13,
  GADBC_ERROR_UNAUTHORIZED = 14,
} GADBCError;

#define GADBC_ERROR gadbc_error_quark()

GADBC_AVAILABLE_IN_0_1
GQuark gadbc_error_quark(void);

G_END_DECLS

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

#include <arrow-adbc/adbc_driver_manager.h>

/* This is needless. This is just for cpplint. */
#include <adbc-glib/error.h>

#include <adbc-glib/error-raw.h>

/**
 * SECTION: error
 * @title: GADBCError
 * @include: adbc-glib/adbc-glib.h
 *
 * #GADBCError provides error codes corresponding to `ADBC_STATUS_*`
 * values.
 */

/* clang-format off */
G_DEFINE_QUARK(gadbc-error-quark, gadbc_error)
/* clang-format on */

GADBCError gadbc_error_from_status_code(AdbcStatusCode status_code) {
  switch (status_code) {
    case ADBC_STATUS_OK:
      return GADBC_ERROR_OK;
    case ADBC_STATUS_UNKNOWN:
      return GADBC_ERROR_UNKNOWN;
    case ADBC_STATUS_NOT_IMPLEMENTED:
      return GADBC_ERROR_NOT_IMPLEMENTED;
    case ADBC_STATUS_NOT_FOUND:
      return GADBC_ERROR_NOT_FOUND;
    case ADBC_STATUS_ALREADY_EXISTS:
      return GADBC_ERROR_ALREADY_EXISTS;
    case ADBC_STATUS_INVALID_ARGUMENT:
      return GADBC_ERROR_INVALID_ARGUMENT;
    case ADBC_STATUS_INVALID_STATE:
      return GADBC_ERROR_INVALID_STATE;
    case ADBC_STATUS_INVALID_DATA:
      return GADBC_ERROR_INVALID_DATA;
    case ADBC_STATUS_INTEGRITY:
      return GADBC_ERROR_INTEGRITY;
    case ADBC_STATUS_INTERNAL:
      return GADBC_ERROR_INTERNAL;
    case ADBC_STATUS_IO:
      return GADBC_ERROR_IO;
    case ADBC_STATUS_CANCELLED:
      return GADBC_ERROR_CANCELLED;
    case ADBC_STATUS_TIMEOUT:
      return GADBC_ERROR_TIMEOUT;
    case ADBC_STATUS_UNAUTHENTICATED:
      return GADBC_ERROR_UNAUTHENTICATED;
    case ADBC_STATUS_UNAUTHORIZED:
      return GADBC_ERROR_UNAUTHORIZED;
    default:
      return GADBC_ERROR_UNKNOWN;
  }
}

gboolean gadbc_error_check(GError** error, AdbcStatusCode status_code,
                           struct AdbcError* adbc_error, const gchar* context) {
  if (status_code == ADBC_STATUS_OK) {
    return TRUE;
  } else {
    if (adbc_error && adbc_error->message) {
      g_set_error(error, GADBC_ERROR, gadbc_error_from_status_code(status_code),
                  "%s[%s][%d] %s", context, AdbcStatusCodeMessage(status_code),
                  adbc_error->vendor_code, adbc_error->message);
    } else {
      g_set_error(error, GADBC_ERROR, gadbc_error_from_status_code(status_code), "%s[%s]",
                  context, AdbcStatusCodeMessage(status_code));
    }
    if (adbc_error && adbc_error->release) {
      adbc_error->release(adbc_error);
    }
    return FALSE;
  }
}

void gadbc_error_warn(AdbcStatusCode status_code, struct AdbcError* adbc_error,
                      const gchar* context) {
  if (status_code == ADBC_STATUS_OK) {
    return;
  }
  g_warning("%s[%s][%d] %s", context, AdbcStatusCodeMessage(status_code),
            adbc_error->vendor_code, adbc_error->message);
  adbc_error->release(adbc_error);
}

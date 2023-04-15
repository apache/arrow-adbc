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

#include <adbc-glib/database.h>

G_BEGIN_DECLS

/**
 * GADBCInfo:
 * @GADBC_INFO_VENDOR_NAME: The database vendor/product name (e.g. the
 *   server name). (type: utf8).
 * @GADBC_INFO_VENDOR_VERSION: The database vendor/product library version
 *   (type: utf8).
 * @GADBC_INFO_VENDOR_ARROW_VERSION: The database vendor/product Arrow
 *   library version (type: utf8).
 * @GADBC_INFO_DRIVER_NAME: The driver name (type: utf8).
 * @GADBC_INFO_DRIVER_VERSION: The driver version (type: utf8).
 * @GADBC_INFO_DRIVER_ARROW_VERSION: The driver Arrow library version
 *   (type: utf8).
 *
 * The information code that is used by gadbc_connection_get_info().
 *
 * They are corresponding to `ADBC_INFO_*` values.
 *
 * Since: 0.4.0
 */
typedef enum {
  GADBC_INFO_VENDOR_NAME = 0,
  GADBC_INFO_VENDOR_VERSION = 1,
  GADBC_INFO_VENDOR_ARROW_VERSION = 2,
  GADBC_INFO_DRIVER_NAME = 100,
  GADBC_INFO_DRIVER_VERSION = 101,
  GADBC_INFO_DRIVER_ARROW_VERSION = 102,
} GADBCInfo;

#define GADBC_TYPE_CONNECTION (gadbc_connection_get_type())
G_DECLARE_DERIVABLE_TYPE(GADBCConnection, gadbc_connection, GADBC, CONNECTION, GObject)
struct _GADBCConnectionClass {
  GObjectClass parent_class;
};

GADBC_AVAILABLE_IN_0_1
GADBCConnection* gadbc_connection_new(GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_connection_release(GADBCConnection* connection, GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_connection_set_option(GADBCConnection* connection, const gchar* key,
                                     const gchar* value, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_connection_set_auto_commit(GADBCConnection* connection,
                                          gboolean auto_commit, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_connection_set_read_only(GADBCConnection* connection, gboolean read_only,
                                        GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_connection_init(GADBCConnection* connection, GADBCDatabase* database,
                               GError** error);

GADBC_AVAILABLE_IN_0_4
gpointer gadbc_connection_get_info(GADBCConnection* connection, guint32* info_codes,
                                   gsize n_info_codes, GError** error);
GADBC_AVAILABLE_IN_0_4
gpointer gadbc_connection_get_table_schema(GADBCConnection* connection,
                                           const gchar* catalog, const gchar* db_schema,
                                           const gchar* table_name, GError** error);
GADBC_AVAILABLE_IN_0_4
gpointer gadbc_connection_get_table_types(GADBCConnection* connection, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_connection_commit(GADBCConnection* connection, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_connection_rollback(GADBCConnection* connection, GError** error);

G_END_DECLS

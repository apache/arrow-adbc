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
 * They are corresponding to `ADBC_INFO_*` values in `adbc.h`.
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

/**
 * GADBCIsolationLevel:
 * @GADBC_ISOLATION_LEVEL_DEFAULT: Use database or driver default
 *   isolation level
 * @GADBC_ISOLATION_LEVEL_READ_UNCOMMITTED: The lowest isolation
 *   level. Dirty reads are allowed, so one transaction may see
 *   not-yet-committed changes made by others.
 * @GADBC_ISOLATION_LEVEL_READ_COMMITTED: Lock-based concurrency
 *   control keeps write locks until the end of the transaction, but
 *   read locks are released as soon as a SELECT is
 *   performed. Non-repeatable reads can occur in this isolation
 *   level.
 *   More simply put, Read Committed is an isolation level that
 *   guarantees that any data read is committed at the moment it is
 *   read. It simply restricts the reader from seeing any
 *   intermediate, uncommitted, 'dirty' reads. It makes no promise
 *   whatsoever that if the transaction re-issues the read, it will
 *   find the same data; data is free to change after it is read.
 * @GADBC_ISOLATION_LEVEL_REPEATABLE_READ: Lock-based concurrency
 *   control keeps read AND write locks (acquired on selection data)
 *   until the end of the transaction.
 *   However, range-locks are not managed, so phantom reads can occur.
 *   Write skew is possible at this isolation level in some systems.
 * @GADBC_ISOLATION_LEVEL_SNAPSHOT: This isolation guarantees that all
 *   reads in the transaction will see a consistent snapshot of the
 *   database and the transaction should only successfully commit if
 *   no updates conflict with any concurrent updates made since that
 *   snapshot.
 * @GADBC_ISOLATION_LEVEL_SERIALIZABLE: Serializability requires read
 *   and write locks to be released only at the end of the
 *   transaction. This includes acquiring range- locks when a select
 *   query uses a ranged WHERE clause to avoid phantom reads.
 * @GADBC_ISOLATION_LEVEL_LINEARIZABLE: The central distinction
 *   between serializability and linearizability is that
 *   serializability is a global property; a property of an entire
 *   history of operations and transactions. Linearizability is a
 *   local property; a property of a single operation/transaction.
 *
 *   Linearizability can be viewed as a special case of strict
 *   serializability where transactions are restricted to consist of a
 *   single operation applied to a single object.
 *
 * The isolation levels that are used by
 * gadbc_connection_set_isolation_level().
 *
 * They are corresponding to `ADBC_OPTION_ISOLATION_LEVEL_*` values in
 * `adbc.h`.
 *
 * Since: 0.4.0
 */
typedef enum {
  GADBC_ISOLATION_LEVEL_DEFAULT,
  GADBC_ISOLATION_LEVEL_READ_UNCOMMITTED,
  GADBC_ISOLATION_LEVEL_READ_COMMITTED,
  GADBC_ISOLATION_LEVEL_REPEATABLE_READ,
  GADBC_ISOLATION_LEVEL_SNAPSHOT,
  GADBC_ISOLATION_LEVEL_SERIALIZABLE,
  GADBC_ISOLATION_LEVEL_LINEARIZABLE,
} GADBCIsolationLevel;

GADBC_AVAILABLE_IN_0_4
const gchar* gadbc_isolation_level_to_string(GADBCIsolationLevel level);

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
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_connection_set_isolation_level(GADBCConnection* connection,
                                              GADBCIsolationLevel level, GError** error);
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

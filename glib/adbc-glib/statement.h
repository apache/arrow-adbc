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

#include <adbc-glib/connection.h>

G_BEGIN_DECLS

/**
 * GADBCIngestMode:
 * @GADBC_INGEST_MODE_CREATE: Create the table and insert data;
 *   error if the table exists.
 * @GADBC_INGEST_MODE_APPEND: Do not create the table, and insert
 *   data; error if the table does not exist (%GADBC_ERROR_NOT_FOUND)
 *   or does not match the schema of the data to append
 *   (%GADBC_ERROR_ALREADY_EXISTS).
 *
 * Whether to create (the default) or append on bulk insert.
 *
 * Since: 0.4.0
 */
typedef enum {
  GADBC_INGEST_MODE_CREATE,
  GADBC_INGEST_MODE_APPEND,
} GADBCIngestMode;

#define GADBC_TYPE_STATEMENT (gadbc_statement_get_type())
G_DECLARE_DERIVABLE_TYPE(GADBCStatement, gadbc_statement, GADBC, STATEMENT, GObject)
struct _GADBCStatementClass {
  GObjectClass parent_class;
};

GADBC_AVAILABLE_IN_0_10
gboolean gadbc_statement_initialize(GADBCStatement* statement,
                                    GADBCConnection* connection, const gchar* context,
                                    GError** error);
GADBC_AVAILABLE_IN_0_1
GADBCStatement* gadbc_statement_new(GADBCConnection* connection, GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_statement_release(GADBCStatement* statement, GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_statement_set_sql_query(GADBCStatement* statement, const gchar* query,
                                       GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_statement_set_option(GADBCStatement* statement, const gchar* key,
                                    const gchar* value, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_statement_set_ingest_target_table(GADBCStatement* statement,
                                                 const gchar* table, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_statement_set_ingest_mode(GADBCStatement* statement, GADBCIngestMode mode,
                                         GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_statement_prepare(GADBCStatement* statement, GError** error);
GADBC_AVAILABLE_IN_1_8
gboolean gadbc_statement_get_parameter_schema(GADBCStatement* statement,
                                              gpointer* c_abi_schema, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_statement_bind(GADBCStatement* statement, gpointer c_abi_array,
                              gpointer c_abi_schema, GError** error);
GADBC_AVAILABLE_IN_0_4
gboolean gadbc_statement_bind_stream(GADBCStatement* statement,
                                     gpointer c_abi_array_stream, GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_statement_execute(GADBCStatement* statement, gboolean need_result,
                                 gpointer* c_abi_array_stream, gint64* n_rows_affected,
                                 GError** error);

G_END_DECLS

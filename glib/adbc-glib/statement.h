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

#define GADBC_TYPE_STATEMENT (gadbc_statement_get_type())
G_DECLARE_DERIVABLE_TYPE(GADBCStatement, gadbc_statement, GADBC, STATEMENT, GObject)
struct _GADBCStatementClass {
  GObjectClass parent_class;
};

GADBC_AVAILABLE_IN_1_0
GADBCStatement* gadbc_statement_new(GADBCConnection* connection, GError** error);
GADBC_AVAILABLE_IN_1_0
gboolean gadbc_statement_release(GADBCStatement* statement, GError** error);
GADBC_AVAILABLE_IN_1_0
gboolean gadbc_statement_set_sql_query(GADBCStatement* statement, const gchar* query,
                                       GError** error);
GADBC_AVAILABLE_IN_1_0
gboolean gadbc_statement_execute(GADBCStatement* statement, gpointer* c_abi_array_stream,
                                 gint64* n_rows_affected, GError** error);

G_END_DECLS

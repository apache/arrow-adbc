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

#include <adbc-glib/adbc-glib.h>
#include <arrow-glib/arrow-glib.h>

#include <adbc-arrow-glib/version.h>

G_BEGIN_DECLS

#define GADBC_ARROW_TYPE_CONNECTION (gadbc_arrow_connection_get_type())
G_DECLARE_DERIVABLE_TYPE(GADBCArrowConnection, gadbc_arrow_connection, GADBCArrow,
                         CONNECTION, GADBCConnection)
struct _GADBCArrowConnectionClass {
  GADBCConnectionClass parent_class;
};

GADBC_ARROW_AVAILABLE_IN_1_0
GADBCArrowConnection* gadbc_arrow_connection_new(GError** error);
GADBC_ARROW_AVAILABLE_IN_1_0
GArrowRecordBatchReader* gadbc_arrow_connection_get_info(GADBCArrowConnection* connection,
                                                         guint32* info_codes,
                                                         gsize n_info_codes,
                                                         GError** error);
GADBC_ARROW_AVAILABLE_IN_1_0
GArrowRecordBatchReader* gadbc_arrow_connection_get_objects(
    GADBCArrowConnection* connection, GADBCObjectDepth depth, const gchar* catalog,
    const gchar* db_schema, const gchar* table_name, const gchar** table_types,
    const gchar* column_name, GError** error);
GADBC_ARROW_AVAILABLE_IN_1_0
GArrowSchema* gadbc_arrow_connection_get_table_schema(GADBCArrowConnection* connection,
                                                      const gchar* catalog,
                                                      const gchar* db_schema,
                                                      const gchar* table_name,
                                                      GError** error);
GADBC_ARROW_AVAILABLE_IN_1_0
GArrowRecordBatchReader* gadbc_arrow_connection_get_table_types(
    GADBCArrowConnection* connection, GError** error);
GADBC_ARROW_AVAILABLE_IN_1_0
GArrowRecordBatchReader* gadbc_arrow_connection_get_statistics(
    GADBCArrowConnection* connection, const gchar* catalog, const gchar* db_schema,
    const gchar* table_name, gboolean approximate, GError** error);
GADBC_ARROW_AVAILABLE_IN_1_0
GArrowRecordBatchReader* gadbc_arrow_connection_get_statistic_names(
    GADBCArrowConnection* connection, GError** error);

G_END_DECLS

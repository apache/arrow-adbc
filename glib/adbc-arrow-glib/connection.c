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

#include <adbc-arrow-glib/connection.h>

/**
 * SECTION: connection
 * @title: GADBCArrowStatement
 * @include: adbc-arrow-glib/adbc-arrow-glib.h
 *
 * #GADBCArrowConnection is a class for Arrow GLib integrated
 * connection.
 */

G_DEFINE_TYPE(GADBCArrowConnection, gadbc_arrow_connection, GADBC_TYPE_CONNECTION)

static void gadbc_arrow_connection_init(GADBCArrowConnection* connection) {}

static void gadbc_arrow_connection_class_init(GADBCArrowConnectionClass* klass) {}

/**
 * gadbc_arrow_connection_new:
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GADBCArrowConnection.
 *
 * Since: 1.0.0
 */
GADBCArrowConnection* gadbc_arrow_connection_new(GError** error) {
  GADBCArrowConnection* connection = g_object_new(GADBC_ARROW_TYPE_CONNECTION, NULL);
  if (gadbc_connection_initialize(GADBC_CONNECTION(connection),
                                  "[adbc-arrow][connection][new]", error)) {
    return connection;
  } else {
    g_object_unref(connection);
    return NULL;
  }
}

static GArrowRecordBatchReader* gadbc_arrow_import_array_stream(
    gpointer c_abi_array_stream, GError** error) {
  if (!c_abi_array_stream) {
    return NULL;
  }
  GArrowRecordBatchReader* reader =
      garrow_record_batch_reader_import(c_abi_array_stream, error);
  g_free(c_abi_array_stream);
  return reader;
}

static GArrowSchema* gadbc_arrow_import_schema(gpointer c_abi_schema, GError** error) {
  if (!c_abi_schema) {
    return NULL;
  }
  GArrowSchema* schema = garrow_schema_import(c_abi_schema, error);
  g_free(c_abi_schema);
  return schema;
}

/**
 * gadbc_arrow_connection_get_info:
 * @connection: A #GADBCArrowConnection.
 * @info_codes: (nullable) (array length=n_info_codes): A list of
 *   metadata codes to fetch, or %NULL to fetch all.
 * @n_info_codes: The length of the info_codes parameter. Ignored if
 *   info_codes is %NULL.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a wrapper of gadbc_arrow_connection_get_info().
 *
 * Returns: (transfer full) (nullable): The result set as
 *   #GArrowRecordBatchReader on success, %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchReader* gadbc_arrow_connection_get_info(GADBCArrowConnection* connection,
                                                         guint32* info_codes,
                                                         gsize n_info_codes,
                                                         GError** error) {
  gpointer c_abi_array_stream = gadbc_connection_get_info(
      GADBC_CONNECTION(connection), info_codes, n_info_codes, error);
  return gadbc_arrow_import_array_stream(c_abi_array_stream, error);
}

/**
 * gadbc_arrow_connection_get_objects:
 * @connection: A #GADBCArrowConnection.
 * @depth: The level of nesting to display. If
 *   @GADBC_OBJECT_DEPTH_ALL, display all levels. If
 *   @GADBC_OBJECT_DEPTH_CATALOGS, display only catalogs
 *   (i.e. `catalog_schemas` will be null). If
 *   @GADBC_OBJECT_DEPTH_DB_SCHEMAS, display only catalogs and schemas
 *   (i.e. `db_schema_tables` will be null). if
 *   @GADBC_OBJECT_DEPTH_TABLES, display only catalogs, schemas and
 *   tables (i.e. `table_columns` and `table_constraints` will be
 *   null).
 * @catalog: (nullable): Only show tables in the given catalog. If
 *   %NULL, do not filter by catalog. If an empty string, only show
 *   tables without a catalog. May be a search pattern (see section
 *   documentation).
 * @db_schema: (nullable): Only show tables in the given database
 *   schema. If %NULL, do not filter by database schema. If an empty
 *   string, only show tables without a database schema. May be a
 *   search pattern (see section documentation).
 * @table_name: (nullable): Only show tables with the given name. If
 *   %NULL, do not filter by name. May be a search pattern (see
 *   section documentation).
 * @table_types: (nullable) (array zero-terminated=1): Only show
 *   tables matching one of the given table types. If %NULL, show
 *   tables of any type. Valid table types can be fetched from
 *   gadbc_connection_get_table_types(). Terminate the list with a
 *   %NULL entry.
 * @column_name: (nullable): Only show columns with the given name. If
 *   %NULL, do not filter by name. May be a search pattern (see section
 *   documentation).
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a wrapper of gadbc_connection_get_objects().
 *
 * Returns: (transfer full) (nullable): The result set as
 *   #GArrowRecordBatchReader on success, %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchReader* gadbc_arrow_connection_get_objects(
    GADBCArrowConnection* connection, GADBCObjectDepth depth, const gchar* catalog,
    const gchar* db_schema, const gchar* table_name, const gchar** table_types,
    const gchar* column_name, GError** error) {
  gpointer c_abi_array_stream = gadbc_connection_get_objects(
      GADBC_CONNECTION(connection), depth, catalog, db_schema, table_name, table_types,
      column_name, error);
  return gadbc_arrow_import_array_stream(c_abi_array_stream, error);
}

/**
 * gadbc_arrow_connection_get_table_schema:
 * @connection: A #GADBCArrowConnection.
 * @catalog: (nullable): A catalog or %NULL if not applicable.
 * @db_schema: (nullable): A database schema or %NULL if not applicable.
 * @table_name: A table name.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a wrapper of gadbc_connection_get_table_schema().
 *
 * Returns: (transfer full) (nullable): The result set as
 *   #GArrowSchema on success, %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowSchema* gadbc_arrow_connection_get_table_schema(GADBCArrowConnection* connection,
                                                      const gchar* catalog,
                                                      const gchar* db_schema,
                                                      const gchar* table_name,
                                                      GError** error) {
  gpointer c_abi_schema = gadbc_connection_get_table_schema(
      GADBC_CONNECTION(connection), catalog, db_schema, table_name, error);
  return gadbc_arrow_import_schema(c_abi_schema, error);
}

/**
 * gadbc_arrow_connection_get_table_types:
 * @connection: A #GADBCArrowConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a wrapper of gadbc_connection_get_table_types().
 *
 * Returns: (transfer full) (nullable): The result set as
 *   #GArrowRecordBatchReader on success, %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchReader* gadbc_arrow_connection_get_table_types(
    GADBCArrowConnection* connection, GError** error) {
  gpointer c_abi_array_stream =
      gadbc_connection_get_table_types(GADBC_CONNECTION(connection), error);
  return gadbc_arrow_import_array_stream(c_abi_array_stream, error);
}

/**
 * gadbc_arrow_connection_get_statistics:
 * @connection: A #GADBCArrowConnection.
 * @catalog: (nullable): A catalog or %NULL if not applicable.
 * @db_schema: (nullable): A database schema or %NULL if not applicable.
 * @table_name: (nullable): A table name.
 * @approximate: Whether approximate values are allowed or not. If
 *   this is %TRUE, best-effort, approximate or cached values may be
 *   returned. Otherwise, exact values are requested. Note that the
 *   database may return approximate values regardless as indicated
 *   in the result. Request exact values may be expensive or
 *   unsupported.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a wrapper of gadbc_arrow_connection_get_statistics().
 *
 * Returns: (transfer full) (nullable): The result set as
 *   #GArrowRecordBatchReader on success, %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchReader* gadbc_arrow_connection_get_statistics(
    GADBCArrowConnection* connection, const gchar* catalog, const gchar* db_schema,
    const gchar* table_name, gboolean approximate, GError** error) {
  gpointer c_abi_array_stream = gadbc_connection_get_statistics(
      GADBC_CONNECTION(connection), catalog, db_schema, table_name, approximate, error);
  return gadbc_arrow_import_array_stream(c_abi_array_stream, error);
}

/**
 * gadbc_arrow_connection_get_statistic_names:
 * @connection: A #GADBCArrowConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a wrapper of gadbc_connection_get_statistic_names().
 *
 * Returns: (transfer full) (nullable): The result set as
 *   #GArrowRecordBatchReader on success, %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowRecordBatchReader* gadbc_arrow_connection_get_statistic_names(
    GADBCArrowConnection* connection, GError** error) {
  gpointer c_abi_array_stream =
      gadbc_connection_get_statistic_names(GADBC_CONNECTION(connection), error);
  return gadbc_arrow_import_array_stream(c_abi_array_stream, error);
}

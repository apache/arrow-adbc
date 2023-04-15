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

/* This is needless. This is just for cpplint. */
#include <adbc-glib/connection.h>

#include <adbc-glib/connection-raw.h>
#include <adbc-glib/database-raw.h>
#include <adbc-glib/error-raw.h>

/**
 * SECTION: connection
 * @title: GADBCConnection
 * @include: adbc-glib/adbc-glib.h
 *
 * #GADBCConnection is a class for connection.
 */

#define BOOLEAN_TO_OPTION_VALUE(boolean) \
  ((boolean) ? ADBC_OPTION_VALUE_ENABLED : ADBC_OPTION_VALUE_DISABLED)

/**
 * gadbc_isolation_level_to_string:
 * @level: A #GADBCIsolationLevel.
 *
 * Returns: The string representation of @level.
 *
 * Since: 0.4.0
 */
const gchar* gadbc_isolation_level_to_string(GADBCIsolationLevel level) {
  switch (level) {
    case GADBC_ISOLATION_LEVEL_DEFAULT:
      return ADBC_OPTION_ISOLATION_LEVEL_DEFAULT;
    case GADBC_ISOLATION_LEVEL_READ_UNCOMMITTED:
      return ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED;
    case GADBC_ISOLATION_LEVEL_READ_COMMITTED:
      return ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED;
    case GADBC_ISOLATION_LEVEL_REPEATABLE_READ:
      return ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ;
    case GADBC_ISOLATION_LEVEL_SNAPSHOT:
      return ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT;
    case GADBC_ISOLATION_LEVEL_SERIALIZABLE:
      return ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE;
    case GADBC_ISOLATION_LEVEL_LINEARIZABLE:
      return ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE;
    default:
      return "adbc.connection.transaction.isolation.invalid";
  }
}

typedef struct {
  gboolean initialized;
  struct AdbcConnection adbc_connection;
  GADBCDatabase* database;
} GADBCConnectionPrivate;

#define gadbc_connection_init gadbc_connection_init_
G_DEFINE_TYPE_WITH_PRIVATE(GADBCConnection, gadbc_connection, G_TYPE_OBJECT)
#undef gadbc_connection_init

static void gadbc_connection_dispose(GObject* object) {
  GADBCConnectionPrivate* priv =
      gadbc_connection_get_instance_private(GADBC_CONNECTION(object));
  if (priv->initialized) {
    struct AdbcError adbc_error = {};
    AdbcStatusCode status_code =
        AdbcConnectionRelease(&(priv->adbc_connection), &adbc_error);
    gadbc_error_warn(status_code, &adbc_error, "[adbc][connection][finalize]");
  }
  if (priv->database) {
    g_object_unref(priv->database);
    priv->database = NULL;
  }
  G_OBJECT_CLASS(gadbc_connection_parent_class)->dispose(object);
}

static void gadbc_connection_init_(GADBCConnection* connection) {}

static void gadbc_connection_class_init(GADBCConnectionClass* klass) {
  GObjectClass* gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = gadbc_connection_dispose;
}

/**
 * gadbc_connection_new:
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GADBCConnection.
 *
 * Since: 0.1.0
 */
GADBCConnection* gadbc_connection_new(GError** error) {
  GADBCConnection* connection = g_object_new(GADBC_TYPE_CONNECTION, NULL);
  GADBCConnectionPrivate* priv = gadbc_connection_get_instance_private(connection);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcConnectionNew(&(priv->adbc_connection), &adbc_error);
  priv->initialized =
      gadbc_error_check(error, status_code, &adbc_error, "[adbc][connection][new]");
  if (!priv->initialized) {
    g_object_unref(connection);
    return NULL;
  }
  return connection;
}

/**
 * gadbc_connection_release:
 * @connection: A #GADBCConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Release this connection explicitly. Normally, you don't need to call
 * this explicitly. If this connection is freed by g_object_unref(),
 * this connection is released automatically.
 *
 * You can't use this connection anymore after you call this.
 *
 * Returns: %TRUE if this connection is released successfully, %FALSE otherwise.
 *
 * Since: 0.1.0
 */
gboolean gadbc_connection_release(GADBCConnection* connection, GError** error) {
  const gchar* context = "[adbc][connection][release]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  if (!adbc_connection) {
    return FALSE;
  }
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcConnectionRelease(adbc_connection, &adbc_error);
  gboolean success = gadbc_error_check(error, status_code, &adbc_error, context);
  if (success) {
    GADBCConnectionPrivate* priv = gadbc_connection_get_instance_private(connection);
    priv->initialized = FALSE;
  }
  return success;
}

/**
 * gadbc_connection_set_option:
 * @connection: A #GADBCConnection.
 * @key: An option key.
 * @value: An option value.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Options may be set before gadbc_connection_init(). Some drivers may
 * support setting options after initialization as well.
 *
 * Returns: %TRUE if option is set successfully, %FALSE otherwise.
 *
 * Since: 0.1.0
 */
gboolean gadbc_connection_set_option(GADBCConnection* connection, const gchar* key,
                                     const gchar* value, GError** error) {
  const gchar* context = "[adbc][connection][set-option]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  if (!adbc_connection) {
    return FALSE;
  }
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcConnectionSetOption(adbc_connection, key, value, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error, context);
}

/**
 * gadbc_connection_set_auto_commit:
 * @connection: A #GADBCConnection.
 * @auto_commit: Whether auto commit is enabled or not.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE if this is set successfully, %FALSE otherwise.
 *
 * Since: 0.4.0
 */
gboolean gadbc_connection_set_auto_commit(GADBCConnection* connection,
                                          gboolean auto_commit, GError** error) {
  return gadbc_connection_set_option(connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                     BOOLEAN_TO_OPTION_VALUE(auto_commit), error);
}

/**
 * gadbc_connection_set_read_only:
 * @connection: A #GADBCConnection.
 * @read_only: Whether read only or not.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE if this is set successfully, %FALSE otherwise.
 *
 * Since: 0.4.0
 */
gboolean gadbc_connection_set_read_only(GADBCConnection* connection, gboolean read_only,
                                        GError** error) {
  return gadbc_connection_set_option(connection, ADBC_CONNECTION_OPTION_READ_ONLY,
                                     BOOLEAN_TO_OPTION_VALUE(read_only), error);
}

/**
 * gadbc_connection_set_isolation_level:
 * @connection: A #GADBCConnection.
 * @level: A #GADBCIsolationLevel to be used.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE if this is set successfully, %FALSE otherwise.
 *
 * Since: 0.4.0
 */
gboolean gadbc_connection_set_isolation_level(GADBCConnection* connection,
                                              GADBCIsolationLevel level, GError** error) {
  return gadbc_connection_set_option(connection, ADBC_CONNECTION_OPTION_ISOLATION_LEVEL,
                                     gadbc_isolation_level_to_string(level), error);
}

/**
 * gadbc_connection_init:
 * @connection: A #GADBCConnection.
 * @database: A #GADBCDatabase.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Finish setting options and initialize the connection.
 *
 * Some drivers may support setting options after initialization as
 * well.
 *
 * Returns: %TRUE if initialization is done successfully, %FALSE otherwise.
 *
 * Since: 0.1.0
 */
gboolean gadbc_connection_init(GADBCConnection* connection, GADBCDatabase* database,
                               GError** error) {
  const gchar* context = "[adbc][connection][init]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  if (!adbc_connection) {
    return FALSE;
  }
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database, context, error);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcConnectionInit(adbc_connection, adbc_database, &adbc_error);
  gboolean success = gadbc_error_check(error, status_code, &adbc_error, context);
  if (success) {
    GADBCConnectionPrivate* priv = gadbc_connection_get_instance_private(connection);
    priv->database = database;
    g_object_ref(priv->database);
  }
  return success;
}

/**
 * gadbc_connection_get_info:
 * @connection: A #GADBCConnection.
 * @info_codes: (nullable) (array length=n_info_codes): A list of
 *   metadata codes to fetch, or %NULL to fetch all.
 * @n_info_codes: The length of the info_codes parameter. Ignored if
 *   info_codes is %NULL.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * The result is an Arrow dataset with the following schema:
 *
 * Field Name                  | Field Type
 * ----------------------------|------------------------
 * info_name                   | uint32 not null
 * info_value                  | INFO_SCHEMA
 *
 * INFO_SCHEMA is a dense union with members:
 *
 * Field Name (Type Code)      | Field Type
 * ----------------------------|------------------------
 * string_value (0)            | utf8
 * bool_value (1)              | bool
 * int64_value (2)             | int64
 * int32_bitmask (3)           | int32
 * string_list (4)             | list<utf8>
 * int32_to_int32_list_map (5) | map<int32, list<int32>>
 *
 * Each metadatum is identified by an integer code.  The recognized
 * codes are defined as constants.  Codes [0, 10_000) are reserved
 * for ADBC usage.  Drivers/vendors will ignore requests for
 * unrecognized codes (the row will be omitted from the result).
 *
 * Returns: The result set as `struct ArrowArrayStream *`. It should
 *   be freed with the `ArrowArrayStream::release` callback then
 *   g_free() when no longer needed.
 *
 * Since: 0.4.0
 */
gpointer gadbc_connection_get_info(GADBCConnection* connection, guint32* info_codes,
                                   gsize n_info_codes, GError** error) {
  const gchar* context = "[adbc][connection][get-info]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  if (!adbc_connection) {
    return NULL;
  }
  struct ArrowArrayStream* array_stream = g_new0(struct ArrowArrayStream, 1);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcConnectionGetInfo(
      adbc_connection, info_codes, n_info_codes, array_stream, &adbc_error);
  if (gadbc_error_check(error, status_code, &adbc_error, context)) {
    return array_stream;
  } else {
    g_free(array_stream);
    return NULL;
  }
}

/**
 * gadbc_connection_get_table_schema:
 * @connection: A #GADBCConnection.
 * @catalog: (nullable): A catalog or %NULL if not applicable.
 * @db_schema: (nullable): A database schema or %NULL if not applicable.
 * @table_name: A table name.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get the Apache Arrow schema of a table.
 *
 * Returns: The result set as `struct ArrowSchema *`. It should
 *   be freed with the `ArrowSchema::release` callback then
 *   g_free() when no longer needed.
 *
 * Since: 0.4.0
 */
gpointer gadbc_connection_get_table_schema(GADBCConnection* connection,
                                           const gchar* catalog, const gchar* db_schema,
                                           const gchar* table_name, GError** error) {
  const gchar* context = "[adbc][connection][get-table-schema]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  if (!adbc_connection) {
    return NULL;
  }
  struct ArrowSchema* array_schema = g_new0(struct ArrowSchema, 1);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcConnectionGetTableSchema(
      adbc_connection, catalog, db_schema, table_name, array_schema, &adbc_error);
  if (gadbc_error_check(error, status_code, &adbc_error, context)) {
    return array_schema;
  } else {
    g_free(array_schema);
    return NULL;
  }
}

/**
 * gadbc_connection_get_table_types:
 * @connection: A #GADBCConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get a list of table types in the database.
 *
 * The result is an Arrow dataset with the following schema:
 *
 * Field Name     | Field Type
 * ---------------|--------------
 * table_type     | utf8 not null
 *
 *
 * Returns: The result set as `struct ArrowArrayStream *`. It should
 *   be freed with the `ArrowArrayStream::release` callback then
 *   g_free() when no longer needed.
 *
 * Since: 0.4.0
 */
gpointer gadbc_connection_get_table_types(GADBCConnection* connection, GError** error) {
  const gchar* context = "[adbc][connection][get-table-types]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  if (!adbc_connection) {
    return NULL;
  }
  struct ArrowArrayStream* array_stream = g_new0(struct ArrowArrayStream, 1);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcConnectionGetTableTypes(adbc_connection, array_stream, &adbc_error);
  if (gadbc_error_check(error, status_code, &adbc_error, context)) {
    return array_stream;
  } else {
    g_free(array_stream);
    return NULL;
  }
}

/**
 * gadbc_connection_commit:
 * @connection: A #GADBCConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Commit any pending transactions. Only used if auto commit is
 * disabled.
 *
 * Behavior is undefined if this is mixed with SQL transaction
 * statements.
 *
 * Returns: %TRUE if the commit is done successfully, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean gadbc_connection_commit(GADBCConnection* connection, GError** error) {
  const gchar* context = "[adbc][connection][commit]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  struct AdbcError adbc_error = {};
  return gadbc_error_check(error, AdbcConnectionCommit(adbc_connection, &adbc_error),
                           &adbc_error, context);
}

/**
 * gadbc_connection_rollback:
 * @connection: A #GADBCConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Rollback any pending transactions. Only used if auto commit is
 * disabled.
 *
 * Behavior is undefined if this is mixed with SQL transaction
 * statements.
 *
 * Returns: %TRUE if the rollback is done successfully, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean gadbc_connection_rollback(GADBCConnection* connection, GError** error) {
  const gchar* context = "[adbc][connection][rollback]";
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(connection, context, error);
  struct AdbcError adbc_error = {};
  return gadbc_error_check(error, AdbcConnectionRollback(adbc_connection, &adbc_error),
                           &adbc_error, context);
}

/**
 * gadbc_connection_get_raw:
 * @connection: A #GADBCConnection.
 * @context: (nullable): A context where this is called from. This is used in
 *   error message.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): The underlying `AdbcConnection` if this connection
 *   isn't released yet, %NULL otherwise.
 *
 * Since: 0.1.0
 */
struct AdbcConnection* gadbc_connection_get_raw(GADBCConnection* connection,
                                                const gchar* context, GError** error) {
  GADBCConnectionPrivate* priv = gadbc_connection_get_instance_private(connection);
  if (priv->initialized) {
    return &(priv->adbc_connection);
  } else {
    g_set_error(error, GADBC_ERROR, GADBC_ERROR_INVALID_ARGUMENT,
                "%s connection is already released", context);
    return NULL;
  }
}

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
#include <adbc-glib/statement.h>

#include <adbc-glib/connection-raw.h>
#include <adbc-glib/error-raw.h>
#include <adbc-glib/statement-raw.h>

/**
 * SECTION: statement
 * @title: GADBCStatement
 * @include: adbc-glib/adbc-glib.h
 *
 * #GADBCStatement is a class for statement.
 */

typedef struct {
  struct AdbcStatement adbc_statement;
  GADBCConnection* connection;
} GADBCStatementPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GADBCStatement, gadbc_statement, G_TYPE_OBJECT)

static void gadbc_statement_dispose(GObject* object) {
  GADBCStatementPrivate* priv =
      gadbc_statement_get_instance_private(GADBC_STATEMENT(object));
  if (priv->connection) {
    g_object_unref(priv->connection);
    priv->connection = NULL;
  }
  G_OBJECT_CLASS(gadbc_statement_parent_class)->dispose(object);
}

static void gadbc_statement_finalize(GObject* object) {
  struct AdbcStatement* adbc_statement = gadbc_statement_get_raw(GADBC_STATEMENT(object));
  AdbcStatementRelease(adbc_statement, NULL);
  G_OBJECT_CLASS(gadbc_statement_parent_class)->finalize(object);
}

static void gadbc_statement_init(GADBCStatement* statement) {}

static void gadbc_statement_class_init(GADBCStatementClass* klass) {
  GObjectClass* gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = gadbc_statement_dispose;
  gobject_class->finalize = gadbc_statement_finalize;
}

/**
 * gadbc_statement_new:
 * @connection: A #GADBCConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GADBCStatement for @connection.
 *
 * Since: 1.0.0
 */
GADBCStatement* gadbc_statement_new(GADBCConnection* connection, GError** error) {
  GADBCStatement* statement = g_object_new(GADBC_TYPE_STATEMENT, NULL);
  struct AdbcStatement* adbc_statement = gadbc_statement_get_raw(statement);
  struct AdbcConnection* adbc_connection = gadbc_connection_get_raw(connection);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcStatementNew(adbc_connection, adbc_statement, &adbc_error);
  gboolean success =
      gadbc_error_check(error, status_code, &adbc_error, "[adbc][statement][new]");
  if (success) {
    GADBCStatementPrivate* priv = gadbc_statement_get_instance_private(statement);
    priv->connection = connection;
    g_object_ref(priv->connection);
  } else {
    g_object_unref(statement);
    statement = NULL;
  }
  return statement;
}

/**
 * gadbc_statement_set_sql_query:
 * @statement: A #GADBCStatement.
 * @query: A query to execute.
 * @error: (out) (optional): Return location for a #GError or %NULL.
 *
 * Set the SQL query to execute.
 *
 * The query can then be executed with gadbc_statement_execute(). For
 * queries expected to be executed repeatedly,
 * gadbc_statement_prepare() the statement first.
 *
 * Returns: %TRUE if option is set successfully, %FALSE otherwise.
 *
 * Since: 1.0.0
 */
gboolean gadbc_statement_set_sql_query(GADBCStatement* statement, const gchar* query,
                                       GError** error) {
  struct AdbcStatement* adbc_statement = gadbc_statement_get_raw(statement);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcStatementSetSqlQuery(adbc_statement, query, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error,
                           "[adbc][statement][set-sql-query]");
}

/**
 * gadbc_statement_execute:
 * @statement: A #GADBCStatement.
 * @c_abi_array_stream: (out) (optional): Return location for the execution as
 *   `struct ArrowArrayStream *`. It should be freed with the
 *   `ArrowArrayStream::release` callback then g_free() when no longer needed.
 * @n_rows_affected: (out) (optional): Return location for the number of rows
 *   affected if known.
 * @error: (out) (optional): Return location for a #GError or %NULL.
 *
 * Execute a statement and get the results.
 *
 * This invalidates any prior result sets.
 *
 * Returns: %TRUE if option is set successfully, %FALSE otherwise.
 *
 * Since: 1.0.0
 */
gboolean gadbc_statement_execute(GADBCStatement* statement, gpointer* c_abi_array_stream,
                                 gint64* n_rows_affected, GError** error) {
  struct AdbcStatement* adbc_statement = gadbc_statement_get_raw(statement);
  struct ArrowArrayStream* array_stream = NULL;
  if (c_abi_array_stream) {
    array_stream = g_new0(struct ArrowArrayStream, 1);
  }
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcStatementExecuteQuery(adbc_statement, array_stream,
                                                         n_rows_affected, &adbc_error);
  gboolean success =
      gadbc_error_check(error, status_code, &adbc_error, "[adbc][statement][execute]");
  if (success) {
    *c_abi_array_stream = array_stream;
  } else {
    if (array_stream) {
      g_free(array_stream);
    }
  }
  return success;
}

struct AdbcStatement* gadbc_statement_get_raw(GADBCStatement* statement) {
  GADBCStatementPrivate* priv = gadbc_statement_get_instance_private(statement);
  return &(priv->adbc_statement);
}

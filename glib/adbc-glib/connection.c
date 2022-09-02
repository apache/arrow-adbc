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

typedef struct {
  struct AdbcConnection adbc_connection;
  GADBCDatabase* database;
} GADBCConnectionPrivate;

#define gadbc_connection_init gadbc_connection_init_
G_DEFINE_TYPE_WITH_PRIVATE(GADBCConnection, gadbc_connection, G_TYPE_OBJECT)
#undef gadbc_connection_init

static void gadbc_connection_dispose(GObject* object) {
  GADBCConnectionPrivate* priv =
      gadbc_connection_get_instance_private(GADBC_CONNECTION(object));
  if (priv->database) {
    g_object_unref(priv->database);
    priv->database = NULL;
  }
  G_OBJECT_CLASS(gadbc_connection_parent_class)->dispose(object);
}

static void gadbc_connection_finalize(GObject* object) {
  struct AdbcConnection* adbc_connection =
      gadbc_connection_get_raw(GADBC_CONNECTION(object));
  AdbcConnectionRelease(adbc_connection, NULL);
  G_OBJECT_CLASS(gadbc_connection_parent_class)->finalize(object);
}

static void gadbc_connection_init_(GADBCConnection* connection) {
  struct AdbcConnection* adbc_connection = gadbc_connection_get_raw(connection);
  AdbcConnectionNew(adbc_connection, NULL);
}

static void gadbc_connection_class_init(GADBCConnectionClass* klass) {
  GObjectClass* gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = gadbc_connection_dispose;
  gobject_class->finalize = gadbc_connection_finalize;
}

/**
 * gadbc_connection_new:
 *
 * Returns: A newly created #GADBCConnection.
 *
 * Since: 1.0.0
 */
GADBCConnection* gadbc_connection_new(void) {
  return g_object_new(GADBC_TYPE_CONNECTION, NULL);
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
 * Since: 1.0.0
 */
gboolean gadbc_connection_set_option(GADBCConnection* connection, const gchar* key,
                                     const gchar* value, GError** error) {
  struct AdbcConnection* adbc_connection = gadbc_connection_get_raw(connection);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcConnectionSetOption(adbc_connection, key, value, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error,
                           "[adbc][connection][set-option]");
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
 * Since: 1.0.0
 */
gboolean gadbc_connection_init(GADBCConnection* connection, GADBCDatabase* database,
                               GError** error) {
  GADBCConnectionPrivate* priv = gadbc_connection_get_instance_private(connection);
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcConnectionInit(&(priv->adbc_connection), adbc_database, &adbc_error);
  gboolean success =
      gadbc_error_check(error, status_code, &adbc_error, "[adbc][connection][init]");
  if (success) {
    priv->database = database;
    g_object_ref(priv->database);
  }
  return success;
}

struct AdbcConnection* gadbc_connection_get_raw(GADBCConnection* connection) {
  GADBCConnectionPrivate* priv = gadbc_connection_get_instance_private(connection);
  return &(priv->adbc_connection);
}

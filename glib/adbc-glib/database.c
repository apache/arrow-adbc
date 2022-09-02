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
#include <adbc-glib/database.h>

#include <adbc-glib/database-raw.h>
#include <adbc-glib/error-raw.h>

/**
 * SECTION: database
 * @title: GADBCDatabase
 * @include: adbc-glib/adbc-glib.h
 *
 * #GADBCDatabase is a class for database.
 */

typedef struct {
  struct AdbcDatabase adbc_database;
} GADBCDatabasePrivate;

#define gadbc_database_init gadbc_database_init_
G_DEFINE_TYPE_WITH_PRIVATE(GADBCDatabase, gadbc_database, G_TYPE_OBJECT)
#undef gadbc_database_init

static void gadbc_database_finalize(GObject* object) {
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(GADBC_DATABASE(object));
  AdbcDatabaseRelease(adbc_database, NULL);
  G_OBJECT_CLASS(gadbc_database_parent_class)->finalize(object);
}

static void gadbc_database_init_(GADBCDatabase* database) {
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database);
  AdbcDatabaseNew(adbc_database, NULL);
}

static void gadbc_database_class_init(GADBCDatabaseClass* klass) {
  GObjectClass* gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = gadbc_database_finalize;
}

/**
 * gadbc_database_new:
 *
 * Returns: A newly created #GADBCDatabase.
 *
 * Since: 1.0.0
 */
GADBCDatabase* gadbc_database_new(void) {
  return g_object_new(GADBC_TYPE_DATABASE, NULL);
}

/**
 * gadbc_database_set_option:
 * @database: A #GADBCDatabase.
 * @key: An option key.
 * @value: An option value.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Options may be set before gadbc_database_init(). Some drivers may
 * support setting options after initialization as well.
 *
 * Returns: %TRUE if option is set successfully, %FALSE otherwise.
 *
 * Since: 1.0.0
 */
gboolean gadbc_database_set_option(GADBCDatabase* database, const gchar* key,
                                   const gchar* value, GError** error) {
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcDatabaseSetOption(adbc_database, key, value, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error,
                           "[adbc][database][set-option]");
}

/**
 * gadbc_database_init:
 * @database: A #GADBCDatabase.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Finish setting options and initialize the database.
 *
 * Some drivers may support setting options after initialization as
 * well.
 *
 * Returns: %TRUE if initialization is done successfully, %FALSE otherwise.
 *
 * Since: 1.0.0
 */
gboolean gadbc_database_init(GADBCDatabase* database, GError** error) {
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcDatabaseInit(adbc_database, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error, "[adbc][database][init]");
}

struct AdbcDatabase* gadbc_database_get_raw(GADBCDatabase* database) {
  GADBCDatabasePrivate* priv = gadbc_database_get_instance_private(database);
  return &(priv->adbc_database);
}

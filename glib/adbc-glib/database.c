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

#include <arrow-adbc/adbc_driver_manager.h>

/**
 * SECTION: database
 * @title: GADBCDatabase
 * @include: adbc-glib/adbc-glib.h
 *
 * #GADBCDatabase is a class for database.
 */

typedef struct {
  gboolean initialized;
  struct AdbcDatabase adbc_database;
} GADBCDatabasePrivate;

#define gadbc_database_init gadbc_database_init_
G_DEFINE_TYPE_WITH_PRIVATE(GADBCDatabase, gadbc_database, G_TYPE_OBJECT)
#undef gadbc_database_init

static void gadbc_database_finalize(GObject* object) {
  GADBCDatabasePrivate* priv =
      gadbc_database_get_instance_private(GADBC_DATABASE(object));
  if (priv->initialized) {
    struct AdbcError adbc_error = {};
    AdbcStatusCode status_code = AdbcDatabaseRelease(&(priv->adbc_database), &adbc_error);
    gadbc_error_warn(status_code, &adbc_error, "[adbc][database][finalize]");
  }
  G_OBJECT_CLASS(gadbc_database_parent_class)->finalize(object);
}

static void gadbc_database_init_(GADBCDatabase* database) {}

static void gadbc_database_class_init(GADBCDatabaseClass* klass) {
  GObjectClass* gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = gadbc_database_finalize;
}

/**
 * gadbc_database_new:
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GADBCDatabase.
 *
 * Since: 0.1.0
 */
GADBCDatabase* gadbc_database_new(GError** error) {
  GADBCDatabase* database = g_object_new(GADBC_TYPE_DATABASE, NULL);
  GADBCDatabasePrivate* priv = gadbc_database_get_instance_private(database);
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcDatabaseNew(&(priv->adbc_database), &adbc_error);
  priv->initialized =
      gadbc_error_check(error, status_code, &adbc_error, "[adbc][database][new]");
  if (!priv->initialized) {
    g_object_unref(database);
    return NULL;
  }
  return database;
}

/**
 * gadbc_database_release:
 * @database: A #GADBCDatabase.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Release this database explicitly. Normally, you don't need to call
 * this explicitly. If this database is freed by g_object_unref(),
 * this database is released automatically.
 *
 * You can't use this database anymore after you call this.
 *
 * Returns: %TRUE if this database is released successfully, %FALSE otherwise.
 *
 * Since: 0.1.0
 */
gboolean gadbc_database_release(GADBCDatabase* database, GError** error) {
  const gchar* context = "[adbc][database][release]";
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database, context, error);
  if (!adbc_database) {
    return FALSE;
  }
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcDatabaseRelease(adbc_database, &adbc_error);
  gboolean success = gadbc_error_check(error, status_code, &adbc_error, context);
  if (success) {
    GADBCDatabasePrivate* priv = gadbc_database_get_instance_private(database);
    priv->initialized = FALSE;
  }
  return success;
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
 * Since: 0.1.0
 */
gboolean gadbc_database_set_option(GADBCDatabase* database, const gchar* key,
                                   const gchar* value, GError** error) {
  const gchar* context = "[adbc][database][set-option]";
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database, context, error);
  if (!adbc_database) {
    return FALSE;
  }
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcDatabaseSetOption(adbc_database, key, value, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error, context);
}

/**
 * gadbc_database_set_load_flags:
 * @database: A #GADBCDatabase.
 * @flags: Flags to control the directories searched.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is an extension to the ADBC API. This function lets you set
 * the load flags explicitly, for applications that can dynamically
 * load drivers on their own.
 *
 * If this function isn't called, the default load flags are just to
 * allow relative paths, disallowing the lookups of manifests.
 *
 * Flags must be set before gadbc_database_init().
 *
 * Returns: %TRUE if flags are set successfully, %FALSE otherwise.
 *
 * Since: 1.7.0
 */
gboolean gadbc_database_set_load_flags(GADBCDatabase* database, GADBCLoadFlags flags,
                                       GError** error) {
  const gchar* context = "[adbc][database][set-load-flags]";
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database, context, error);
  if (!adbc_database) {
    return FALSE;
  }
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code =
      AdbcDriverManagerDatabaseSetLoadFlags(adbc_database, flags, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error, context);
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
 * Since: 0.1.0
 */
gboolean gadbc_database_init(GADBCDatabase* database, GError** error) {
  const gchar* context = "[adbc][database][init]";
  struct AdbcDatabase* adbc_database = gadbc_database_get_raw(database, context, error);
  if (!adbc_database) {
    return FALSE;
  }
  struct AdbcError adbc_error = {};
  AdbcStatusCode status_code = AdbcDatabaseInit(adbc_database, &adbc_error);
  return gadbc_error_check(error, status_code, &adbc_error, context);
}

/**
 * gadbc_database_get_raw:
 * @database: A #GADBCDatabase.
 * @context: (nullable): A context where this is called from. This is used in
 *   error message.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): The underlying `AdbcDatabase` if this database
 *   isn't released yet, %NULL otherwise.
 *
 * Since: 0.1.0
 */
struct AdbcDatabase* gadbc_database_get_raw(GADBCDatabase* database, const gchar* context,
                                            GError** error) {
  GADBCDatabasePrivate* priv = gadbc_database_get_instance_private(database);
  if (priv->initialized) {
    return &(priv->adbc_database);
  } else {
    g_set_error(error, GADBC_ERROR, GADBC_ERROR_INVALID_ARGUMENT,
                "%s database is already released", context);
    return NULL;
  }
}

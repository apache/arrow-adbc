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

#include <glib-object.h>

#include <adbc-glib/version.h>

G_BEGIN_DECLS

/**
 * GADBCLoadFlags:
 * @GADBC_LOAD_SEARCH_ENV: Search the directory paths in the environment
 * variable ADBC_DRIVER_PATH and (when built or installed with conda)
 * search in the conda environment
 * @GADBC_LOAD_SEARCH_USER: Search the user configuration directory.
 * @GADBC_LOAD_SEARCH_SYSTEM: Search the system configuration directory.
 * @GADBC_LOAD_ALLOW_RELATIVE_PATHS: Allow relative driver paths.
 *
 * The flags are used by gadbc_database_set_load_flags().
 *
 * They are corresponding to `ADBC_LOAD_FLAG_*` values.
 *
 * Since: 1.7.0
 */
typedef enum {
  GADBC_LOAD_SEARCH_ENV = 1 << 0,
  GADBC_LOAD_SEARCH_USER = 1 << 1,
  GADBC_LOAD_SEARCH_SYSTEM = 1 << 2,
  GADBC_LOAD_ALLOW_RELATIVE_PATHS = 1 << 3,
} GADBCLoadFlags;

/**
 * GADBC_LOAD_FLAGAS_DEFAULT:
 *
 * The default GADBCLoadFlags.
 *
 * Since: 1.7.0
 */
#define GADBC_LOAD_FLAGS_DEFAULT                                               \
  (GADBC_LOAD_SEARCH_ENV | GADBC_LOAD_SEARCH_USER | GADBC_LOAD_SEARCH_SYSTEM | \
   GADBC_LOAD_ALLOW_RELATIVE_PATHS)

#define GADBC_TYPE_DATABASE (gadbc_database_get_type())
G_DECLARE_DERIVABLE_TYPE(GADBCDatabase, gadbc_database, GADBC, DATABASE, GObject)
struct _GADBCDatabaseClass {
  GObjectClass parent_class;
};

GADBC_AVAILABLE_IN_0_1
GADBCDatabase* gadbc_database_new(GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_database_release(GADBCDatabase* database, GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_database_set_option(GADBCDatabase* database, const gchar* key,
                                   const gchar* value, GError** error);
GADBC_AVAILABLE_IN_1_7
gboolean gadbc_database_set_load_flags(GADBCDatabase* database, GADBCLoadFlags flags,
                                       GError** error);
GADBC_AVAILABLE_IN_0_1
gboolean gadbc_database_init(GADBCDatabase* database, GError** error);

G_END_DECLS

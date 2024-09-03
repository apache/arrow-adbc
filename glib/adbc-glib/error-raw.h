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

#include <arrow-adbc/adbc.h>

#include <adbc-glib/error.h>

G_BEGIN_DECLS

GADBC_AVAILABLE_IN_0_1
GADBCError gadbc_error_from_status_code(AdbcStatusCode status_code);

GADBC_AVAILABLE_IN_0_1
gboolean gadbc_error_check(GError** error, AdbcStatusCode status_code,
                           struct AdbcError* adbc_error, const gchar* context);
GADBC_AVAILABLE_IN_0_1
void gadbc_error_warn(AdbcStatusCode status_code, struct AdbcError* adbc_error,
                      const gchar* context);

G_END_DECLS

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// \file arrow-adbc/driver/db2.h ADBC IBM Db2 Driver
///
/// An ADBC driver for IBM Db2 LUW that wraps the DB2 CLI library.
///
/// \warning This driver is currently scoped to connection management
///   only (database/connection lifecycle, options, and authentication).
///   Statement execution, metadata APIs, transactions, and bulk
///   ingestion are not yet implemented and will be added in
///   subsequent pull requests.

#pragma once

#include <arrow-adbc/adbc.h>

/// \defgroup adbc-driver-db2 ADBC Db2 Driver
///
/// Connection-string options understood by the Db2 driver.  Set them on
/// the AdbcDatabase via \ref AdbcDatabaseSetOption.  The standard
/// ``"uri"`` option is also supported and, when present, takes priority
/// over the individual ``adbc.db2.*`` options.
///
/// @{

/// \brief The Db2 database (catalog) name.
#define ADBC_DB2_OPTION_DATABASE "adbc.db2.database"

/// \brief The Db2 server hostname.
#define ADBC_DB2_OPTION_HOSTNAME "adbc.db2.hostname"

/// \brief The Db2 server port (as a string, e.g. ``"50000"``).
#define ADBC_DB2_OPTION_PORT "adbc.db2.port"

/// \brief The Db2 user identifier.  The standard ``"username"`` option
///   is also accepted as a synonym.
#define ADBC_DB2_OPTION_UID "adbc.db2.uid"

/// \brief The Db2 password.  The standard ``"password"`` option is also
///   accepted as a synonym.
#define ADBC_DB2_OPTION_PWD "adbc.db2.pwd"

/// @}

#ifdef __cplusplus
extern "C" {
#endif

/// \brief Entrypoint for the Db2 driver.
ADBC_EXPORT
AdbcStatusCode AdbcDriverDb2Init(int version, void* raw_driver,
                                 struct AdbcError* error);

#ifdef __cplusplus
}
#endif

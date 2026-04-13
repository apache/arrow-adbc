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

/// \file arrow-adbc/driver/db2.h ADBC IBM DB2 Driver
///
/// A driver for IBM DB2 using DB2 CLI / ODBC.

#pragma once

#include <arrow-adbc/adbc.h>

/// \defgroup adbc-driver-db2 ADBC DB2 Driver Option Constants
/// @{

/// \brief Set the DB2 database name (used to build connection string).
#define ADBC_DB2_OPTION_DATABASE "adbc.db2.database"

/// \brief Set the DB2 hostname.
#define ADBC_DB2_OPTION_HOSTNAME "adbc.db2.hostname"

/// \brief Set the DB2 port.
#define ADBC_DB2_OPTION_PORT "adbc.db2.port"

/// \brief Set the DB2 user ID.
#define ADBC_DB2_OPTION_UID "adbc.db2.uid"

/// \brief Set the DB2 password.
#define ADBC_DB2_OPTION_PWD "adbc.db2.pwd"

/// \brief Set the number of rows per batch when fetching results.
///
/// Default: 65536.  Applies to AdbcStatement.
#define ADBC_DB2_OPTION_BATCH_ROWS "adbc.db2.query.batch_rows"

/// @}

#ifdef __cplusplus
extern "C" {
#endif

/// \brief Initialize the DB2 ADBC driver.
ADBC_EXPORT
AdbcStatusCode AdbcDriverDb2Init(int version, void* raw_driver,
                                 struct AdbcError* error);

#ifdef __cplusplus
}
#endif

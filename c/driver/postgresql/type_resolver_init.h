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

#pragma once

#include <libpq-fe.h>

#include "driver/framework/status.h"
#include "postgres_type.h"

namespace adbcpq {

enum class TypeResolverMode {
  // Only use the built-in type OID definitions
  kBuiltin,
  // Query the database up front
  kServer,
};

adbc::driver::Status RebuildTypeResolver(PGconn* conn, PostgresTypeResolver& resolver);
adbc::driver::Status InitializeTypeResolver(PostgresTypeResolver& resolver);

}  // namespace adbcpq

// exposed for testing

ADBC_EXPORT
adbc::driver::Status InternalAdbcRebuildTypeResolver(
    PGconn* conn, adbcpq::PostgresTypeResolver& resolver);

ADBC_EXPORT
adbc::driver::Status InternalAdbcInitializeTypeResolver(
    adbcpq::PostgresTypeResolver& resolver);

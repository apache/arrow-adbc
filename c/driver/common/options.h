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

/// Common options that haven't yet been formally standardized.
/// https://github.com/apache/arrow-adbc/issues/1055

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/// \brief The catalog of the table for bulk insert.
///
/// The type is char*.
#define ADBC_INGEST_OPTION_TARGET_CATALOG "adbc.ingest.target_catalog"

/// \brief The schema of the table for bulk insert.
///
/// The type is char*.
#define ADBC_INGEST_OPTION_TARGET_DB_SCHEMA "adbc.ingest.target_db_schema"

#ifdef __cplusplus
}
#endif

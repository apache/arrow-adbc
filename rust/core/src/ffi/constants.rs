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

use std::os::raw::c_int;

use super::types::FFI_AdbcStatusCode;

pub const ADBC_STATUS_OK: FFI_AdbcStatusCode = 0;
pub const ADBC_STATUS_UNKNOWN: FFI_AdbcStatusCode = 1;
pub const ADBC_STATUS_NOT_IMPLEMENTED: FFI_AdbcStatusCode = 2;
pub const ADBC_STATUS_NOT_FOUND: FFI_AdbcStatusCode = 3;
pub const ADBC_STATUS_ALREADY_EXISTS: FFI_AdbcStatusCode = 4;
pub const ADBC_STATUS_INVALID_ARGUMENT: FFI_AdbcStatusCode = 5;
pub const ADBC_STATUS_INVALID_STATE: FFI_AdbcStatusCode = 6;
pub const ADBC_STATUS_INVALID_DATA: FFI_AdbcStatusCode = 7;
pub const ADBC_STATUS_INTEGRITY: FFI_AdbcStatusCode = 8;
pub const ADBC_STATUS_INTERNAL: FFI_AdbcStatusCode = 9;
pub const ADBC_STATUS_IO: FFI_AdbcStatusCode = 10;
pub const ADBC_STATUS_CANCELLED: FFI_AdbcStatusCode = 11;
pub const ADBC_STATUS_TIMEOUT: FFI_AdbcStatusCode = 12;
pub const ADBC_STATUS_UNAUTHENTICATED: FFI_AdbcStatusCode = 13;
pub const ADBC_STATUS_UNAUTHORIZED: FFI_AdbcStatusCode = 14;

pub const ADBC_VERSION_1_0_0: c_int = 1_000_000;
pub const ADBC_VERSION_1_1_0: c_int = 1_001_000;

pub const ADBC_INFO_VENDOR_NAME: u32 = 0;
pub const ADBC_INFO_VENDOR_VERSION: u32 = 1;
pub const ADBC_INFO_VENDOR_ARROW_VERSION: u32 = 2;
pub const ADBC_INFO_DRIVER_NAME: u32 = 100;
pub const ADBC_INFO_DRIVER_VERSION: u32 = 101;
pub const ADBC_INFO_DRIVER_ARROW_VERSION: u32 = 102;
pub const ADBC_INFO_DRIVER_ADBC_VERSION: u32 = 103;

pub const ADBC_OBJECT_DEPTH_ALL: c_int = 0;
pub const ADBC_OBJECT_DEPTH_CATALOGS: c_int = 1;
pub const ADBC_OBJECT_DEPTH_DB_SCHEMAS: c_int = 2;
pub const ADBC_OBJECT_DEPTH_TABLES: c_int = 3;
pub const ADBC_OBJECT_DEPTH_COLUMNS: c_int = ADBC_OBJECT_DEPTH_ALL;

pub const ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA: i32 = i32::MIN;

pub const ADBC_INGEST_OPTION_TARGET_TABLE: &str = "adbc.ingest.target_table";
pub const ADBC_INGEST_OPTION_MODE: &str = "adbc.ingest.mode";

pub const ADBC_INGEST_OPTION_MODE_CREATE: &str = "adbc.ingest.mode.create";
pub const ADBC_INGEST_OPTION_MODE_APPEND: &str = "adbc.ingest.mode.append";
pub const ADBC_INGEST_OPTION_MODE_REPLACE: &str = "adbc.ingest.mode.replace";
pub const ADBC_INGEST_OPTION_MODE_CREATE_APPEND: &str = "adbc.ingest.mode.create_append";

pub const ADBC_OPTION_URI: &str = "uri";
pub const ADBC_OPTION_USERNAME: &str = "username";
pub const ADBC_OPTION_PASSWORD: &str = "password";

pub const ADBC_CONNECTION_OPTION_AUTOCOMMIT: &str = "adbc.connection.autocommit";
pub const ADBC_CONNECTION_OPTION_READ_ONLY: &str = "adbc.connection.readonly";
pub const ADBC_CONNECTION_OPTION_CURRENT_CATALOG: &str = "adbc.connection.catalog";
pub const ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA: &str = "adbc.connection.db_schema";
pub const ADBC_CONNECTION_OPTION_ISOLATION_LEVEL: &str =
    "adbc.connection.transaction.isolation_level";

pub const ADBC_STATEMENT_OPTION_INCREMENTAL: &str = "adbc.statement.exec.incremental";
pub const ADBC_STATEMENT_OPTION_PROGRESS: &str = "adbc.statement.exec.progress";
pub const ADBC_STATEMENT_OPTION_MAX_PROGRESS: &str = "adbc.statement.exec.max_progress";

pub const ADBC_OPTION_ISOLATION_LEVEL_DEFAULT: &str =
    "adbc.connection.transaction.isolation.default";
pub const ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED: &str =
    "adbc.connection.transaction.isolation.read_uncommitted";
pub const ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED: &str =
    "adbc.connection.transaction.isolation.read_committed";
pub const ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ: &str =
    "adbc.connection.transaction.isolation.repeatable_read";
pub const ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT: &str =
    "adbc.connection.transaction.isolation.snapshot";
pub const ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE: &str =
    "adbc.connection.transaction.isolation.serializable";
pub const ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE: &str =
    "adbc.connection.transaction.isolation.linearizable";

pub const ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY: i16 = 0;
pub const ADBC_STATISTIC_DISTINCT_COUNT_KEY: i16 = 1;
pub const ADBC_STATISTIC_MAX_BYTE_WIDTH_KEY: i16 = 2;
pub const ADBC_STATISTIC_MAX_VALUE_KEY: i16 = 3;
pub const ADBC_STATISTIC_MIN_VALUE_KEY: i16 = 4;
pub const ADBC_STATISTIC_NULL_COUNT_KEY: i16 = 5;
pub const ADBC_STATISTIC_ROW_COUNT_KEY: i16 = 6;

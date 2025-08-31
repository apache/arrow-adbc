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

#![allow(non_camel_case_types, non_snake_case)]

use std::os::raw::{c_char, c_int};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use adbc_core::{ffi::constants::ADBC_STATUS_NOT_IMPLEMENTED, error::AdbcStatusCode};

use super::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcError,
    FFI_AdbcErrorDetail, FFI_AdbcPartitions, FFI_AdbcStatement,
};

macro_rules! method {
    ($func_name:ident ; $type_name:ident ; $return_type:ty ; $return_value:expr ; $( $arg:ty ),*) => {
        #[allow(dead_code)]
        pub(crate) type $type_name = unsafe extern "C" fn($( $arg ),*) -> $return_type;
        #[allow(dead_code)]
        #[doc(hidden)]
        pub unsafe extern "C" fn $func_name($(_:$arg),*) -> $return_type {
            $return_value
        }
    };
}

method!(DatabaseInit ; FuncDatabaseInit ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(DatabaseNew ; FuncDatabaseNew ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(DatabaseSetOption ; FuncDatabaseSetOption ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *const c_char, *mut FFI_AdbcError);
method!(DatabaseRelease ; FuncDatabaseRelease ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(ConnectionCommit ; FuncConnectionCommit ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionGetInfo ; FuncConnectionGetInfo ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const u32, usize, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionGetObjects ; FuncConnectionGetObjects ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, c_int, *const c_char, *const c_char, *const c_char, *const *const c_char, *const c_char, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionGetTableSchema ; FuncConnectionGetTableSchema ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const c_char, *const c_char, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(ConnectionGetTableTypes ; FuncConnectionGetTableTypes ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionInit ; FuncConnectionInit ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(ConnectionNew ; FuncConnectionNew ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionSetOption ; FuncConnectionSetOption ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const c_char, *mut FFI_AdbcError);
method!(ConnectionReadPartition ; FuncConnectionReadPartition ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const u8, usize, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionRelease ; FuncConnectionRelease ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionRollback ; FuncConnectionRollback ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(StatementBind ; FuncStatementBind ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowArray, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(StatementBindStream ; FuncStatementBindStream ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(StatementExecuteQuery ; FuncStatementExecuteQuery ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowArrayStream, *mut i64, *mut FFI_AdbcError);
method!(StatementExecutePartitions ; FuncStatementExecutePartitions ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowSchema, *mut FFI_AdbcPartitions, *mut i64, *mut FFI_AdbcError);
method!(StatementGetParameterSchema ; FuncStatementGetParameterSchema ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(StatementNew ; FuncStatementNew ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementPrepare ; FuncStatementPrepare ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementRelease ; FuncStatementRelease ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementSetOption ; FuncStatementSetOption ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *const c_char, *mut FFI_AdbcError);
method!(StatementSetSqlQuery ; FuncStatementSetSqlQuery ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut FFI_AdbcError);
method!(StatementSetSubstraitPlan ; FuncStatementSetSubstraitPlan ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const u8, usize, *mut FFI_AdbcError);
method!(ErrorGetDetailCount ; FuncErrorGetDetailCount ; c_int ; 0 ; *const FFI_AdbcError);
method!(ErrorGetDetail ; FuncErrorGetDetail ; FFI_AdbcErrorDetail ; FFI_AdbcErrorDetail::default() ; *const FFI_AdbcError, c_int);
method!(_ErrorFromArrayStream ; FuncErrorFromArrayStream ; *const FFI_AdbcError ; std::ptr::null() ; *mut FFI_ArrowArrayStream, *mut AdbcStatusCode);
method!(DatabaseGetOption ; FuncDatabaseGetOption ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError);
method!(DatabaseGetOptionBytes ; FuncDatabaseGetOptionBytes ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut u8, *mut usize, *mut FFI_AdbcError);
method!(DatabaseGetOptionDouble ; FuncDatabaseGetOptionDouble ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut f64, *mut FFI_AdbcError);
method!(DatabaseGetOptionInt ; FuncDatabaseGetOptionInt ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut i64, *mut FFI_AdbcError);
method!(DatabaseSetOptionBytes ; FuncDatabaseSetOptionBytes ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *const u8, usize, *mut FFI_AdbcError);
method!(DatabaseSetOptionDouble ; FuncDatabaseSetOptionDouble ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, f64, *mut FFI_AdbcError);
method!(DatabaseSetOptionInt ; FuncDatabaseSetOptionInt ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, i64, *mut FFI_AdbcError);
method!(ConnectionCancel ; FuncConnectionCancel ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionGetOption ; FuncConnectionGetOption ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError);
method!(ConnectionGetOptionBytes ; FuncConnectionGetOptionBytes ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut u8, *mut usize, *mut FFI_AdbcError);
method!(ConnectionGetOptionDouble ; FuncConnectionGetOptionDouble ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut f64, *mut FFI_AdbcError);
method!(ConnectionGetOptionInt ; FuncConnectionGetOptionInt ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut i64, *mut FFI_AdbcError);
method!(ConnectionGetStatistics ; FuncConnectionGetStatistics ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const c_char, *const c_char, c_char, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionGetStatisticNames ; FuncConnectionGetStatisticNames ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionSetOptionBytes ; FuncConnectionSetOptionBytes ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const u8, usize, *mut FFI_AdbcError);
method!(ConnectionSetOptionDouble ; FuncConnectionSetOptionDouble ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, f64, *mut FFI_AdbcError);
method!(ConnectionSetOptionInt ; FuncConnectionSetOptionInt ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, i64, *mut FFI_AdbcError);
method!(StatementCancel ; FuncStatementCancel ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementExecuteSchema ; FuncStatementExecuteSchema ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(StatementGetOption ; FuncStatementGetOption ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError);
method!(StatementGetOptionBytes ; FuncStatementGetOptionBytes ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut u8, *mut usize, *mut FFI_AdbcError);
method!(StatementGetOptionDouble ; FuncStatementGetOptionDouble ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut f64, *mut FFI_AdbcError);
method!(StatementGetOptionInt ; FuncStatementGetOptionInt ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut i64, *mut FFI_AdbcError);
method!(StatementSetOptionBytes ; FuncStatementSetOptionBytes ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *const u8, usize, *mut FFI_AdbcError);
method!(StatementSetOptionDouble ; FuncStatementSetOptionDouble ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, f64, *mut FFI_AdbcError);
method!(StatementSetOptionInt ; FuncStatementSetOptionInt ; AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, i64, *mut FFI_AdbcError);

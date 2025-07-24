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

use super::{
    constants::ADBC_STATUS_NOT_IMPLEMENTED, FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcError,
    FFI_AdbcErrorDetail, FFI_AdbcPartitions, FFI_AdbcStatement, FFI_AdbcStatusCode,
};

macro_rules! method {
    ($func_name:ident ; $type_name:ident ; $return_type:ty ; $return_value:expr ; $( $arg:ty ),*) => {
        #[allow(dead_code)]
        pub(crate) type $type_name = unsafe extern "C" fn($( $arg ),*) -> $return_type;
        #[allow(dead_code)]
        pub unsafe extern "C" fn $func_name($(_:$arg),*) -> $return_type {
            $return_value
        }
    };
}

method!(DatabaseInit ; FuncDatabaseInit ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(DatabaseNew ; FuncDatabaseNew ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(DatabaseSetOption ; FuncDatabaseSetOption ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *const c_char, *mut FFI_AdbcError);
method!(DatabaseRelease ; FuncDatabaseRelease ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(ConnectionCommit ; FuncConnectionCommit ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionGetInfo ; FuncConnectionGetInfo ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const u32, usize, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionGetObjects ; FuncConnectionGetObjects ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, c_int, *const c_char, *const c_char, *const c_char, *const *const c_char, *const c_char, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionGetTableSchema ; FuncConnectionGetTableSchema ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const c_char, *const c_char, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(ConnectionGetTableTypes ; FuncConnectionGetTableTypes ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionInit ; FuncConnectionInit ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcDatabase, *mut FFI_AdbcError);
method!(ConnectionNew ; FuncConnectionNew ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionSetOption ; FuncConnectionSetOption ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const c_char, *mut FFI_AdbcError);
method!(ConnectionReadPartition ; FuncConnectionReadPartition ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const u8, usize, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionRelease ; FuncConnectionRelease ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionRollback ; FuncConnectionRollback ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(StatementBind ; FuncStatementBind ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowArray, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(StatementBindStream ; FuncStatementBindStream ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(StatementExecuteQuery ; FuncStatementExecuteQuery ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowArrayStream, *mut i64, *mut FFI_AdbcError);
method!(StatementExecutePartitions ; FuncStatementExecutePartitions ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowSchema, *mut FFI_AdbcPartitions, *mut i64, *mut FFI_AdbcError);
method!(StatementGetParameterSchema ; FuncStatementGetParameterSchema ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(StatementNew ; FuncStatementNew ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementPrepare ; FuncStatementPrepare ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementRelease ; FuncStatementRelease ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementSetOption ; FuncStatementSetOption ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *const c_char, *mut FFI_AdbcError);
method!(StatementSetSqlQuery ; FuncStatementSetSqlQuery ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut FFI_AdbcError);
method!(StatementSetSubstraitPlan ; FuncStatementSetSubstraitPlan ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const u8, usize, *mut FFI_AdbcError);
method!(ErrorGetDetailCount ; FuncErrorGetDetailCount ; c_int ; 0 ; *const FFI_AdbcError);
method!(ErrorGetDetail ; FuncErrorGetDetail ; FFI_AdbcErrorDetail ; FFI_AdbcErrorDetail::default() ; *const FFI_AdbcError, c_int);
method!(_ErrorFromArrayStream ; FuncErrorFromArrayStream ; *const FFI_AdbcError ; std::ptr::null() ; *mut FFI_ArrowArrayStream, *mut FFI_AdbcStatusCode);
method!(DatabaseGetOption ; FuncDatabaseGetOption ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError);
method!(DatabaseGetOptionBytes ; FuncDatabaseGetOptionBytes ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut u8, *mut usize, *mut FFI_AdbcError);
method!(DatabaseGetOptionDouble ; FuncDatabaseGetOptionDouble ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut f64, *mut FFI_AdbcError);
method!(DatabaseGetOptionInt ; FuncDatabaseGetOptionInt ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *mut i64, *mut FFI_AdbcError);
method!(DatabaseSetOptionBytes ; FuncDatabaseSetOptionBytes ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, *const u8, usize, *mut FFI_AdbcError);
method!(DatabaseSetOptionDouble ; FuncDatabaseSetOptionDouble ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, f64, *mut FFI_AdbcError);
method!(DatabaseSetOptionInt ; FuncDatabaseSetOptionInt ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcDatabase, *const c_char, i64, *mut FFI_AdbcError);
method!(ConnectionCancel ; FuncConnectionCancel ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_AdbcError);
method!(ConnectionGetOption ; FuncConnectionGetOption ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError);
method!(ConnectionGetOptionBytes ; FuncConnectionGetOptionBytes ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut u8, *mut usize, *mut FFI_AdbcError);
method!(ConnectionGetOptionDouble ; FuncConnectionGetOptionDouble ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut f64, *mut FFI_AdbcError);
method!(ConnectionGetOptionInt ; FuncConnectionGetOptionInt ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *mut i64, *mut FFI_AdbcError);
method!(ConnectionGetStatistics ; FuncConnectionGetStatistics ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const c_char, *const c_char, c_char, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionGetStatisticNames ; FuncConnectionGetStatisticNames ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *mut FFI_ArrowArrayStream, *mut FFI_AdbcError);
method!(ConnectionSetOptionBytes ; FuncConnectionSetOptionBytes ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, *const u8, usize, *mut FFI_AdbcError);
method!(ConnectionSetOptionDouble ; FuncConnectionSetOptionDouble ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, f64, *mut FFI_AdbcError);
method!(ConnectionSetOptionInt ; FuncConnectionSetOptionInt ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcConnection, *const c_char, i64, *mut FFI_AdbcError);
method!(StatementCancel ; FuncStatementCancel ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_AdbcError);
method!(StatementExecuteSchema ; FuncStatementExecuteSchema ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *mut FFI_ArrowSchema, *mut FFI_AdbcError);
method!(StatementGetOption ; FuncStatementGetOption ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError);
method!(StatementGetOptionBytes ; FuncStatementGetOptionBytes ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut u8, *mut usize, *mut FFI_AdbcError);
method!(StatementGetOptionDouble ; FuncStatementGetOptionDouble ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut f64, *mut FFI_AdbcError);
method!(StatementGetOptionInt ; FuncStatementGetOptionInt ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *mut i64, *mut FFI_AdbcError);
method!(StatementSetOptionBytes ; FuncStatementSetOptionBytes ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, *const u8, usize, *mut FFI_AdbcError);
method!(StatementSetOptionDouble ; FuncStatementSetOptionDouble ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, f64, *mut FFI_AdbcError);
method!(StatementSetOptionInt ; FuncStatementSetOptionInt ; FFI_AdbcStatusCode ; ADBC_STATUS_NOT_IMPLEMENTED ; *mut FFI_AdbcStatement, *const c_char, i64, *mut FFI_AdbcError);

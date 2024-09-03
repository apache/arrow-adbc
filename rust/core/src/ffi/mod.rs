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

//! C-compatible items as defined in [`adbc.h`](https://github.com/apache/arrow-adbc/blob/main/adbc.h)

pub mod constants;
pub(crate) mod methods;
pub(crate) mod types;
pub use types::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError,
    FFI_AdbcErrorDetail, FFI_AdbcPartitions, FFI_AdbcStatement, FFI_AdbcStatusCode,
};

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

//! ADBC: Arrow Database Connectivity
//!
//! ADBC is a set of APIs and libraries for [Arrow](https://arrow.apache.org/)-native
//! access to databases. Execute SQL and [Substrait](https://substrait.io/)
//! queries, query database catalogs, and more, all using Arrow data to
//! eliminate unnecessary data copies, speed up access, and make it more
//! convenient to build analytical applications.
//!
//! Read more about ADBC at <https://arrow.apache.org/adbc/>
//!
//! This library currently provides:
//! - Structs for C-compatible items as defined in [`adbc.h`](https://github.com/apache/arrow-adbc/blob/main/c/include/arrow-adbc/adbc.h).
//! - A driver exporter that takes an implementation of the abstract API and
//!   turns it into an object file that implements the C API.
//!
//! # Driver Exporter
//!
//! The driver exporter allows exposing native Rust drivers as C drivers to be
//! used by other languages via their own driver manager. Once you have an
//! implementation of [adbc_core::Driver], provided that it also implements [Default], you
//! can build it as an object file implementing the C API with the
//! [export_driver] macro.

pub mod driver_exporter;
#[doc(hidden)]
pub use driver_exporter::FFIDriver;
pub mod methods;
pub(crate) mod types;
pub use types::{
    FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError,
    FFI_AdbcErrorDetail, FFI_AdbcPartitions, FFI_AdbcStatement,
};

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

//! Snowflake ADBC driver, based on the Go driver.
//!
//! # Features
//!
//! ## Linking
//!
//! ### `bundled` (default)
//!
//! Builds the Go Snowflake driver from source and links it statically. This
//! requires a Go compiler to be installed.
//!
//! ### `linked`
//!
//! Link the Go Snowflake driver at build time. Set `ADBC_SNOWFLAKE_GO_LIB_DIR`
//! to add a search paths for the linker.
//!
//! ## Configuration
//!
//! ### `env` (default)
//!
//! Adds `from_env` methods to initialize builders from environment variables.
//!
//! ### `dotenv`: `env` (default)
//!
//! Loads environment variables from `.env` files in `from_env` methods.
//!
//!

#![cfg_attr(docsrs, feature(doc_auto_cfg, doc_cfg))]

pub mod driver;
pub use driver::Driver;

pub mod database;
pub use database::Database;

pub mod connection;
pub use connection::Connection;

pub mod statement;
pub use statement::Statement;

pub mod builder;

#[cfg(feature = "env")]
pub(crate) mod duration;

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

#![cfg_attr(docsrs, feature(doc_auto_cfg, doc_cfg))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico",
    html_favicon_url = "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico"
)]
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

pub mod driver;
pub use driver::Driver;

pub mod database;
pub use database::Database;

pub mod connection;
pub use connection::Connection;

pub mod statement;
pub use statement::Statement;

pub(crate) mod builder;

#[cfg(feature = "env")]
pub(crate) mod duration;

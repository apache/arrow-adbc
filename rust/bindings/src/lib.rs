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

//! Rust bindings for Arrow Database Connectivity (ADBC).
//!
//! These bindings are based on [adbc.h] and generated with [bindgen].
//!
//! [adbc.h]: https://github.com/apache/arrow-adbc/blob/main/adbc.h
//! [bindgen]: https://rust-lang.github.io/rust-bindgen/

#![allow(non_upper_case_globals, non_camel_case_types, non_snake_case)]
#![cfg_attr(doc, allow(rustdoc::invalid_html_tags))]

include!("../adbc.rs");

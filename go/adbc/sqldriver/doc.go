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

// Package sqldriver is a wrapper around the ADBC (Arrow Database
// Connectivity) interfaces to support the standard golang database/sql
// package, described here: https://go.dev/src/database/sql/doc.txt
//
// This allows any ADBC driver implementation to also be used as-is
// with the database/sql package of the standard library rather than
// having to implement drivers for both separately.
//
// Registering the driver can be done by importing this and then running
//
//	sql.Register("drivername", sqldriver.Driver{adbcdriver})
//
// Additionally, the sqldriver/flightsql package simplifies registration
// of the FlightSQL ADBC driver implementation, so that only a single
// import statement is needed. See the example in that package.
//
// EXPERIMENTAL. The ADBC interfaces are subject to change and as such
// this wrapper is also subject to change based on that.
package sqldriver

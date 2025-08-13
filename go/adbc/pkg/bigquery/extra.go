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

//go:build driverlib

// Handwritten. Not regenerated.

package main

// #cgo CFLAGS: -DADBC_EXPORTING
// #cgo CXXFLAGS: -std=c++17 -DADBC_EXPORTING
// #include "../../drivermgr/arrow-adbc/adbc.h"
import "C"

//export AdbcDriverBigqueryInit
func AdbcDriverBigqueryInit(version C.int, rawDriver *C.void, err *C.struct_AdbcError) C.AdbcStatusCode {
	// The driver manager expects the spelling Bigquery and not BigQuery
	// as it derives the init symbol name from the filename
	// (adbc_driver_bigquery -> AdbcDriverBigquery)
	return AdbcDriverBigQueryInit(version, rawDriver, err)
}

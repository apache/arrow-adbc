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

package main

// #cgo CFLAGS: -DADBC_EXPORTING
// #cgo CXXFLAGS: -std=c++17 -DADBC_EXPORTING
// #include "../../drivermgr/arrow-adbc/adbc.h"
// #include "utils.h"
// #include <errno.h>
// #include <string.h>
//
//
import "C"
import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

//export FlightSqlDriverInit
func FlightSqlDriverInit(version C.int, rawDriver *C.void, err *C.struct_AdbcError) C.AdbcStatusCode {
	// For backwards compatibility
	return AdbcDriverFlightsqlInit(version, rawDriver, err)
}

//export FlightSQLDriverInit
func FlightSQLDriverInit(version C.int, rawDriver *C.void, err *C.struct_AdbcError) C.AdbcStatusCode {
	// For backwards compatibility
	return AdbcDriverFlightsqlInit(version, rawDriver, err)
}

//export AdbcDriverFlightsqlInit
func AdbcDriverFlightsqlInit(version C.int, rawDriver *C.void, err *C.struct_AdbcError) C.AdbcStatusCode {
	driver := (*C.struct_AdbcDriver)(unsafe.Pointer(rawDriver))

	switch version {
	case C.ADBC_VERSION_1_0_0:
		sink := fromCArr[byte]((*byte)(unsafe.Pointer(driver)), C.ADBC_DRIVER_1_0_0_SIZE)
		memory.Set(sink, 0)
	case C.ADBC_VERSION_1_1_0:
		sink := fromCArr[byte]((*byte)(unsafe.Pointer(driver)), C.ADBC_DRIVER_1_1_0_SIZE)
		memory.Set(sink, 0)
	default:
		setErr(err, "Only version 1.0.0/1.1.0 supported, got %d", int(version))
		return C.ADBC_STATUS_NOT_IMPLEMENTED
	}

	driver.DatabaseInit = (*[0]byte)(C.FlightSQLDatabaseInit)
	driver.DatabaseNew = (*[0]byte)(C.FlightSQLDatabaseNew)
	driver.DatabaseRelease = (*[0]byte)(C.FlightSQLDatabaseRelease)
	driver.DatabaseSetOption = (*[0]byte)(C.FlightSQLDatabaseSetOption)

	driver.ConnectionNew = (*[0]byte)(C.FlightSQLConnectionNew)
	driver.ConnectionInit = (*[0]byte)(C.FlightSQLConnectionInit)
	driver.ConnectionRelease = (*[0]byte)(C.FlightSQLConnectionRelease)
	driver.ConnectionSetOption = (*[0]byte)(C.FlightSQLConnectionSetOption)
	driver.ConnectionGetInfo = (*[0]byte)(C.FlightSQLConnectionGetInfo)
	driver.ConnectionGetObjects = (*[0]byte)(C.FlightSQLConnectionGetObjects)
	driver.ConnectionGetTableSchema = (*[0]byte)(C.FlightSQLConnectionGetTableSchema)
	driver.ConnectionGetTableTypes = (*[0]byte)(C.FlightSQLConnectionGetTableTypes)
	driver.ConnectionReadPartition = (*[0]byte)(C.FlightSQLConnectionReadPartition)
	driver.ConnectionCommit = (*[0]byte)(C.FlightSQLConnectionCommit)
	driver.ConnectionRollback = (*[0]byte)(C.FlightSQLConnectionRollback)

	driver.StatementNew = (*[0]byte)(C.FlightSQLStatementNew)
	driver.StatementRelease = (*[0]byte)(C.FlightSQLStatementRelease)
	driver.StatementSetOption = (*[0]byte)(C.FlightSQLStatementSetOption)
	driver.StatementSetSqlQuery = (*[0]byte)(C.FlightSQLStatementSetSqlQuery)
	driver.StatementSetSubstraitPlan = (*[0]byte)(C.FlightSQLStatementSetSubstraitPlan)
	driver.StatementBind = (*[0]byte)(C.FlightSQLStatementBind)
	driver.StatementBindStream = (*[0]byte)(C.FlightSQLStatementBindStream)
	driver.StatementExecuteQuery = (*[0]byte)(C.FlightSQLStatementExecuteQuery)
	driver.StatementExecutePartitions = (*[0]byte)(C.FlightSQLStatementExecutePartitions)
	driver.StatementGetParameterSchema = (*[0]byte)(C.FlightSQLStatementGetParameterSchema)
	driver.StatementPrepare = (*[0]byte)(C.FlightSQLStatementPrepare)

	if version == C.ADBC_VERSION_1_1_0 {
		driver.ErrorGetDetailCount = (*[0]byte)(C.FlightSQLErrorGetDetailCount)
		driver.ErrorGetDetail = (*[0]byte)(C.FlightSQLErrorGetDetail)
		driver.ErrorFromArrayStream = (*[0]byte)(C.FlightSQLErrorFromArrayStream)

		driver.DatabaseGetOption = (*[0]byte)(C.FlightSQLDatabaseGetOption)
		driver.DatabaseGetOptionBytes = (*[0]byte)(C.FlightSQLDatabaseGetOptionBytes)
		driver.DatabaseGetOptionDouble = (*[0]byte)(C.FlightSQLDatabaseGetOptionDouble)
		driver.DatabaseGetOptionInt = (*[0]byte)(C.FlightSQLDatabaseGetOptionInt)
		driver.DatabaseSetOptionBytes = (*[0]byte)(C.FlightSQLDatabaseSetOptionBytes)
		driver.DatabaseSetOptionDouble = (*[0]byte)(C.FlightSQLDatabaseSetOptionDouble)
		driver.DatabaseSetOptionInt = (*[0]byte)(C.FlightSQLDatabaseSetOptionInt)

		driver.ConnectionCancel = (*[0]byte)(C.FlightSQLConnectionCancel)
		driver.ConnectionGetOption = (*[0]byte)(C.FlightSQLConnectionGetOption)
		driver.ConnectionGetOptionBytes = (*[0]byte)(C.FlightSQLConnectionGetOptionBytes)
		driver.ConnectionGetOptionDouble = (*[0]byte)(C.FlightSQLConnectionGetOptionDouble)
		driver.ConnectionGetOptionInt = (*[0]byte)(C.FlightSQLConnectionGetOptionInt)
		driver.ConnectionGetStatistics = (*[0]byte)(C.FlightSQLConnectionGetStatistics)
		driver.ConnectionGetStatisticNames = (*[0]byte)(C.FlightSQLConnectionGetStatisticNames)
		driver.ConnectionSetOptionBytes = (*[0]byte)(C.FlightSQLConnectionSetOptionBytes)
		driver.ConnectionSetOptionDouble = (*[0]byte)(C.FlightSQLConnectionSetOptionDouble)
		driver.ConnectionSetOptionInt = (*[0]byte)(C.FlightSQLConnectionSetOptionInt)

		driver.StatementCancel = (*[0]byte)(C.FlightSQLStatementCancel)
		driver.StatementExecuteSchema = (*[0]byte)(C.FlightSQLStatementExecuteSchema)
		driver.StatementGetOption = (*[0]byte)(C.FlightSQLStatementGetOption)
		driver.StatementGetOptionBytes = (*[0]byte)(C.FlightSQLStatementGetOptionBytes)
		driver.StatementGetOptionDouble = (*[0]byte)(C.FlightSQLStatementGetOptionDouble)
		driver.StatementGetOptionInt = (*[0]byte)(C.FlightSQLStatementGetOptionInt)
		driver.StatementSetOptionBytes = (*[0]byte)(C.FlightSQLStatementSetOptionBytes)
		driver.StatementSetOptionDouble = (*[0]byte)(C.FlightSQLStatementSetOptionDouble)
		driver.StatementSetOptionInt = (*[0]byte)(C.FlightSQLStatementSetOptionInt)
	}

	return C.ADBC_STATUS_OK
}
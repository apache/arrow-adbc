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

#pragma once

#include <cstdint>

#include "db2_odbc.h"

#include <nanoarrow/nanoarrow.h>

#include "driver/framework/status.h"

namespace adbc::db2 {

using driver::Status;

/// Describes one column's DB2 metadata and how to bind it.
struct Db2Column {
  SQLSMALLINT sql_type;
  SQLULEN column_size;
  SQLSMALLINT decimal_digits;
  SQLSMALLINT nullable;
  ArrowType arrow_type;
  /// The C type to request from SQLBindCol / SQLGetData.
  SQLSMALLINT c_type;
  /// The byte width of each element in the row-set buffer.
  /// For variable-length types this is the max buffer per element.
  SQLULEN element_size;
};

/// Map a DB2 CLI SQL_* type (as returned by SQLDescribeCol) to an
/// ArrowType and populate the binding metadata in |col|.
Status MapDb2TypeToArrow(SQLSMALLINT sql_type, SQLULEN column_size,
                         SQLSMALLINT decimal_digits, SQLSMALLINT nullable,
                         Db2Column* col);

/// Set a NanoArrow schema node to match the given Db2Column.
Status SetSchemaFromDb2Column(ArrowSchema* schema, const char* name,
                              const Db2Column& col);

/// Return a DB2 DDL type name for a given ArrowType (used in CREATE TABLE).
const char* ArrowTypeToDb2Ddl(ArrowType type);

/// Return the ODBC C type constant for binding an Arrow column via SQLBindParameter.
SQLSMALLINT ArrowTypeToCType(ArrowType type);

/// Return the ODBC SQL type constant for binding an Arrow column via SQLBindParameter.
SQLSMALLINT ArrowTypeToSqlType(ArrowType type);

}  // namespace adbc::db2

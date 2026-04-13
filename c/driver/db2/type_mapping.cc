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

#include "type_mapping.h"

#include <algorithm>
#include <cstring>

#include <nanoarrow/nanoarrow.h>

namespace adbc::db2 {
namespace status = adbc::driver::status;

// SQL_BLOB / SQL_CLOB / SQL_DBCLOB / SQL_XML may not be defined in
// all ODBC driver managers.  DB2 CLI defines them; fall back to
// reasonable constants if missing.
#ifndef SQL_BLOB
#define SQL_BLOB (-98)
#endif
#ifndef SQL_CLOB
#define SQL_CLOB (-99)
#endif
#ifndef SQL_DBCLOB
#define SQL_DBCLOB (-350)
#endif
#ifndef SQL_XML
#define SQL_XML (-370)
#endif

static constexpr SQLULEN kMaxVarcharBuf = 65536;

Status MapDb2TypeToArrow(SQLSMALLINT sql_type, SQLULEN column_size,
                         SQLSMALLINT decimal_digits, SQLSMALLINT nullable,
                         Db2Column* col) {
  col->sql_type = sql_type;
  col->column_size = column_size;
  col->decimal_digits = decimal_digits;
  col->nullable = nullable;

  switch (sql_type) {
    // --- Boolean ---
    case SQL_BIT:
      col->arrow_type = NANOARROW_TYPE_BOOL;
      col->c_type = SQL_C_BIT;
      col->element_size = 1;
      break;

    // --- Integer types ---
    case SQL_TINYINT:
      col->arrow_type = NANOARROW_TYPE_INT8;
      col->c_type = SQL_C_STINYINT;
      col->element_size = 1;
      break;

    case SQL_SMALLINT:
      col->arrow_type = NANOARROW_TYPE_INT16;
      col->c_type = SQL_C_SSHORT;
      col->element_size = sizeof(SQLSMALLINT);
      break;

    case SQL_INTEGER:
      col->arrow_type = NANOARROW_TYPE_INT32;
      col->c_type = SQL_C_SLONG;
      col->element_size = sizeof(SQLINTEGER);
      break;

    case SQL_BIGINT:
      col->arrow_type = NANOARROW_TYPE_INT64;
      col->c_type = SQL_C_SBIGINT;
      col->element_size = sizeof(SQLBIGINT);
      break;

    // --- Floating point ---
    case SQL_REAL:
      col->arrow_type = NANOARROW_TYPE_FLOAT;
      col->c_type = SQL_C_FLOAT;
      col->element_size = sizeof(SQLREAL);
      break;

    case SQL_FLOAT:
    case SQL_DOUBLE:
      col->arrow_type = NANOARROW_TYPE_DOUBLE;
      col->c_type = SQL_C_DOUBLE;
      col->element_size = sizeof(SQLDOUBLE);
      break;

    // --- Decimal / Numeric: fetch as string and represent as string ---
    // Arrow DECIMAL128 requires precise 128-bit integer representation.
    // We represent as STRING with metadata indicating the original type,
    // precision, and scale -- matching the PostgreSQL driver's approach.
    case SQL_DECIMAL:
    case SQL_NUMERIC:
      col->arrow_type = NANOARROW_TYPE_STRING;
      col->c_type = SQL_C_CHAR;
      // precision digits + sign + decimal point + null terminator
      col->element_size = column_size + 4;
      break;

    // --- Character types ---
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
      col->arrow_type = NANOARROW_TYPE_STRING;
      col->c_type = SQL_C_CHAR;
      col->element_size =
          (column_size > 0 && column_size < kMaxVarcharBuf) ? column_size + 1
                                                            : kMaxVarcharBuf;
      break;

    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
      col->arrow_type = NANOARROW_TYPE_STRING;
      col->c_type = SQL_C_CHAR;
      // Request data as UTF-8 from the driver (DB2 CLI converts).
      col->element_size =
          (column_size > 0 && column_size * 4 < kMaxVarcharBuf) ? column_size * 4 + 1
                                                                : kMaxVarcharBuf;
      break;

    // --- Binary ---
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
      col->arrow_type = NANOARROW_TYPE_BINARY;
      col->c_type = SQL_C_BINARY;
      col->element_size =
          (column_size > 0 && column_size < kMaxVarcharBuf) ? column_size : kMaxVarcharBuf;
      break;

    // --- Date / Time / Timestamp ---
    case SQL_TYPE_DATE:
      col->arrow_type = NANOARROW_TYPE_DATE32;
      col->c_type = SQL_C_TYPE_DATE;
      col->element_size = sizeof(SQL_DATE_STRUCT);
      break;

    case SQL_TYPE_TIME:
      col->arrow_type = NANOARROW_TYPE_TIME64;
      col->c_type = SQL_C_TYPE_TIME;
      col->element_size = sizeof(SQL_TIME_STRUCT);
      break;

    case SQL_TYPE_TIMESTAMP:
      col->arrow_type = NANOARROW_TYPE_TIMESTAMP;
      col->c_type = SQL_C_TYPE_TIMESTAMP;
      col->element_size = sizeof(SQL_TIMESTAMP_STRUCT);
      break;

    // --- LOB types (DB2-specific) ---
    case SQL_BLOB:
      col->arrow_type = NANOARROW_TYPE_LARGE_BINARY;
      col->c_type = SQL_C_BINARY;
      col->element_size = kMaxVarcharBuf;
      break;

    case SQL_CLOB:
    case SQL_DBCLOB:
    case SQL_XML:
      col->arrow_type = NANOARROW_TYPE_LARGE_STRING;
      col->c_type = SQL_C_CHAR;
      col->element_size = kMaxVarcharBuf;
      break;

    // --- GUID ---
    case SQL_GUID:
      col->arrow_type = NANOARROW_TYPE_FIXED_SIZE_BINARY;
      col->c_type = SQL_C_CHAR;
      col->element_size = 37;  // 36 chars + NUL
      break;

    default:
      return status::InvalidArgument("[DB2] Unsupported SQL type: ", sql_type);
  }

  return Status::Ok();
}

/// Helper: check a NanoArrow return code and convert to Status.
static Status CheckNanoArrow(int rc, const char* context) {
  if (rc == NANOARROW_OK) return Status::Ok();
  return status::Internal("[DB2] NanoArrow error in ", context, ": ", rc);
}

Status SetSchemaFromDb2Column(ArrowSchema* schema, const char* name,
                              const Db2Column& col) {
  ArrowSchemaInit(schema);
  int rc = NANOARROW_OK;

  switch (col.arrow_type) {
    case NANOARROW_TYPE_BOOL:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL);
      break;
    case NANOARROW_TYPE_INT8:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_INT8);
      break;
    case NANOARROW_TYPE_INT16:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16);
      break;
    case NANOARROW_TYPE_INT32:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32);
      break;
    case NANOARROW_TYPE_INT64:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64);
      break;
    case NANOARROW_TYPE_FLOAT:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT);
      break;
    case NANOARROW_TYPE_DOUBLE:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE);
      break;
    case NANOARROW_TYPE_STRING:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING);
      break;
    case NANOARROW_TYPE_BINARY:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY);
      break;
    case NANOARROW_TYPE_LARGE_STRING:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_LARGE_STRING);
      break;
    case NANOARROW_TYPE_LARGE_BINARY:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_LARGE_BINARY);
      break;
    case NANOARROW_TYPE_DATE32:
      rc = ArrowSchemaSetType(schema, NANOARROW_TYPE_DATE32);
      break;
    case NANOARROW_TYPE_TIME64:
      rc = ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64,
                                      NANOARROW_TIME_UNIT_MICRO, nullptr);
      break;
    case NANOARROW_TYPE_TIMESTAMP:
      rc = ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                      NANOARROW_TIME_UNIT_MICRO, nullptr);
      break;
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      rc = ArrowSchemaSetTypeFixedSize(schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, 36);
      break;
    default:
      return status::Internal("[DB2] Cannot build schema for ArrowType: ",
                              static_cast<int>(col.arrow_type));
  }
  UNWRAP_STATUS(CheckNanoArrow(rc, "ArrowSchemaSetType"));

  rc = ArrowSchemaSetName(schema, name);
  UNWRAP_STATUS(CheckNanoArrow(rc, "ArrowSchemaSetName"));

  if (col.nullable != SQL_NO_NULLS) {
    schema->flags |= ARROW_FLAG_NULLABLE;
  } else {
    schema->flags &= ~ARROW_FLAG_NULLABLE;
  }

  return Status::Ok();
}

const char* ArrowTypeToDb2Ddl(ArrowType type) {
  switch (type) {
    case NANOARROW_TYPE_BOOL:
      return "SMALLINT";
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_UINT16:
      return "SMALLINT";
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_UINT32:
      return "INTEGER";
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_UINT64:
      return "BIGINT";
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT:
      return "REAL";
    case NANOARROW_TYPE_DOUBLE:
      return "DOUBLE";
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW:
      return "VARCHAR(32672)";
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_BINARY_VIEW:
      return "VARCHAR(32672) FOR BIT DATA";
    case NANOARROW_TYPE_DATE32:
      return "DATE";
    case NANOARROW_TYPE_TIME64:
      return "TIME";
    case NANOARROW_TYPE_TIMESTAMP:
      return "TIMESTAMP";
    case NANOARROW_TYPE_DURATION:
      return "BIGINT";
    default:
      return nullptr;
  }
}

SQLSMALLINT ArrowTypeToCType(ArrowType type) {
  switch (type) {
    case NANOARROW_TYPE_BOOL:
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
      return SQL_C_STINYINT;
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_UINT16:
      return SQL_C_SSHORT;
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_UINT32:
      return SQL_C_SLONG;
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_UINT64:
    case NANOARROW_TYPE_DURATION:
      return SQL_C_SBIGINT;
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT:
      return SQL_C_FLOAT;
    case NANOARROW_TYPE_DOUBLE:
      return SQL_C_DOUBLE;
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW:
      return SQL_C_CHAR;
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_BINARY_VIEW:
      return SQL_C_BINARY;
    case NANOARROW_TYPE_DATE32:
      return SQL_C_TYPE_DATE;
    case NANOARROW_TYPE_TIME64:
      return SQL_C_TYPE_TIME;
    case NANOARROW_TYPE_TIMESTAMP:
      return SQL_C_TYPE_TIMESTAMP;
    default:
      return SQL_C_DEFAULT;
  }
}

SQLSMALLINT ArrowTypeToSqlType(ArrowType type) {
  switch (type) {
    case NANOARROW_TYPE_BOOL:
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_UINT16:
      return SQL_SMALLINT;
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_UINT32:
      return SQL_INTEGER;
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_UINT64:
    case NANOARROW_TYPE_DURATION:
      return SQL_BIGINT;
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT:
      return SQL_REAL;
    case NANOARROW_TYPE_DOUBLE:
      return SQL_DOUBLE;
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW:
      return SQL_VARCHAR;
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_BINARY_VIEW:
      return SQL_VARBINARY;
    case NANOARROW_TYPE_DATE32:
      return SQL_TYPE_DATE;
    case NANOARROW_TYPE_TIME64:
      return SQL_TYPE_TIME;
    case NANOARROW_TYPE_TIMESTAMP:
      return SQL_TYPE_TIMESTAMP;
    default:
      return SQL_VARCHAR;
  }
}

}  // namespace adbc::db2

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

#include "statement.h"

#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "error.h"
#include "result_reader.h"
#include "type_mapping.h"

namespace adbc::db2 {

Status Db2Statement::InitImpl(void* parent) {
  connection_ = reinterpret_cast<Db2Connection*>(parent);
  return status::Ok();
}

Status Db2Statement::ReleaseImpl() {
  FreeStatement();
  return status::Ok();
}

Status Db2Statement::AllocateStatement() {
  if (hstmt_ != SQL_NULL_HSTMT) return status::Ok();

  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, connection_->hdbc(), &hstmt_);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    // Check if the connection is dead (ODBC 3.5+)
#ifndef SQL_ATTR_CONNECTION_DEAD
#define SQL_ATTR_CONNECTION_DEAD 1209
#endif
#ifndef SQL_CD_TRUE
#define SQL_CD_TRUE 1
#endif
    SQLUINTEGER dead = 0;
    SQLRETURN check_rc = SQLGetConnectAttr(
        connection_->hdbc(), SQL_ATTR_CONNECTION_DEAD, &dead, 0, nullptr);
    if ((check_rc == SQL_SUCCESS || check_rc == SQL_SUCCESS_WITH_INFO) &&
        dead == SQL_CD_TRUE) {
      return status::IO("[DB2] Connection is no longer active. Please reconnect.");
    }
    UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, connection_->hdbc(), rc,
                           "SQLAllocHandle(STMT)"));
  }
  return status::Ok();
}

void Db2Statement::FreeStatement() {
  if (hstmt_ != SQL_NULL_HSTMT) {
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt_);
    hstmt_ = SQL_NULL_HSTMT;
  }
  prepared_ = false;
}

Status Db2Statement::PrepareImpl(QueryState& state) {
  FreeStatement();
  UNWRAP_STATUS(AllocateStatement());

  SQLRETURN rc = SQLPrepare(
      hstmt_,
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(state.query.c_str())),
      static_cast<SQLINTEGER>(state.query.size()));
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt_, rc, "SQLPrepare"));
  prepared_ = true;
  return status::Ok();
}

Status Db2Statement::SetupArrayView(ArrowArrayView* view, ArrowSchema* schema,
                                     ArrowArray* array) {
  ArrowArrayViewInitFromType(view, NANOARROW_TYPE_STRUCT);
  int na_rc = ArrowArrayViewAllocateChildren(view, schema->n_children);
  if (na_rc != NANOARROW_OK) {
    return status::Internal("[DB2] Failed to allocate param array view");
  }
  for (int64_t c = 0; c < schema->n_children; c++) {
    ArrowSchemaView child_view;
    struct ArrowError arrow_error = {};
    na_rc = ArrowSchemaViewInit(&child_view, schema->children[c], &arrow_error);
    if (na_rc != NANOARROW_OK) {
      return status::Internal("[DB2] Failed to parse param schema: ",
                              arrow_error.message);
    }
    ArrowArrayViewInitFromType(view->children[c], child_view.type);
  }
  struct ArrowError set_error = {};
  na_rc = ArrowArrayViewSetArray(view, array, &set_error);
  if (na_rc != NANOARROW_OK) {
    return status::Internal("[DB2] Failed to set param array on view: ",
                            set_error.message);
  }
  return status::Ok();
}

Result<int64_t> Db2Statement::ExecuteCommon(const std::string& query, bool is_prepared,
                                            ArrowArrayStream* stream) {
  UNWRAP_STATUS(AllocateStatement());

  // For prepared statements with bound multi-row params and no result stream,
  // execute once per row across all batches in the bound stream.
  if (is_prepared && bind_parameters_.release && !stream) {
    ArrowSchema param_schema;
    std::memset(&param_schema, 0, sizeof(param_schema));
    int na_rc = bind_parameters_.get_schema(&bind_parameters_, &param_schema);
    if (na_rc != 0) {
      if (param_schema.release) param_schema.release(&param_schema);
      return status::Internal("[DB2] Failed to get parameter schema");
    }

    int64_t total_rows = 0;
    while (true) {
      ArrowArray param_array;
      std::memset(&param_array, 0, sizeof(param_array));
      na_rc = bind_parameters_.get_next(&bind_parameters_, &param_array);
      if (na_rc != 0) {
        param_schema.release(&param_schema);
        return status::Internal("[DB2] Failed to get parameter batch");
      }
      if (!param_array.release) break;

      if (param_array.length > 0) {
        ArrowArrayView param_view;
        Status view_status = SetupArrayView(&param_view, &param_schema, &param_array);
        if (!view_status.ok()) {
          ArrowArrayViewReset(&param_view);
          param_array.release(&param_array);
          param_schema.release(&param_schema);
          return view_status;
        }

        for (int64_t row = 0; row < param_array.length; row++) {
          ClearBindData();
          Status bind_status = BindParameters(hstmt_, &param_schema, &param_view, row);
          if (!bind_status.ok()) {
            ArrowArrayViewReset(&param_view);
            param_array.release(&param_array);
            param_schema.release(&param_schema);
            return bind_status;
          }

          SQLRETURN rc = SQLExecute(hstmt_);
          if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA) {
            Status err = Db2Error(SQL_HANDLE_STMT, hstmt_, "SQLExecute");
            ArrowArrayViewReset(&param_view);
            param_array.release(&param_array);
            param_schema.release(&param_schema);
            FreeStatement();
            return err;
          }

          SQLLEN row_count = 0;
          SQLRowCount(hstmt_, &row_count);
          if (row_count > 0) total_rows += row_count;
          SQLFreeStmt(hstmt_, SQL_CLOSE);
        }
        ArrowArrayViewReset(&param_view);
      }
      param_array.release(&param_array);
    }

    param_schema.release(&param_schema);
    return total_rows;
  }

  // For prepared queries with bound parameters (single-row, returning results),
  // bind the first row only.
  if (is_prepared && bind_parameters_.release) {
    ArrowSchema param_schema;
    std::memset(&param_schema, 0, sizeof(param_schema));
    int na_rc = bind_parameters_.get_schema(&bind_parameters_, &param_schema);
    if (na_rc != 0) {
      if (param_schema.release) param_schema.release(&param_schema);
      return status::Internal("[DB2] Failed to get parameter schema");
    }

    ArrowArray param_array;
    std::memset(&param_array, 0, sizeof(param_array));
    na_rc = bind_parameters_.get_next(&bind_parameters_, &param_array);
    if (na_rc != 0) {
      param_schema.release(&param_schema);
      return status::Internal("[DB2] Failed to get parameter batch");
    }

    if (param_array.release && param_array.length > 0) {
      ArrowArrayView param_view;
      Status view_status = SetupArrayView(&param_view, &param_schema, &param_array);
      if (!view_status.ok()) {
        ArrowArrayViewReset(&param_view);
        param_array.release(&param_array);
        param_schema.release(&param_schema);
        return view_status;
      }

      ClearBindData();
      Status bind_status = BindParameters(hstmt_, &param_schema, &param_view, 0);
      ArrowArrayViewReset(&param_view);
      param_array.release(&param_array);
      param_schema.release(&param_schema);
      UNWRAP_STATUS(bind_status);
    } else {
      if (param_array.release) param_array.release(&param_array);
      param_schema.release(&param_schema);
    }
  }

  connection_->RegisterActiveStatement(hstmt_);
  SQLRETURN rc;
  if (is_prepared) {
    rc = SQLExecute(hstmt_);
  } else {
    rc = SQLExecDirect(
        hstmt_,
        const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(query.c_str())),
        static_cast<SQLINTEGER>(query.size()));
  }
  connection_->UnregisterActiveStatement();

  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA) {
    Status err = Db2Error(SQL_HANDLE_STMT, hstmt_, "SQLExecDirect/SQLExecute");
    FreeStatement();
    return err;
  }

  if (stream) {
    auto reader = new Db2ResultReader(hstmt_, batch_size_);
    Status init_status = reader->Init();
    if (!init_status.ok()) {
      delete reader;
      hstmt_ = SQL_NULL_HSTMT;
      return init_status;
    }
    reader->ExportTo(stream);
    hstmt_ = SQL_NULL_HSTMT;
    prepared_ = false;
    return -1;
  }

  SQLLEN row_count = 0;
  rc = SQLRowCount(hstmt_, &row_count);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    row_count = -1;
  }
  SQLFreeStmt(hstmt_, SQL_CLOSE);
  return static_cast<int64_t>(row_count);
}

Result<int64_t> Db2Statement::ExecuteQueryImpl(QueryState& state,
                                               ArrowArrayStream* stream) {
  return ExecuteCommon(state.query, false, stream);
}

Result<int64_t> Db2Statement::ExecuteQueryImpl(PreparedState& state,
                                               ArrowArrayStream* stream) {
  return ExecuteCommon(state.query, true, stream);
}

Result<int64_t> Db2Statement::ExecuteUpdateImpl(QueryState& state) {
  return ExecuteCommon(state.query, false, nullptr);
}

Result<int64_t> Db2Statement::ExecuteUpdateImpl(PreparedState& state) {
  return ExecuteCommon(state.query, true, nullptr);
}

Status Db2Statement::SetOptionImpl(std::string_view key, Option value) {
  if (key == "adbc.db2.query.batch_rows") {
    int64_t v;
    UNWRAP_RESULT(v, value.AsInt());
    if (v <= 0) {
      return status::InvalidArgument("[DB2] batch_rows must be > 0");
    }
    batch_size_ = v;
    return status::Ok();
  }
  return status::NotImplemented("[DB2] Unknown statement option ", key, "=",
                                value.Format());
}

void Db2Statement::ClearBindData() {
  bind_data_int8_.clear();
  bind_data_int16_.clear();
  bind_data_int32_.clear();
  bind_data_int64_.clear();
  bind_data_float_.clear();
  bind_data_double_.clear();
  bind_data_strings_.clear();
  bind_data_date_.clear();
  bind_data_time_.clear();
  bind_data_timestamp_.clear();
  bind_indicators_.clear();
}

Status Db2Statement::BindParameters(SQLHSTMT hstmt, ArrowSchema* schema,
                                    ArrowArrayView* array_view, int64_t row) {
  // Reserve capacity to prevent reallocation during the loop.
  // SQLBindParameter stores pointers into these vectors, so any
  // reallocation would create dangling pointers and cause
  // SQL_NEED_DATA (rc=99) or other undefined behavior.
  auto n = static_cast<size_t>(schema->n_children);
  bind_indicators_.reserve(bind_indicators_.size() + n);
  bind_data_int8_.reserve(bind_data_int8_.size() + n);
  bind_data_int16_.reserve(bind_data_int16_.size() + n);
  bind_data_int32_.reserve(bind_data_int32_.size() + n);
  bind_data_int64_.reserve(bind_data_int64_.size() + n);
  bind_data_float_.reserve(bind_data_float_.size() + n);
  bind_data_double_.reserve(bind_data_double_.size() + n);
  bind_data_strings_.reserve(bind_data_strings_.size() + n);
  bind_data_date_.reserve(bind_data_date_.size() + n);
  bind_data_time_.reserve(bind_data_time_.size() + n);
  bind_data_timestamp_.reserve(bind_data_timestamp_.size() + n);

  for (int64_t col = 0; col < schema->n_children; col++) {
    ArrowSchemaView child_view;
    struct ArrowError arrow_error = {};
    int na_rc = ArrowSchemaViewInit(&child_view, schema->children[col], &arrow_error);
    if (na_rc != NANOARROW_OK) {
      return status::Internal("[DB2] Failed to parse schema for column ", col, ": ",
                              arrow_error.message);
    }

    ArrowArrayView* child = array_view->children[col];
    SQLUSMALLINT param_num = static_cast<SQLUSMALLINT>(col + 1);

    if (ArrowArrayViewIsNull(child, row)) {
      SQLSMALLINT null_sql_type = ArrowTypeToSqlType(child_view.type);
      bind_indicators_.push_back(SQL_NULL_DATA);
      SQLRETURN rc = SQLBindParameter(hstmt, param_num, SQL_PARAM_INPUT,
                                      SQL_C_DEFAULT, null_sql_type, 0, 0,
                                      nullptr, 0, &bind_indicators_.back());
      UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter(NULL)"));
      continue;
    }

    ArrowType type = child_view.type;
    SQLSMALLINT c_type = ArrowTypeToCType(type);
    SQLSMALLINT sql_type = ArrowTypeToSqlType(type);

    switch (type) {
      case NANOARROW_TYPE_BOOL: {
        int8_t val = static_cast<int8_t>(ArrowArrayViewGetIntUnsafe(child, row) ? 1 : 0);
        bind_data_int8_.push_back(val);
        bind_indicators_.push_back(0);
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, c_type, sql_type, 0, 0,
            &bind_data_int8_.back(), 0, &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_UINT8: {
        int8_t val = static_cast<int8_t>(ArrowArrayViewGetIntUnsafe(child, row));
        bind_data_int8_.push_back(val);
        bind_indicators_.push_back(0);
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, c_type, sql_type, 0, 0,
            &bind_data_int8_.back(), 0, &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_UINT16: {
        int16_t val = static_cast<int16_t>(ArrowArrayViewGetIntUnsafe(child, row));
        bind_data_int16_.push_back(val);
        bind_indicators_.push_back(0);
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, c_type, sql_type, 0, 0,
            &bind_data_int16_.back(), 0, &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_UINT32: {
        int32_t val = static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(child, row));
        bind_data_int32_.push_back(val);
        bind_indicators_.push_back(0);
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, c_type, sql_type, 0, 0,
            &bind_data_int32_.back(), 0, &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_INT64:
      case NANOARROW_TYPE_UINT64:
      case NANOARROW_TYPE_DURATION: {
        int64_t val = ArrowArrayViewGetIntUnsafe(child, row);
        bind_data_int64_.push_back(val);
        bind_indicators_.push_back(0);
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, c_type, sql_type, 0, 0,
            &bind_data_int64_.back(), 0, &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_FLOAT: {
        float val = static_cast<float>(ArrowArrayViewGetDoubleUnsafe(child, row));
        bind_data_float_.push_back(val);
        bind_indicators_.push_back(0);
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, c_type, sql_type, 0, 0,
            &bind_data_float_.back(), 0, &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_DOUBLE: {
        double val = ArrowArrayViewGetDoubleUnsafe(child, row);
        bind_data_double_.push_back(val);
        bind_indicators_.push_back(0);
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, c_type, sql_type, 0, 0,
            &bind_data_double_.back(), 0, &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_STRING:
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW: {
        ArrowStringView sv = ArrowArrayViewGetStringUnsafe(child, row);
        bind_data_strings_.emplace_back(sv.data, sv.size_bytes);
        bind_indicators_.push_back(static_cast<SQLLEN>(sv.size_bytes));
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
            static_cast<SQLULEN>(sv.size_bytes), 0,
            const_cast<char*>(bind_data_strings_.back().data()),
            static_cast<SQLLEN>(sv.size_bytes + 1), &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_BINARY:
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW: {
        ArrowBufferView bv = ArrowArrayViewGetBytesUnsafe(child, row);
        bind_data_strings_.emplace_back(
            reinterpret_cast<const char*>(bv.data.data), bv.size_bytes);
        bind_indicators_.push_back(static_cast<SQLLEN>(bv.size_bytes));
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY,
            static_cast<SQLULEN>(bv.size_bytes), 0,
            const_cast<char*>(bind_data_strings_.back().data()),
            static_cast<SQLLEN>(bv.size_bytes), &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_DATE32: {
        int32_t days = static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(child, row));
        // Pure integer civil-date conversion (Howard Hinnant's algorithm).
        // Avoids gmtime_r which has platform-specific edge cases for
        // negative time_t values.
        static constexpr int32_t kEpochOffset = 719468;
        int32_t z = days + kEpochOffset;
        int32_t era = (z >= 0 ? z : z - 146096) / 146097;
        unsigned doe = static_cast<unsigned>(z - era * 146097);
        unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        int32_t y = static_cast<int32_t>(yoe) + era * 400;
        unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        unsigned mp = (5 * doy + 2) / 153;
        unsigned d = doy - (153 * mp + 2) / 5 + 1;
        unsigned m = mp < 10 ? mp + 3 : mp - 9;
        y += (m <= 2);
        SQL_DATE_STRUCT ds;
        ds.year = static_cast<SQLSMALLINT>(y);
        ds.month = static_cast<SQLUSMALLINT>(m);
        ds.day = static_cast<SQLUSMALLINT>(d);
        bind_data_date_.push_back(ds);
        bind_indicators_.push_back(sizeof(SQL_DATE_STRUCT));
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, SQL_C_TYPE_DATE, SQL_TYPE_DATE,
            10, 0, &bind_data_date_.back(), sizeof(SQL_DATE_STRUCT),
            &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_TIME64: {
        int64_t micros = ArrowArrayViewGetIntUnsafe(child, row);
        SQL_TIME_STRUCT ts;
        int64_t total_secs = (micros / 1000000) % 86400;
        if (total_secs < 0) total_secs += 86400;
        ts.hour = static_cast<SQLUSMALLINT>(total_secs / 3600);
        ts.minute = static_cast<SQLUSMALLINT>((total_secs % 3600) / 60);
        ts.second = static_cast<SQLUSMALLINT>(total_secs % 60);
        bind_data_time_.push_back(ts);
        bind_indicators_.push_back(sizeof(SQL_TIME_STRUCT));
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, SQL_C_TYPE_TIME, SQL_TYPE_TIME,
            8, 0, &bind_data_time_.back(), sizeof(SQL_TIME_STRUCT),
            &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      case NANOARROW_TYPE_TIMESTAMP: {
        int64_t micros = ArrowArrayViewGetIntUnsafe(child, row);
        time_t secs = static_cast<time_t>(micros / 1000000);
        int32_t frac = static_cast<int32_t>(micros % 1000000);
        if (frac < 0) {
          secs -= 1;
          frac += 1000000;
        }
        struct tm tm_val;
#ifdef _WIN32
        gmtime_s(&tm_val, &secs);
#else
        gmtime_r(&secs, &tm_val);
#endif
        SQL_TIMESTAMP_STRUCT tss;
        tss.year = static_cast<SQLSMALLINT>(tm_val.tm_year + 1900);
        tss.month = static_cast<SQLUSMALLINT>(tm_val.tm_mon + 1);
        tss.day = static_cast<SQLUSMALLINT>(tm_val.tm_mday);
        tss.hour = static_cast<SQLUSMALLINT>(tm_val.tm_hour);
        tss.minute = static_cast<SQLUSMALLINT>(tm_val.tm_min);
        tss.second = static_cast<SQLUSMALLINT>(tm_val.tm_sec);
        tss.fraction = static_cast<SQLUINTEGER>(frac) * 1000;
        bind_data_timestamp_.push_back(tss);
        bind_indicators_.push_back(sizeof(SQL_TIMESTAMP_STRUCT));
        SQLRETURN rc = SQLBindParameter(
            hstmt, param_num, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
            SQL_TYPE_TIMESTAMP, 26, 6, &bind_data_timestamp_.back(),
            sizeof(SQL_TIMESTAMP_STRUCT), &bind_indicators_.back());
        UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt, rc, "SQLBindParameter"));
        break;
      }
      default:
        return status::NotImplemented("[DB2] Cannot bind Arrow type ",
                                      static_cast<int>(type), " for parameter ", col);
    }
  }
  return status::Ok();
}

AdbcStatusCode Db2Statement::ExecuteSchema(ArrowSchema* schema, AdbcError* error) {
  if (hstmt_ == SQL_NULL_HSTMT || !prepared_) {
    return status::InvalidState("[DB2] Statement must be prepared before ExecuteSchema")
        .ToAdbc(error);
  }

  SQLSMALLINT num_cols = 0;
  SQLRETURN rc = SQLNumResultCols(hstmt_, &num_cols);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    return Db2Error(SQL_HANDLE_STMT, hstmt_, "SQLNumResultCols").ToAdbc(error);
  }

  ArrowSchemaInit(schema);
  int na_rc = ArrowSchemaSetTypeStruct(schema, num_cols);
  if (na_rc != NANOARROW_OK) {
    return status::Internal("[DB2] Failed to init schema struct").ToAdbc(error);
  }

  for (SQLSMALLINT i = 0; i < num_cols; i++) {
    SQLCHAR col_name[256];
    SQLSMALLINT name_len = 0;
    SQLSMALLINT sql_type = 0;
    SQLULEN col_size = 0;
    SQLSMALLINT decimal_digits = 0;
    SQLSMALLINT nullable = SQL_NULLABLE_UNKNOWN;

    rc = SQLDescribeCol(hstmt_, static_cast<SQLUSMALLINT>(i + 1), col_name,
                        sizeof(col_name), &name_len, &sql_type, &col_size,
                        &decimal_digits, &nullable);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
      schema->release(schema);
      return Db2Error(SQL_HANDLE_STMT, hstmt_, "SQLDescribeCol").ToAdbc(error);
    }

    Db2Column db2_col;
    Status map_status =
        MapDb2TypeToArrow(sql_type, col_size, decimal_digits, nullable, &db2_col);
    if (!map_status.ok()) {
      schema->release(schema);
      return map_status.ToAdbc(error);
    }

    name_len = std::min(name_len, static_cast<SQLSMALLINT>(sizeof(col_name) - 1));
    col_name[name_len] = '\0';
    Status s = SetSchemaFromDb2Column(schema->children[i],
                                      reinterpret_cast<const char*>(col_name), db2_col);
    if (!s.ok()) {
      schema->release(schema);
      return s.ToAdbc(error);
    }
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode Db2Statement::Cancel(AdbcError* error) {
  if (hstmt_ != SQL_NULL_HSTMT) {
    SQLRETURN rc = SQLCancel(hstmt_);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
      return Db2Error(SQL_HANDLE_STMT, hstmt_, "SQLCancel").ToAdbc(error);
    }
  }
  return ADBC_STATUS_OK;
}

Status Db2Statement::GetParameterSchemaImpl(PreparedState& state,
                                            ArrowSchema* schema) {
  if (hstmt_ == SQL_NULL_HSTMT || !prepared_) {
    return status::InvalidState("[DB2] Statement must be prepared first");
  }

  SQLSMALLINT num_params = 0;
  SQLRETURN rc = SQLNumParams(hstmt_, &num_params);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt_, rc, "SQLNumParams"));

  ArrowSchemaInit(schema);
  UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(schema, num_params));

  for (SQLSMALLINT i = 0; i < num_params; i++) {
    SQLSMALLINT sql_type = SQL_VARCHAR;
    SQLULEN param_size = 0;
    SQLSMALLINT decimal_digits = 0;
    SQLSMALLINT nullable = SQL_NULLABLE_UNKNOWN;

    rc = SQLDescribeParam(hstmt_, static_cast<SQLUSMALLINT>(i + 1),
                          &sql_type, &param_size, &decimal_digits, &nullable);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
      // If SQLDescribeParam is not supported, fall back to NA type
      ArrowSchemaInit(schema->children[i]);
      UNWRAP_ERRNO(Internal,
                   ArrowSchemaSetType(schema->children[i], NANOARROW_TYPE_NA));
      UNWRAP_ERRNO(Internal, ArrowSchemaSetName(schema->children[i], ""));
      continue;
    }

    Db2Column col;
    Status map_status =
        MapDb2TypeToArrow(sql_type, param_size, decimal_digits, nullable, &col);
    if (!map_status.ok()) {
      schema->release(schema);
      return map_status;
    }
    Status schema_status =
        SetSchemaFromDb2Column(schema->children[i], "", col);
    if (!schema_status.ok()) {
      schema->release(schema);
      return schema_status;
    }
  }
  return status::Ok();
}

Result<int64_t> Db2Statement::ExecuteIngestImpl(IngestState& state) {
  if (!bind_parameters_.release) {
    return status::InvalidState("[DB2] Must Bind() before bulk ingestion");
  }
  if (!state.target_table) {
    return status::InvalidState("[DB2] Must set ", ADBC_INGEST_OPTION_TARGET_TABLE);
  }

  ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  int na_rc = bind_parameters_.get_schema(&bind_parameters_, &schema);
  if (na_rc != 0) {
    return status::Internal("[DB2] Failed to get schema from bound stream");
  }

  // Build qualified table name
  std::string table_name;
  if (state.target_schema) {
    table_name = "\"" + *state.target_schema + "\".\"" + *state.target_table + "\"";
  } else {
    table_name = "\"" + *state.target_table + "\"";
  }

  // Handle table create/drop based on ingest mode
  switch (state.table_exists_) {
    case Base::TableExists::kReplace: {
      std::string drop = "DROP TABLE " + table_name;
      // Ignore error (table may not exist)
      SQLHSTMT tmp = SQL_NULL_HSTMT;
      SQLAllocHandle(SQL_HANDLE_STMT, connection_->hdbc(), &tmp);
      SQLExecDirect(tmp,
                    const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(drop.c_str())),
                    static_cast<SQLINTEGER>(drop.size()));
      SQLFreeHandle(SQL_HANDLE_STMT, tmp);
      break;
    }
    case Base::TableExists::kAppend:
    case Base::TableExists::kFail:
      break;
  }

  bool need_create = false;
  switch (state.table_does_not_exist_) {
    case Base::TableDoesNotExist::kCreate:
      need_create = true;
      break;
    case Base::TableDoesNotExist::kFail:
      need_create = false;
      break;
  }

  if (need_create) {
    std::ostringstream ddl;
    if (state.table_exists_ == Base::TableExists::kAppend) {
      // CREATE TABLE IF NOT EXISTS is not standard DB2; use a workaround
      // by catching the -601 SQLSTATE (name already exists in the catalog)
      ddl << "CREATE TABLE " << table_name << " (";
    } else {
      ddl << "CREATE TABLE " << table_name << " (";
    }
    for (int64_t i = 0; i < schema.n_children; i++) {
      if (i > 0) ddl << ", ";
      ddl << "\"" << schema.children[i]->name << "\"";

      ArrowSchemaView child_view;
      struct ArrowError arrow_error = {};
      na_rc = ArrowSchemaViewInit(&child_view, schema.children[i], &arrow_error);
      if (na_rc != NANOARROW_OK) {
        schema.release(&schema);
        return status::Internal("[DB2] Failed to parse schema for column ", i, ": ",
                                arrow_error.message);
      }
      const char* db2_type = ArrowTypeToDb2Ddl(child_view.type);
      if (!db2_type) {
        schema.release(&schema);
        return status::NotImplemented("[DB2] Unsupported Arrow type ",
                                      static_cast<int>(child_view.type),
                                      " for DDL generation");
      }
      ddl << " " << db2_type;
    }
    ddl << ")";

    std::string ddl_str = ddl.str();
    SQLHSTMT ddl_stmt = SQL_NULL_HSTMT;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, connection_->hdbc(), &ddl_stmt);
    UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, connection_->hdbc(), rc,
                           "SQLAllocHandle(STMT)"));
    rc = SQLExecDirect(
        ddl_stmt,
        const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(ddl_str.c_str())),
        static_cast<SQLINTEGER>(ddl_str.size()));
    bool create_ok = (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO);
    if (!create_ok) {
      // If APPEND mode, ignore "table already exists" errors (SQLSTATE 42S01 or 42710)
      if (state.table_exists_ == Base::TableExists::kAppend) {
        SQLCHAR sqlstate[6] = {};
        SQLINTEGER native_error = 0;
        SQLCHAR msg[256] = {};
        SQLSMALLINT msg_len = 0;
        SQLGetDiagRec(SQL_HANDLE_STMT, ddl_stmt, 1, sqlstate, &native_error,
                      msg, sizeof(msg), &msg_len);
        std::string state_str(reinterpret_cast<char*>(sqlstate), 5);
        if (state_str != "42S01" && state_str != "42710") {
          Status err = Db2Error(SQL_HANDLE_STMT, ddl_stmt, "CREATE TABLE");
          SQLFreeHandle(SQL_HANDLE_STMT, ddl_stmt);
          schema.release(&schema);
          return err;
        }
      } else {
        Status err = Db2Error(SQL_HANDLE_STMT, ddl_stmt, "CREATE TABLE");
        SQLFreeHandle(SQL_HANDLE_STMT, ddl_stmt);
        schema.release(&schema);
        return err;
      }
    }
    SQLFreeHandle(SQL_HANDLE_STMT, ddl_stmt);
  }

  // Build INSERT statement
  std::ostringstream insert_sql;
  insert_sql << "INSERT INTO " << table_name << " (";
  for (int64_t i = 0; i < schema.n_children; i++) {
    if (i > 0) insert_sql << ", ";
    insert_sql << "\"" << schema.children[i]->name << "\"";
  }
  insert_sql << ") VALUES (";
  for (int64_t i = 0; i < schema.n_children; i++) {
    if (i > 0) insert_sql << ", ";
    insert_sql << "?";
  }
  insert_sql << ")";
  std::string insert_str = insert_sql.str();

  SQLHSTMT ins_stmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, connection_->hdbc(), &ins_stmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, connection_->hdbc(), rc,
                         "SQLAllocHandle(STMT)"));
  rc = SQLPrepare(
      ins_stmt,
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(insert_str.c_str())),
      static_cast<SQLINTEGER>(insert_str.size()));
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    Status err = Db2Error(SQL_HANDLE_STMT, ins_stmt, "SQLPrepare INSERT");
    SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
    schema.release(&schema);
    return err;
  }

  int64_t ncols = schema.n_children;

  // Pre-parse column types once
  std::vector<ArrowType> col_types(ncols);
  for (int64_t c = 0; c < ncols; c++) {
    ArrowSchemaView sv;
    struct ArrowError ae = {};
    na_rc = ArrowSchemaViewInit(&sv, schema.children[c], &ae);
    if (na_rc != NANOARROW_OK) {
      SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
      schema.release(&schema);
      return status::Internal("[DB2] Failed to parse schema for column ", c);
    }
    col_types[c] = sv.type;
  }

  SQLSetStmtAttr(ins_stmt, SQL_ATTR_PARAM_BIND_TYPE,
                 reinterpret_cast<SQLPOINTER>(SQL_PARAM_BIND_BY_COLUMN), 0);

  int64_t total_rows = 0;

  while (true) {
    ArrowArray array;
    std::memset(&array, 0, sizeof(array));
    na_rc = bind_parameters_.get_next(&bind_parameters_, &array);
    if (na_rc != 0) {
      SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
      schema.release(&schema);
      return status::Internal("[DB2] Failed to get next batch from bound stream");
    }
    if (!array.release) break;
    if (array.length == 0) {
      array.release(&array);
      continue;
    }

    ArrowArrayView array_view;
    ArrowArrayViewInitFromType(&array_view, NANOARROW_TYPE_STRUCT);
    na_rc = ArrowArrayViewAllocateChildren(&array_view, ncols);
    if (na_rc != NANOARROW_OK) {
      array.release(&array);
      SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
      schema.release(&schema);
      return status::Internal("[DB2] Failed to allocate array view children");
    }
    for (int64_t c = 0; c < ncols; c++) {
      ArrowSchemaView sv;
      struct ArrowError ae = {};
      ArrowSchemaViewInit(&sv, schema.children[c], &ae);
      ArrowArrayViewInitFromType(array_view.children[c], sv.type);
    }
    struct ArrowError set_error = {};
    na_rc = ArrowArrayViewSetArray(&array_view, &array, &set_error);
    if (na_rc != NANOARROW_OK) {
      ArrowArrayViewReset(&array_view);
      array.release(&array);
      SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
      schema.release(&schema);
      return status::Internal("[DB2] Failed to set array on view: ", set_error.message);
    }

    auto nrows = static_cast<SQLULEN>(array.length);
    SQLSetStmtAttr(ins_stmt, SQL_ATTR_PARAMSET_SIZE,
                   reinterpret_cast<SQLPOINTER>(nrows), 0);

    std::vector<std::vector<SQLLEN>> indicators(ncols);
    std::vector<std::vector<char>> data_bufs(ncols);
    Status bind_err = status::Ok();

    for (int64_t c = 0; c < ncols && bind_err.ok(); c++) {
      indicators[c].resize(nrows);
      ArrowArrayView* child = array_view.children[c];
      auto param_num = static_cast<SQLUSMALLINT>(c + 1);

      switch (col_types[c]) {
        case NANOARROW_TYPE_BOOL:
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_UINT8: {
          data_bufs[c].resize(nrows * sizeof(int8_t), 0);
          auto* data = reinterpret_cast<int8_t*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              int64_t v = ArrowArrayViewGetIntUnsafe(child, r);
              data[r] = (col_types[c] == NANOARROW_TYPE_BOOL)
                            ? static_cast<int8_t>(v ? 1 : 0)
                            : static_cast<int8_t>(v);
              indicators[c][r] = 0;
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                ArrowTypeToCType(col_types[c]),
                                ArrowTypeToSqlType(col_types[c]), 0, 0, data,
                                sizeof(int8_t), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_UINT16: {
          data_bufs[c].resize(nrows * sizeof(int16_t), 0);
          auto* data = reinterpret_cast<int16_t*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              data[r] = static_cast<int16_t>(ArrowArrayViewGetIntUnsafe(child, r));
              indicators[c][r] = 0;
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                ArrowTypeToCType(col_types[c]),
                                ArrowTypeToSqlType(col_types[c]), 0, 0, data,
                                sizeof(int16_t), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_UINT32: {
          data_bufs[c].resize(nrows * sizeof(int32_t), 0);
          auto* data = reinterpret_cast<int32_t*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              data[r] = static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(child, r));
              indicators[c][r] = 0;
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                ArrowTypeToCType(col_types[c]),
                                ArrowTypeToSqlType(col_types[c]), 0, 0, data,
                                sizeof(int32_t), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_INT64:
        case NANOARROW_TYPE_UINT64:
        case NANOARROW_TYPE_DURATION: {
          data_bufs[c].resize(nrows * sizeof(int64_t), 0);
          auto* data = reinterpret_cast<int64_t*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              data[r] = ArrowArrayViewGetIntUnsafe(child, r);
              indicators[c][r] = 0;
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                ArrowTypeToCType(col_types[c]),
                                ArrowTypeToSqlType(col_types[c]), 0, 0, data,
                                sizeof(int64_t), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_FLOAT: {
          data_bufs[c].resize(nrows * sizeof(float), 0);
          auto* data = reinterpret_cast<float*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              data[r] = static_cast<float>(ArrowArrayViewGetDoubleUnsafe(child, r));
              indicators[c][r] = 0;
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                ArrowTypeToCType(col_types[c]),
                                ArrowTypeToSqlType(col_types[c]), 0, 0, data,
                                sizeof(float), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_DOUBLE: {
          data_bufs[c].resize(nrows * sizeof(double), 0);
          auto* data = reinterpret_cast<double*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              data[r] = ArrowArrayViewGetDoubleUnsafe(child, r);
              indicators[c][r] = 0;
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                ArrowTypeToCType(col_types[c]),
                                ArrowTypeToSqlType(col_types[c]), 0, 0, data,
                                sizeof(double), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_STRING_VIEW: {
          SQLULEN max_len = 1;
          for (SQLULEN r = 0; r < nrows; r++) {
            if (!ArrowArrayViewIsNull(child, r)) {
              ArrowStringView sv = ArrowArrayViewGetStringUnsafe(child, r);
              if (static_cast<SQLULEN>(sv.size_bytes) > max_len)
                max_len = static_cast<SQLULEN>(sv.size_bytes);
            }
          }
          SQLULEN buf_stride = max_len + 1;
          data_bufs[c].resize(nrows * buf_stride, 0);
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              ArrowStringView sv = ArrowArrayViewGetStringUnsafe(child, r);
              std::memcpy(data_bufs[c].data() + r * buf_stride, sv.data, sv.size_bytes);
              indicators[c][r] = static_cast<SQLLEN>(sv.size_bytes);
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT, SQL_C_CHAR,
                                SQL_VARCHAR, max_len, 0, data_bufs[c].data(),
                                static_cast<SQLLEN>(buf_stride), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
        case NANOARROW_TYPE_FIXED_SIZE_BINARY:
        case NANOARROW_TYPE_BINARY_VIEW: {
          SQLULEN max_len = 1;
          for (SQLULEN r = 0; r < nrows; r++) {
            if (!ArrowArrayViewIsNull(child, r)) {
              ArrowBufferView bv = ArrowArrayViewGetBytesUnsafe(child, r);
              if (static_cast<SQLULEN>(bv.size_bytes) > max_len)
                max_len = static_cast<SQLULEN>(bv.size_bytes);
            }
          }
          data_bufs[c].resize(nrows * max_len, 0);
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              ArrowBufferView bv = ArrowArrayViewGetBytesUnsafe(child, r);
              std::memcpy(data_bufs[c].data() + r * max_len, bv.data.data,
                          bv.size_bytes);
              indicators[c][r] = static_cast<SQLLEN>(bv.size_bytes);
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT, SQL_C_BINARY,
                                SQL_VARBINARY, max_len, 0, data_bufs[c].data(),
                                static_cast<SQLLEN>(max_len), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_DATE32: {
          size_t stride = sizeof(SQL_DATE_STRUCT);
          data_bufs[c].resize(nrows * stride, 0);
          auto* data = reinterpret_cast<SQL_DATE_STRUCT*>(data_bufs[c].data());
          static constexpr int32_t kEpochOffset = 719468;
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              int32_t days =
                  static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(child, r));
              int32_t z = days + kEpochOffset;
              int32_t era = (z >= 0 ? z : z - 146096) / 146097;
              unsigned doe = static_cast<unsigned>(z - era * 146097);
              unsigned yoe =
                  (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
              int32_t y = static_cast<int32_t>(yoe) + era * 400;
              unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
              unsigned mp = (5 * doy + 2) / 153;
              unsigned d = doy - (153 * mp + 2) / 5 + 1;
              unsigned m = mp < 10 ? mp + 3 : mp - 9;
              y += (m <= 2);
              data[r].year = static_cast<SQLSMALLINT>(y);
              data[r].month = static_cast<SQLUSMALLINT>(m);
              data[r].day = static_cast<SQLUSMALLINT>(d);
              indicators[c][r] = static_cast<SQLLEN>(stride);
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                SQL_C_TYPE_DATE, SQL_TYPE_DATE, 10, 0, data,
                                static_cast<SQLLEN>(stride), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_TIME64: {
          size_t stride = sizeof(SQL_TIME_STRUCT);
          data_bufs[c].resize(nrows * stride, 0);
          auto* data = reinterpret_cast<SQL_TIME_STRUCT*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              int64_t micros = ArrowArrayViewGetIntUnsafe(child, r);
              int64_t total_secs = (micros / 1000000) % 86400;
              if (total_secs < 0) total_secs += 86400;
              data[r].hour = static_cast<SQLUSMALLINT>(total_secs / 3600);
              data[r].minute =
                  static_cast<SQLUSMALLINT>((total_secs % 3600) / 60);
              data[r].second = static_cast<SQLUSMALLINT>(total_secs % 60);
              indicators[c][r] = static_cast<SQLLEN>(stride);
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                SQL_C_TYPE_TIME, SQL_TYPE_TIME, 8, 0, data,
                                static_cast<SQLLEN>(stride), indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        case NANOARROW_TYPE_TIMESTAMP: {
          size_t stride = sizeof(SQL_TIMESTAMP_STRUCT);
          data_bufs[c].resize(nrows * stride, 0);
          auto* data =
              reinterpret_cast<SQL_TIMESTAMP_STRUCT*>(data_bufs[c].data());
          for (SQLULEN r = 0; r < nrows; r++) {
            if (ArrowArrayViewIsNull(child, r)) {
              indicators[c][r] = SQL_NULL_DATA;
            } else {
              int64_t micros = ArrowArrayViewGetIntUnsafe(child, r);
              time_t secs = static_cast<time_t>(micros / 1000000);
              int32_t frac = static_cast<int32_t>(micros % 1000000);
              if (frac < 0) {
                secs -= 1;
                frac += 1000000;
              }
              struct tm tm_val;
#ifdef _WIN32
              gmtime_s(&tm_val, &secs);
#else
              gmtime_r(&secs, &tm_val);
#endif
              data[r].year = static_cast<SQLSMALLINT>(tm_val.tm_year + 1900);
              data[r].month = static_cast<SQLUSMALLINT>(tm_val.tm_mon + 1);
              data[r].day = static_cast<SQLUSMALLINT>(tm_val.tm_mday);
              data[r].hour = static_cast<SQLUSMALLINT>(tm_val.tm_hour);
              data[r].minute = static_cast<SQLUSMALLINT>(tm_val.tm_min);
              data[r].second = static_cast<SQLUSMALLINT>(tm_val.tm_sec);
              data[r].fraction = static_cast<SQLUINTEGER>(frac) * 1000;
              indicators[c][r] = static_cast<SQLLEN>(stride);
            }
          }
          rc = SQLBindParameter(ins_stmt, param_num, SQL_PARAM_INPUT,
                                SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, 26, 6,
                                data, static_cast<SQLLEN>(stride),
                                indicators[c].data());
          bind_err = CheckRc(SQL_HANDLE_STMT, ins_stmt, rc, "SQLBindParameter");
          break;
        }
        default:
          bind_err = status::NotImplemented(
              "[DB2] Unsupported Arrow type ", static_cast<int>(col_types[c]),
              " for batch ingest column ", c);
          break;
      }
    }

    if (!bind_err.ok()) {
      ArrowArrayViewReset(&array_view);
      array.release(&array);
      SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
      schema.release(&schema);
      return bind_err;
    }

    connection_->RegisterActiveStatement(ins_stmt);
    rc = SQLExecute(ins_stmt);
    connection_->UnregisterActiveStatement();

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
      Status err = Db2Error(SQL_HANDLE_STMT, ins_stmt, "SQLExecute INSERT batch");
      ArrowArrayViewReset(&array_view);
      array.release(&array);
      SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
      schema.release(&schema);
      return err;
    }
    total_rows += static_cast<int64_t>(nrows);

    ArrowArrayViewReset(&array_view);
    array.release(&array);
  }

  SQLFreeHandle(SQL_HANDLE_STMT, ins_stmt);
  schema.release(&schema);
  return total_rows;
}

}  // namespace adbc::db2

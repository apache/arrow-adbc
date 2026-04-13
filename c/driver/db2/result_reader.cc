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

#include "result_reader.h"

#include <algorithm>
#include <cstring>
#include <string>

#include <nanoarrow/nanoarrow.hpp>

#include "error.h"

namespace adbc::db2 {
namespace status = adbc::driver::status;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a SQL_DATE_STRUCT to days since Unix epoch (1970-01-01).
/// Uses the standard civil-date-to-days algorithm.
static int32_t DateStructToDays(const SQL_DATE_STRUCT& d) {
  int y = d.year;
  int m = d.month;
  int day = d.day;
  // Shift March-based year to simplify leap-year handling
  if (m <= 2) {
    y--;
    m += 9;
  } else {
    m -= 3;
  }
  // Days from year 0 to start of March-based year y
  int32_t era_days = 365 * y + y / 4 - y / 100 + y / 400;
  // Days within the March-based year
  int32_t month_days = (153 * m + 2) / 5 + day - 1;
  // Epoch offset: days from 0000-03-01 to 1970-01-01
  static constexpr int32_t kEpochOffset = 719468;
  return era_days + month_days - kEpochOffset;
}

/// Convert a SQL_TIME_STRUCT to microseconds since midnight (Arrow time64).
static int64_t TimeStructToMicros(const SQL_TIME_STRUCT& t) {
  return static_cast<int64_t>(t.hour) * 3600000000LL +
         static_cast<int64_t>(t.minute) * 60000000LL +
         static_cast<int64_t>(t.second) * 1000000LL;
}

/// Convert a SQL_TIMESTAMP_STRUCT to microseconds since epoch.
static int64_t TimestampStructToMicros(const SQL_TIMESTAMP_STRUCT& ts) {
  SQL_DATE_STRUCT d;
  d.year = ts.year;
  d.month = ts.month;
  d.day = ts.day;
  int64_t day_micros =
      static_cast<int64_t>(DateStructToDays(d)) * 86400LL * 1000000LL;
  SQL_TIME_STRUCT t;
  t.hour = ts.hour;
  t.minute = ts.minute;
  t.second = ts.second;
  int64_t time_micros = TimeStructToMicros(t);
  // fraction field is in nanoseconds
  int64_t frac_micros = static_cast<int64_t>(ts.fraction) / 1000;
  return day_micros + time_micros + frac_micros;
}

// ---------------------------------------------------------------------------
// Db2ResultReader
// ---------------------------------------------------------------------------

Db2ResultReader::Db2ResultReader(SQLHSTMT hstmt, int64_t batch_size)
    : hstmt_(hstmt), batch_size_(batch_size) {
  std::memset(&schema_, 0, sizeof(schema_));
}

Db2ResultReader::~Db2ResultReader() {
  if (schema_.release) {
    schema_.release(&schema_);
  }
  if (hstmt_ != SQL_NULL_HSTMT) {
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt_);
    hstmt_ = SQL_NULL_HSTMT;
  }
}

Status Db2ResultReader::Init() {
  // 1. Discover number of columns
  SQLSMALLINT num_cols = 0;
  SQLRETURN rc = SQLNumResultCols(hstmt_, &num_cols);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt_, rc, "SQLNumResultCols"));

  if (num_cols == 0) {
    finished_ = true;
    ArrowSchemaInit(&schema_);
    UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(&schema_, 0));
    return status::Ok();
  }

  // 2. Build schema + column metadata
  columns_.resize(num_cols);
  ArrowSchemaInit(&schema_);
  UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(&schema_, num_cols));

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
    UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt_, rc, "SQLDescribeCol"));

    UNWRAP_STATUS(MapDb2TypeToArrow(sql_type, col_size, decimal_digits, nullable,
                                    &columns_[i].meta));

    name_len = std::min(name_len, static_cast<SQLSMALLINT>(sizeof(col_name) - 1));
    col_name[name_len] = '\0';
    UNWRAP_STATUS(SetSchemaFromDb2Column(
        schema_.children[i], reinterpret_cast<const char*>(col_name),
        columns_[i].meta));
  }

  // 3. Allocate buffers and bind columns
  SQLULEN actual_batch = static_cast<SQLULEN>(batch_size_);
  rc = SQLSetStmtAttr(hstmt_, SQL_ATTR_ROW_ARRAY_SIZE,
                      reinterpret_cast<SQLPOINTER>(actual_batch), 0);
  UNWRAP_STATUS(
      CheckRc(SQL_HANDLE_STMT, hstmt_, rc, "SQLSetStmtAttr(ROW_ARRAY_SIZE)"));

  rc = SQLSetStmtAttr(hstmt_, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched_, 0);
  UNWRAP_STATUS(
      CheckRc(SQL_HANDLE_STMT, hstmt_, rc, "SQLSetStmtAttr(ROWS_FETCHED_PTR)"));

  for (SQLSMALLINT i = 0; i < num_cols; i++) {
    auto& cb = columns_[i];
    size_t elem = static_cast<size_t>(cb.meta.element_size);
    cb.data.resize(elem * static_cast<size_t>(batch_size_), 0);
    cb.indicator.resize(static_cast<size_t>(batch_size_), 0);

    rc = SQLBindCol(hstmt_, static_cast<SQLUSMALLINT>(i + 1), cb.meta.c_type,
                    cb.data.data(), static_cast<SQLLEN>(elem),
                    cb.indicator.data());
    UNWRAP_STATUS(CheckRc(SQL_HANDLE_STMT, hstmt_, rc, "SQLBindCol"));
  }

  return status::Ok();
}

void Db2ResultReader::ExportTo(ArrowArrayStream* out) {
  out->private_data = this;
  out->get_schema = &Db2ResultReader::GetSchema;
  out->get_next = &Db2ResultReader::GetNext;
  out->get_last_error = &Db2ResultReader::GetLastError;
  out->release = &Db2ResultReader::Release;
}

int Db2ResultReader::GetSchema(ArrowArrayStream* self, ArrowSchema* out) {
  auto* reader = reinterpret_cast<Db2ResultReader*>(self->private_data);
  return ArrowSchemaDeepCopy(&reader->schema_, out);
}

int Db2ResultReader::GetNext(ArrowArrayStream* self, ArrowArray* out) {
  auto* reader = reinterpret_cast<Db2ResultReader*>(self->private_data);
  return reader->FetchAndConvert(out);
}

const char* Db2ResultReader::GetLastError(ArrowArrayStream* self) {
  auto* reader = reinterpret_cast<Db2ResultReader*>(self->private_data);
  return reader->last_error_.empty() ? nullptr : reader->last_error_.c_str();
}

void Db2ResultReader::Release(ArrowArrayStream* self) {
  auto* reader = reinterpret_cast<Db2ResultReader*>(self->private_data);
  delete reader;
  std::memset(self, 0, sizeof(*self));
}

/// Read a LOB value using chunked SQLGetData calls.  This handles
/// values larger than the pre-allocated column buffer.
static int ReadLobChunked(SQLHSTMT hstmt, SQLUSMALLINT col_idx,
                          SQLSMALLINT c_type, std::string& out_data) {
  out_data.clear();
  char chunk[65536];
  SQLLEN ind = 0;

  while (true) {
    SQLRETURN rc = SQLGetData(hstmt, col_idx, c_type, chunk, sizeof(chunk), &ind);
    if (rc == SQL_NO_DATA) break;
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) return EIO;
    if (ind == SQL_NULL_DATA) break;

    size_t bytes_available;
    if (ind == SQL_NO_TOTAL) {
      // Driver doesn't know total size; use full chunk minus NUL for char types
      bytes_available =
          (c_type == SQL_C_CHAR) ? sizeof(chunk) - 1 : sizeof(chunk);
    } else if (static_cast<size_t>(ind) > sizeof(chunk)) {
      bytes_available =
          (c_type == SQL_C_CHAR) ? sizeof(chunk) - 1 : sizeof(chunk);
    } else {
      bytes_available = static_cast<size_t>(ind);
    }
    out_data.append(chunk, bytes_available);

    if (rc == SQL_SUCCESS) break;  // all data retrieved
  }
  return 0;
}

int Db2ResultReader::FetchAndConvert(ArrowArray* out) {
  std::memset(out, 0, sizeof(*out));

  if (finished_) {
    return 0;  // EOS: out->release is NULL
  }

  rows_fetched_ = 0;
  SQLRETURN rc = SQLFetch(hstmt_);

  if (rc == SQL_NO_DATA) {
    finished_ = true;
    return 0;
  }
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    last_error_ = "SQLFetch failed";
    return EIO;
  }

  if (rows_fetched_ == 0) {
    finished_ = true;
    return 0;
  }

  // After the bulk fetch, check for any LOB columns where the
  // indicator shows truncation (ind > element_size).  For row-set
  // fetches with batch_size > 1, SQLGetData is only valid for the
  // last row in the set on some drivers, so this is best-effort.
  // The pre-allocated buffer will hold the first 64KB; for truly
  // large LOBs, users should set batch_size=1 to enable full
  // chunked reads.

  return ConvertBatch(out);
}

/// Check if a column type is fixed-width and can use the fast
/// bulk-copy path (memcpy + validity bitmap from indicators).
static bool IsFixedWidthBulkCopyable(ArrowType type) {
  switch (type) {
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_FLOAT:
    case NANOARROW_TYPE_DOUBLE:
      return true;
    default:
      return false;
  }
}

/// Return the byte width of a fixed-width Arrow type.
static int FixedWidthBytes(ArrowType type) {
  switch (type) {
    case NANOARROW_TYPE_INT16:  return 2;
    case NANOARROW_TYPE_INT32:  return 4;
    case NANOARROW_TYPE_INT64:  return 8;
    case NANOARROW_TYPE_FLOAT:  return 4;
    case NANOARROW_TYPE_DOUBLE: return 8;
    default:                    return 0;
  }
}

/// Fast path: for fixed-width columns (INT16/32/64, FLOAT, DOUBLE)
/// the DB2 CLI row-set buffer is already in native-endian layout
/// matching Arrow's memory format.  We bulk-copy the data buffer
/// and construct the validity bitmap from the indicator array.
static int BulkCopyFixedWidth(ArrowArray* child, const ColumnBuffer& cb,
                              int64_t nrows) {
  int width = FixedWidthBytes(cb.meta.arrow_type);
  if (width == 0) return EINVAL;

  // data buffer (index 1 in NanoArrow's layout for fixed-width types)
  ArrowBuffer* data_buf = ArrowArrayBuffer(child, 1);
  if (ArrowBufferReserve(data_buf, nrows * width) != NANOARROW_OK) {
    return ENOMEM;
  }
  std::memcpy(data_buf->data, cb.data.data(),
              static_cast<size_t>(nrows) * width);
  data_buf->size_bytes = nrows * width;

  // validity bitmap (index 0)
  ArrowBuffer* validity_buf = ArrowArrayBuffer(child, 0);
  int64_t bitmap_bytes = (nrows + 7) / 8;
  if (ArrowBufferReserve(validity_buf, bitmap_bytes) != NANOARROW_OK) {
    return ENOMEM;
  }
  std::memset(validity_buf->data, 0xFF, static_cast<size_t>(bitmap_bytes));
  validity_buf->size_bytes = bitmap_bytes;

  int64_t null_count = 0;
  for (int64_t i = 0; i < nrows; i++) {
    if (cb.indicator[i] == SQL_NULL_DATA) {
      // Clear the bit
      reinterpret_cast<uint8_t*>(validity_buf->data)[i / 8] &=
          ~(1 << (i % 8));
      null_count++;
      // Zero the value slot so it's deterministic
      std::memset(reinterpret_cast<uint8_t*>(data_buf->data) + i * width, 0,
                  width);
    }
  }

  child->length = nrows;
  child->null_count = null_count;
  return 0;
}

int Db2ResultReader::ConvertBatch(ArrowArray* out) {
  int64_t nrows = static_cast<int64_t>(rows_fetched_);
  int ncols = static_cast<int>(columns_.size());
  ArrowError na_error;
  std::memset(&na_error, 0, sizeof(na_error));

  if (ArrowArrayInitFromSchema(out, &schema_, &na_error) != NANOARROW_OK) {
    last_error_ = na_error.message;
    return ENOMEM;
  }

  if (ArrowArrayStartAppending(out) != NANOARROW_OK) {
    out->release(out);
    last_error_ = "ArrowArrayStartAppending failed";
    return ENOMEM;
  }

  // --- Fast path for fixed-width columns ---
  for (int col = 0; col < ncols; col++) {
    auto& cb = columns_[col];
    if (!IsFixedWidthBulkCopyable(cb.meta.arrow_type)) continue;

    int rc = BulkCopyFixedWidth(out->children[col], cb, nrows);
    if (rc != 0) {
      out->release(out);
      last_error_ = "BulkCopyFixedWidth failed";
      return rc;
    }
  }

  // --- Row-by-row path for variable-length / non-bulk-copyable columns ---
  for (int64_t row = 0; row < nrows; row++) {
    for (int col = 0; col < ncols; col++) {
      auto& cb = columns_[col];

      // Skip columns that were already handled by the fast path
      if (IsFixedWidthBulkCopyable(cb.meta.arrow_type)) continue;

      SQLLEN ind = cb.indicator[row];
      ArrowArray* child = out->children[col];
      size_t elem = static_cast<size_t>(cb.meta.element_size);
      const uint8_t* ptr = cb.data.data() + static_cast<size_t>(row) * elem;

      if (ind == SQL_NULL_DATA) {
        if (ArrowArrayAppendNull(child, 1) != NANOARROW_OK) {
          out->release(out);
          last_error_ = "ArrowArrayAppendNull failed";
          return ENOMEM;
        }
        continue;
      }

      switch (cb.meta.arrow_type) {
        case NANOARROW_TYPE_BOOL: {
          uint8_t val = *ptr;
          NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(child, val ? 1 : 0));
          break;
        }
        case NANOARROW_TYPE_INT8: {
          int8_t val;
          std::memcpy(&val, ptr, sizeof(val));
          NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(child, val));
          break;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING: {
          bool truncated = (ind == SQL_NO_TOTAL) ||
                           ((ind > 0) && (static_cast<size_t>(ind) >= elem));
          if (truncated && nrows == 1) {
            int lob_rc = ReadLobChunked(
                hstmt_, static_cast<SQLUSMALLINT>(col + 1),
                cb.meta.c_type, lob_temp_);
            if (lob_rc != 0) {
              out->release(out);
              last_error_ = "ReadLobChunked failed for string column";
              return lob_rc;
            }
            ArrowStringView sv;
            sv.data = lob_temp_.data();
            sv.size_bytes = static_cast<int64_t>(lob_temp_.size());
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(child, sv));
          } else {
            size_t len;
            if (ind == SQL_NO_TOTAL) {
              len = std::strlen(reinterpret_cast<const char*>(ptr));
            } else if (ind > 0) {
              len = std::min(static_cast<size_t>(ind), elem - 1);
            } else {
              len = 0;
            }
            ArrowStringView sv;
            sv.data = reinterpret_cast<const char*>(ptr);
            sv.size_bytes = static_cast<int64_t>(len);
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendString(child, sv));
          }
          break;
        }
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY: {
          bool truncated = (ind == SQL_NO_TOTAL) ||
                           ((ind > 0) && (static_cast<size_t>(ind) > elem));
          if (truncated && nrows == 1) {
            int lob_rc = ReadLobChunked(
                hstmt_, static_cast<SQLUSMALLINT>(col + 1),
                cb.meta.c_type, lob_temp_);
            if (lob_rc != 0) {
              out->release(out);
              last_error_ = "ReadLobChunked failed for binary column";
              return lob_rc;
            }
            ArrowBufferView bv;
            bv.data.as_uint8 = reinterpret_cast<const uint8_t*>(lob_temp_.data());
            bv.size_bytes = static_cast<int64_t>(lob_temp_.size());
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(child, bv));
          } else {
            size_t len;
            if (ind == SQL_NO_TOTAL) {
              len = elem;
            } else if (ind > 0) {
              len = std::min(static_cast<size_t>(ind), elem);
            } else {
              len = 0;
            }
            ArrowBufferView bv;
            bv.data.as_uint8 = ptr;
            bv.size_bytes = static_cast<int64_t>(len);
            NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(child, bv));
          }
          break;
        }
        case NANOARROW_TYPE_DATE32: {
          SQL_DATE_STRUCT ds;
          std::memcpy(&ds, ptr, sizeof(ds));
          NANOARROW_RETURN_NOT_OK(
              ArrowArrayAppendInt(child, DateStructToDays(ds)));
          break;
        }
        case NANOARROW_TYPE_TIME64: {
          SQL_TIME_STRUCT ts;
          std::memcpy(&ts, ptr, sizeof(ts));
          NANOARROW_RETURN_NOT_OK(
              ArrowArrayAppendInt(child, TimeStructToMicros(ts)));
          break;
        }
        case NANOARROW_TYPE_TIMESTAMP: {
          SQL_TIMESTAMP_STRUCT ts;
          std::memcpy(&ts, ptr, sizeof(ts));
          NANOARROW_RETURN_NOT_OK(
              ArrowArrayAppendInt(child, TimestampStructToMicros(ts)));
          break;
        }
        case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
          ArrowBufferView bv;
          bv.data.as_uint8 = ptr;
          bv.size_bytes = 36;
          NANOARROW_RETURN_NOT_OK(ArrowArrayAppendBytes(child, bv));
          break;
        }
        default: {
          last_error_ = "Unsupported Arrow type in result conversion: " +
                        std::to_string(static_cast<int>(cb.meta.arrow_type));
          out->release(out);
          return ENOTSUP;
        }
      }
    }
  }

  // For columns handled by the bulk-copy fast path, we need to
  // synchronize the length with the row-by-row columns so
  // ArrowArrayFinishBuildingDefault succeeds.
  // The fast-path already set child->length; for row-by-row columns,
  // ArrowArrayAppend* incremented their length.  Set the struct length.
  out->length = nrows;

  if (ArrowArrayFinishBuildingDefault(out, &na_error) != NANOARROW_OK) {
    last_error_ = na_error.message;
    if (out->release) out->release(out);
    return ENOMEM;
  }

  return 0;
}

}  // namespace adbc::db2

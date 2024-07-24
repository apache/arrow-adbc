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

#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>

#include "copy/reader.h"

namespace adbcpq {

/// \brief A single column in a single row of a result set.
struct PqRecord {
  const char* data;
  const int len;
  const bool is_null;

  std::optional<double> ParseDouble() const {
    char* end;
    double result = std::strtod(data, &end);
    if (errno != 0 || end == data) {
      return std::nullopt;
    }
    return result;
  }
};

// Used by PqResultHelper to provide index-based access to the records within each
// row of a PGresult
class PqResultRow {
 public:
  PqResultRow(PGresult* result, int row_num) : result_(result), row_num_(row_num) {
    ncols_ = PQnfields(result);
  }

  PqRecord operator[](const int& col_num) {
    assert(col_num < ncols_);
    const char* data = PQgetvalue(result_, row_num_, col_num);
    const int len = PQgetlength(result_, row_num_, col_num);
    const bool is_null = PQgetisnull(result_, row_num_, col_num);

    return PqRecord{data, len, is_null};
  }

 private:
  PGresult* result_ = nullptr;
  int row_num_;
  int ncols_;
};

// Helper to manager the lifecycle of a PQResult. The query argument
// will be evaluated as part of the constructor, with the desctructor handling cleanup
// Caller must call Prepare then Execute, checking both for an OK AdbcStatusCode
// prior to iterating
class PqResultHelper {
 public:
  enum class Format {
    kText = 0,
    kBinary = 1,
  };

  explicit PqResultHelper(PGconn* conn, std::string query)
      : conn_(conn), query_(std::move(query)) {}

  PqResultHelper(PqResultHelper&& other)
      : PqResultHelper(other.conn_, std::move(other.query_)) {
    result_ = other.result_;
    other.result_ = nullptr;
  }

  ~PqResultHelper();

  void set_param_format(Format format) { param_format_ = format; }
  void set_output_format(Format format) { output_format_ = format; }

  AdbcStatusCode Prepare(struct AdbcError* error, int n_params = 0,
                         PostgresType* param_types = nullptr);
  AdbcStatusCode DescribePrepared(struct AdbcError* error);
  AdbcStatusCode ExecutePrepared(struct AdbcError* error,
                                 const std::vector<std::string>& params = {});
  AdbcStatusCode Execute(struct AdbcError* error,
                         const std::vector<std::string>& params = {},
                         PostgresType* param_types = nullptr);
  AdbcStatusCode ExecuteCopy(struct AdbcError* error);
  AdbcStatusCode ResolveParamTypes(PostgresTypeResolver& type_resolver,
                                   PostgresType* param_types, struct AdbcError* error);
  AdbcStatusCode ResolveOutputTypes(PostgresTypeResolver& type_resolver,
                                    PostgresType* result_types, struct AdbcError* error);

  bool HasResult() { return result_ != nullptr; }

  PGresult* ReleaseResult();

  void ClearResult() {
    PQclear(result_);
    result_ = nullptr;
  }

  int64_t AffectedRows();

  int NumRows() const { return PQntuples(result_); }

  int NumColumns() const { return PQnfields(result_); }

  const char* FieldName(int column_number) const {
    return PQfname(result_, column_number);
  }
  Oid FieldType(int column_number) const { return PQftype(result_, column_number); }

  class iterator {
    const PqResultHelper& outer_;
    int curr_row_ = 0;

   public:
    explicit iterator(const PqResultHelper& outer, int curr_row = 0)
        : outer_(outer), curr_row_(curr_row) {}
    iterator& operator++() {
      curr_row_++;
      return *this;
    }
    iterator operator++(int) {
      iterator retval = *this;
      ++(*this);
      return retval;
    }
    bool operator==(iterator other) const {
      return outer_.result_ == other.outer_.result_ && curr_row_ == other.curr_row_;
    }
    bool operator!=(iterator other) const { return !(*this == other); }
    PqResultRow operator*() { return PqResultRow(outer_.result_, curr_row_); }
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::vector<PqResultRow>;
    using pointer = const std::vector<PqResultRow>*;
    using reference = const std::vector<PqResultRow>&;
  };

  iterator begin() { return iterator(*this); }
  iterator end() { return iterator(*this, NumRows()); }

 private:
  PGresult* result_ = nullptr;
  PGconn* conn_;
  std::string query_;
  Format param_format_ = Format::kText;
  Format output_format_ = Format::kText;
};

class PqResultArrayReader {
 public:
  PqResultArrayReader(PGconn* conn, std::shared_ptr<PostgresTypeResolver> type_resolver,
                      std::string query)
      : helper_(conn, std::move(query)), type_resolver_(type_resolver) {
    ArrowErrorInit(&na_error_);
    error_ = ADBC_ERROR_INIT;
  }

  ~PqResultArrayReader() { ResetErrors(); }

  int GetSchema(struct ArrowSchema* out);
  int GetNext(struct ArrowArray* out);
  const char* GetLastError();

  AdbcStatusCode ToArrayStream(int64_t* affected_rows, struct ArrowArrayStream* out,
                               struct AdbcError* error);

  AdbcStatusCode Initialize(struct AdbcError* error);

 private:
  PqResultHelper helper_;
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
  std::vector<std::unique_ptr<PostgresCopyFieldReader>> field_readers_;
  nanoarrow::UniqueSchema schema_;
  struct AdbcError error_;
  struct ArrowError na_error_;

  explicit PqResultArrayReader(PqResultArrayReader* other)
      : helper_(std::move(other->helper_)),
        type_resolver_(std::move(other->type_resolver_)),
        field_readers_(std::move(other->field_readers_)),
        schema_(std::move(other->schema_)) {
    ArrowErrorInit(&na_error_);
    error_ = ADBC_ERROR_INIT;
  }

  void ResetErrors() {
    ArrowErrorInit(&na_error_);

    if (error_.private_data != nullptr) {
      error_.release(&error_);
    }
    error_ = ADBC_ERROR_INIT;
  }
};

}  // namespace adbcpq

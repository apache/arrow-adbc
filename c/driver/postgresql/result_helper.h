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
#include <charconv>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>

#include "copy/reader.h"
#include "driver/framework/status.h"

using adbc::driver::Result;
using adbc::driver::Status;

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

  Result<int64_t> ParseInteger() const {
    const char* last = data + len;
    int64_t value = 0;
    auto result = std::from_chars(data, last, value, 10);
    if (result.ec == std::errc() && result.ptr == last) {
      return value;
    } else {
      return Status::Internal("Can't parse '", data, "' as integer");
    }
  }

  Result<std::vector<std::string>> ParseTextArray() const {
    std::string text_array(data, len);
    text_array.erase(0, 1);
    text_array.erase(text_array.size() - 1);

    std::vector<std::string> elements;
    std::stringstream ss(std::move(text_array));
    std::string tmp;

    while (getline(ss, tmp, ',')) {
      elements.push_back(std::move(tmp));
    }

    return elements;
  }

  std::string_view value() { return std::string_view(data, len); }
};

// Used by PqResultHelper to provide index-based access to the records within each
// row of a PGresult
class PqResultRow {
 public:
  PqResultRow() : result_(nullptr), row_num_(-1) {}
  PqResultRow(PGresult* result, int row_num) : result_(result), row_num_(row_num) {}

  PqRecord operator[](int col_num) const {
    assert(col_num < PQnfields(result_));
    const char* data = PQgetvalue(result_, row_num_, col_num);
    const int len = PQgetlength(result_, row_num_, col_num);
    const bool is_null = PQgetisnull(result_, row_num_, col_num);

    return PqRecord{data, len, is_null};
  }

  bool IsValid() const {
    return result_ && row_num_ >= 0 && row_num_ < PQntuples(result_);
  }

  PqResultRow Next() const { return PqResultRow(result_, row_num_ + 1); }

 private:
  PGresult* result_ = nullptr;
  int row_num_;
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

  Status Prepare() const;
  Status Prepare(const std::vector<Oid>& param_oids) const;
  Status DescribePrepared();
  Status Execute(const std::vector<std::string>& params = {},
                 PostgresType* param_types = nullptr);
  Status ExecuteCopy();
  Status ResolveParamTypes(PostgresTypeResolver& type_resolver,
                           PostgresType* param_types);
  Status ResolveOutputTypes(PostgresTypeResolver& type_resolver,
                            PostgresType* result_types);

  bool HasResult() const { return result_ != nullptr; }

  void SetResult(PGresult* result) {
    ClearResult();
    result_ = result;
  }

  PGresult* ReleaseResult();

  void ClearResult() {
    PQclear(result_);
    result_ = nullptr;
  }

  int64_t AffectedRows() const;

  int NumRows() const { return PQntuples(result_); }

  int NumColumns() const { return PQnfields(result_); }

  const char* FieldName(int column_number) const {
    return PQfname(result_, column_number);
  }
  Oid FieldType(int column_number) const { return PQftype(result_, column_number); }
  PqResultRow Row(int i) const { return PqResultRow(result_, i); }

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
    PqResultRow operator*() const { return PqResultRow(outer_.result_, curr_row_); }
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::vector<PqResultRow>;
    using pointer = const std::vector<PqResultRow>*;
    using reference = const std::vector<PqResultRow>&;
  };

  iterator begin() const { return iterator(*this); }
  iterator end() const { return iterator(*this, NumRows()); }

 private:
  PGresult* result_ = nullptr;
  PGconn* conn_;
  std::string query_;
  Format param_format_ = Format::kText;
  Format output_format_ = Format::kText;

  Status PrepareInternal(int n_params, const Oid* param_oids) const;
};

}  // namespace adbcpq

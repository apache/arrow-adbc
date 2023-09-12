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
#include <string>
#include <utility>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>

namespace adbcpq {

/// \brief A single column in a single row of a result set.
struct PqRecord {
  const char* data;
  const int len;
  const bool is_null;

  // XXX: can't use optional due to R
  std::pair<bool, double> ParseDouble() const {
    char* end;
    double result = std::strtod(data, &end);
    if (errno != 0 || end == data) {
      return std::make_pair(false, 0.0);
    }
    return std::make_pair(true, result);
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
  explicit PqResultHelper(PGconn* conn, std::string query, struct AdbcError* error)
      : conn_(conn), query_(std::move(query)), error_(error) {}

  explicit PqResultHelper(PGconn* conn, std::string query,
                          std::vector<std::string> param_values, struct AdbcError* error)
      : conn_(conn),
        query_(std::move(query)),
        param_values_(std::move(param_values)),
        error_(error) {}

  ~PqResultHelper();

  AdbcStatusCode Prepare();
  AdbcStatusCode Execute();

  int NumRows() const { return PQntuples(result_); }

  int NumColumns() const { return PQnfields(result_); }

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
  std::vector<std::string> param_values_;
  struct AdbcError* error_;
};
}  // namespace adbcpq

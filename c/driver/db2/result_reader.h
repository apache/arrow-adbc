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
#include <memory>
#include <string>
#include <vector>

#include "db2_odbc.h"

#include <nanoarrow/nanoarrow.h>

#include "driver/framework/status.h"
#include "type_mapping.h"

namespace adbc::db2 {

using driver::Status;

/// Per-column fetch buffer: holds the raw data from SQLFetch row-set
/// binding plus the indicator/length array.
struct ColumnBuffer {
  Db2Column meta;
  std::vector<uint8_t> data;     // element_size * batch_size bytes
  std::vector<SQLLEN> indicator; // one SQLLEN per row
};

/// Reads results from a DB2 statement handle into Arrow RecordBatches
/// delivered through an ArrowArrayStream.
///
/// The reader:
///  1. Discovers the schema via SQLDescribeCol.
///  2. Allocates column-wise row-set buffers and calls SQLBindCol.
///  3. Sets SQL_ATTR_ROW_ARRAY_SIZE for batch fetching.
///  4. On each get_next() call, invokes SQLFetch to fill the buffers,
///     then converts the buffers into a NanoArrow ArrowArray.
///
/// The reader takes ownership of the SQLHSTMT and frees it on release.
class Db2ResultReader {
 public:
  explicit Db2ResultReader(SQLHSTMT hstmt, int64_t batch_size);
  ~Db2ResultReader();

  /// Discover schema, allocate buffers, bind columns, set row-array size.
  Status Init();

  /// Install this reader as an ArrowArrayStream.  After this call the
  /// stream owns the reader and will delete it on release.
  void ExportTo(ArrowArrayStream* out);

 private:
  SQLHSTMT hstmt_;
  int64_t batch_size_;
  bool finished_ = false;
  std::string last_error_;

  ArrowSchema schema_;
  std::vector<ColumnBuffer> columns_;
  SQLULEN rows_fetched_ = 0;
  std::string lob_temp_;  // reusable buffer for chunked LOB reads

  // ArrowArrayStream callbacks
  static int GetSchema(ArrowArrayStream* self, ArrowSchema* out);
  static int GetNext(ArrowArrayStream* self, ArrowArray* out);
  static const char* GetLastError(ArrowArrayStream* self);
  static void Release(ArrowArrayStream* self);

  /// Fetch one batch from DB2 and convert to an ArrowArray.
  int FetchAndConvert(ArrowArray* out);

  /// Convert the row-set buffers for one batch (rows_fetched_ rows) to
  /// a NanoArrow array.
  int ConvertBatch(ArrowArray* out);
};

}  // namespace adbc::db2

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

#include <cinttypes>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include <adbc.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/util/future.h>
#include <arrow/util/key_value_metadata.h>

#include "common/utils.h"

namespace adbc_bigquery {
class InputStream : virtual public arrow::io::InputStream {
 public:
  InputStream(const std::string_view& serialized_schema,
              const std::string_view& serialized_record_batch)
      : serialized_schema_(serialized_schema),
        serialized_record_batch_(serialized_record_batch) {
    schema_size_ = serialized_schema_.length();
    record_batch_size_ = serialized_record_batch_.length();
  }
  virtual ~InputStream() {}

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    if (position_ < schema_size_) {
      int64_t to_read = std::min(nbytes, schema_size_ - position_);
      std::memcpy(out, serialized_schema_.data() + position_, to_read);
      position_ += to_read;
      return to_read;
    } else if (position_ < schema_size_ + record_batch_size_) {
      int64_t to_read = std::min(nbytes, schema_size_ + record_batch_size_ - position_);
      std::memcpy(out, serialized_record_batch_.data() + position_ - schema_size_,
                  to_read);
      position_ += to_read;
      return to_read;
    } else {
      return 0;
    }
  }
  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> Tell() const override { return position_; }
  bool closed() const override { return closed_; }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    if (position_ < schema_size_) {
      int64_t to_read = std::min(nbytes, schema_size_ - position_);
      auto buffer = std::make_shared<arrow::Buffer>(
          (const uint8_t*)serialized_schema_.data() + position_, to_read);
      position_ += to_read;
      return buffer;
    } else if (position_ < schema_size_ + record_batch_size_) {
      int64_t to_read = std::min(nbytes, schema_size_ + record_batch_size_ - position_);
      auto buffer = std::make_shared<arrow::Buffer>(
          (const uint8_t*)serialized_record_batch_.data() + position_ - schema_size_,
          to_read);
      position_ += to_read;
      return buffer;
    } else {
      return std::make_shared<arrow::Buffer>((const uint8_t*)nullptr, 0);
    }
  }

 protected:
  bool closed_ = false;
  int64_t position_ = 0;
  int64_t schema_size_ = 0;
  int64_t record_batch_size_ = 0;
  std::string_view serialized_schema_;
  std::string_view serialized_record_batch_;
};

}  // namespace adbc_bigquery

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

#include "readrowsiterator.h"

#include <cinttypes>
#include <memory>

#include <adbc.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/reader.h>
#include <google/cloud/bigquery/storage/v1/bigquery_read_client.h>
#include <nanoarrow/nanoarrow.hpp>

#include "common/options.h"
#include "common/utils.h"
#include "connection.h"
#include "database.h"
#include "inputstream.h"

namespace adbc_bigquery {
AdbcStatusCode ReadRowsIterator::init(struct AdbcError* error) {
  connection_ = ::google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection();
  client_ = std::make_shared<::google::cloud::bigquery_storage_v1::BigQueryReadClient>(
      connection_);
  session_ = std::make_shared<::google::cloud::bigquery::storage::v1::ReadSession>();
  session_->set_data_format(::google::cloud::bigquery::storage::v1::DataFormat::ARROW);
  session_->set_table(table_name_);

  constexpr std::int32_t kMaxReadStreams = 1;
  auto session = client_->CreateReadSession(project_name_, *session_, kMaxReadStreams);
  if (!session) {
    auto& status = session.status();
    SetError(error, "%s%" PRId32 ", %s",
             "[bigquery] Cannot create read session: code=", status.code(),
             status.message().c_str());
    return ADBC_STATUS_INVALID_STATE;
  }

  auto response = client_->ReadRows(session->streams(0).name(), 0);
  response_ = std::make_shared<ReadRowsResponse>(std::move(response));
  session_ =
      std::make_shared<::google::cloud::bigquery::storage::v1::ReadSession>(*session);
  current_ = response_->begin();
  this->read_next();

  return ADBC_STATUS_OK;
}

AdbcStatusCode ReadRowsIterator::read_next() {
  if (this->current_ == this->response_->end()) {
    return ADBC_STATUS_NOT_FOUND;
  }
  auto& row = *this->current_;
  if (!row.ok()) {
    return ADBC_STATUS_INTERNAL;
  }

  this->serialized_schema_ = session_->arrow_schema().serialized_schema();
  this->serialized_record_batch_ = row->arrow_record_batch().serialized_record_batch();
  this->input_stream_ = std::make_shared<adbc_bigquery::InputStream>(
      this->serialized_schema_, this->serialized_record_batch_);
  auto result = arrow::ipc::RecordBatchStreamReader::Open(this->input_stream_);
  if (!result.ok()) {
    return ADBC_STATUS_INTERNAL;
  }
  this->reader_ = std::move(result).ValueOrDie();

  auto p = reader_->ReadNext(&this->record_batch);
  if (!p.ok()) {
    std::cerr << "Error: could not read RecordBatch\n";
    return ADBC_STATUS_INTERNAL;
  }

  this->current_++;
  return ADBC_STATUS_OK;
}

int ReadRowsIterator::get_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  if (!stream || !out) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
  if (!ptr) {
    return ADBC_STATUS_INVALID_STATE;
  }

  std::shared_ptr<ReadRowsIterator>& iterator = *ptr;
  if (iterator->parsed_array_ == nullptr) {
    iterator->parsed_array_ = (struct ArrowArray*)ArrowMalloc(sizeof(struct ArrowArray));
    memset(iterator->parsed_array_, 0, sizeof(struct ArrowArray));
    auto pp = arrow::ExportRecordBatch(*iterator->record_batch.get(), out);
    if (!pp.ok()) {
      std::cerr << "Error: could not export RecordBatch\n";
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  } else {
    memset(out, 0, sizeof(struct ArrowArray));
    return ADBC_STATUS_OK;
  }
}

int ReadRowsIterator::get_schema(struct ArrowArrayStream* stream,
                                 struct ArrowSchema* out) {
  if (!stream || !out) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
  if (!ptr) {
    return ADBC_STATUS_INVALID_STATE;
  }

  std::shared_ptr<ReadRowsIterator>& iterator = *ptr;
  if (iterator->parsed_schema_ == nullptr) {
    iterator->parsed_schema_ =
        (struct ArrowSchema*)ArrowMalloc(sizeof(struct ArrowSchema));
    memset(iterator->parsed_schema_, 0, sizeof(struct ArrowArray));
    iterator->parsed_array_ = (struct ArrowArray*)ArrowMalloc(sizeof(struct ArrowArray));
    memset(iterator->parsed_array_, 0, sizeof(struct ArrowArray));
    auto pp = arrow::ExportRecordBatch(*iterator->record_batch.get(),
                                       iterator->parsed_array_, out);
    if (!pp.ok()) {
      std::cerr << "Error: could not export RecordBatch\n";
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  } else {
    memset(out, 0, sizeof(struct ArrowSchema));
    out->format = "+s";
    out->n_children = 1;
    return ADBC_STATUS_OK;
  }
}

void ReadRowsIterator::release(struct ArrowArrayStream* stream) {
  if (stream && stream->private_data) {
    auto* ptr =
        reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
    if (ptr) {
      // struct ArrowSchema * schema = (*ptr)->parsed_schema_;
      // if (schema && schema->release) {
      //   schema->release(schema);
      //   (*ptr)->parsed_schema_ = nullptr;
      // }
      // struct ArrowArray * array = (*ptr)->parsed_array_;
      // if (array && array->release) {
      //   array->release(array);
      //   (*ptr)->parsed_schema_ = nullptr;
      // }
      delete ptr;
    }
    stream->private_data = nullptr;
  }
}

}  // namespace adbc_bigquery

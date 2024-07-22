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

#include "result_helper.h"

#include "copy/reader.h"
#include "driver/common/utils.h"
#include "error.h"

namespace adbcpq {

PqResultHelper::~PqResultHelper() {
  if (result_ != nullptr) {
    PQclear(result_);
  }
}

AdbcStatusCode PqResultHelper::Prepare() {
  // TODO: make stmtName a unique identifier?
  PGresult* result =
      PQprepare(conn_, /*stmtName=*/"", query_.c_str(), param_values_.size(), NULL);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code =
        SetError(error_, result, "[libpq] Failed to prepare query: %s\nQuery was:%s",
                 PQerrorMessage(conn_), query_.c_str());
    PQclear(result);
    return code;
  }

  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::Execute(int output_format) {
  std::vector<const char*> param_c_strs;

  for (size_t index = 0; index < param_values_.size(); index++) {
    param_c_strs.push_back(param_values_[index].c_str());
  }

  result_ = PQexecPrepared(conn_, "", param_values_.size(), param_c_strs.data(), NULL,
                           NULL, output_format);

  ExecStatusType status = PQresultStatus(result_);
  if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
    AdbcStatusCode error =
        SetError(error_, result_, "[libpq] Failed to execute query '%s': %s",
                 query_.c_str(), PQerrorMessage(conn_));
    return error;
  }

  return ADBC_STATUS_OK;
}

int PqResultArrayReader::GetSchema(struct ArrowSchema* out) {
  ResetErrors();

  if (schema_->release == nullptr) {
    AdbcStatusCode status = Initialize();
    if (status != ADBC_STATUS_OK) {
      return EINVAL;
    }
  }

  return ArrowSchemaDeepCopy(schema_.get(), out);
}

int PqResultArrayReader::GetNext(struct ArrowArray* out) {
  ResetErrors();

  if (schema_->release == nullptr) {
    AdbcStatusCode status = Initialize();
    if (status != ADBC_STATUS_OK) {
      return EINVAL;
    }
  }

  nanoarrow::UniqueArray tmp;
  NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_.get(), &na_error_));
  for (int i = 0; i < helper_.NumColumns(); i++) {
    NANOARROW_RETURN_NOT_OK(field_readers_[i]->InitArray(tmp->children[i]));
  }

  // TODO: If we get an EOVERFLOW here (e.g., big string data), we
  // would need to keep track of what row number we're on and start
  // from there instead of begin() on the next call
  struct ArrowBufferView item;
  for (auto it = helper_.begin(); it != helper_.end(); it++) {
    auto row = *it;
    for (int i = 0; i < helper_.NumColumns(); i++) {
      auto pg_item = row[i];
      item.data.data = pg_item.data;
      item.size_bytes = pg_item.len;
      NANOARROW_RETURN_NOT_OK(
          field_readers_[i]->Read(&item, item.size_bytes, tmp->children[i], &na_error_));
    }
  }

  for (int i = 0; i < helper_.NumColumns(); i++) {
    NANOARROW_RETURN_NOT_OK(field_readers_[i]->FinishArray(tmp->children[i], &na_error_));
  }

  NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), &na_error_));

  ArrowArrayMove(tmp.get(), out);
  return NANOARROW_OK;
}

const char* PqResultArrayReader::GetLastError() {
  if (error_.message != nullptr) {
    return error_.message;
  } else {
    return na_error_.message;
  }
}

AdbcStatusCode PqResultArrayReader::Initialize() {
  RAISE_ADBC(helper_.Prepare());
  RAISE_ADBC(helper_.Execute(1));

  ArrowSchemaInit(schema_.get());
  CHECK_NA_DETAIL(INTERNAL, ArrowSchemaSetTypeStruct(schema_.get(), helper_.NumColumns()),
                  &na_error_, &error_);

  for (int i = 0; i < helper_.NumColumns(); i++) {
    PostgresType child_type;
    CHECK_NA_DETAIL(INTERNAL,
                    type_resolver_->Find(helper_.FieldType(i), &child_type, &na_error_),
                    &na_error_, &error_);

    std::unique_ptr<PostgresCopyFieldReader> child_reader;
    CHECK_NA_DETAIL(
        INTERNAL,
        MakeCopyFieldReader(child_type, schema_->children[i], &child_reader, &na_error_),
        &na_error_, &error_);

    CHECK_NA_DETAIL(INTERNAL, child_reader->InitSchema(schema_->children[i]), &na_error_,
                    &error_);

    field_readers_.push_back(std::move(child_reader));
  }

  return ADBC_STATUS_OK;
}

}  // namespace adbcpq

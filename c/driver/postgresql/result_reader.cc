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

#include "copy/reader.h"
#include "driver/common/utils.h"

#include "error.h"

namespace adbcpq {

int PqResultArrayReader::GetSchema(struct ArrowSchema* out) {
  ResetErrors();

  if (schema_->release == nullptr) {
    AdbcStatusCode status = Initialize(&error_);
    if (status != ADBC_STATUS_OK) {
      return EINVAL;
    }
  }

  return ArrowSchemaDeepCopy(schema_.get(), out);
}

int PqResultArrayReader::GetNext(struct ArrowArray* out) {
  ResetErrors();

  if (schema_->release == nullptr) {
    AdbcStatusCode status = Initialize(&error_);
    if (status != ADBC_STATUS_OK) {
      return EINVAL;
    }
  }

  if (!helper_.HasResult()) {
    out->release = nullptr;
    return NANOARROW_OK;
  }

  nanoarrow::UniqueArray tmp;
  NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_.get(), &na_error_));
  NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
  for (int i = 0; i < helper_.NumColumns(); i++) {
    NANOARROW_RETURN_NOT_OK(field_readers_[i]->InitArray(tmp->children[i]));
  }

  // TODO: If we get an EOVERFLOW here (e.g., big string data), we
  // would need to keep track of what row number we're on and start
  // from there instead of begin() on the next call. We could also
  // respect the size hint here to chunk the batches.
  struct ArrowBufferView item;
  for (auto it = helper_.begin(); it != helper_.end(); it++) {
    auto row = *it;
    for (int i = 0; i < helper_.NumColumns(); i++) {
      auto pg_item = row[i];
      item.data.data = pg_item.data;

      if (pg_item.is_null) {
        item.size_bytes = -1;
      } else {
        item.size_bytes = pg_item.len;
      }

      NANOARROW_RETURN_NOT_OK(
          field_readers_[i]->Read(&item, item.size_bytes, tmp->children[i], &na_error_));
    }
  }

  for (int i = 0; i < helper_.NumColumns(); i++) {
    NANOARROW_RETURN_NOT_OK(field_readers_[i]->FinishArray(tmp->children[i], &na_error_));
  }

  tmp->length = helper_.NumRows();
  tmp->null_count = 0;
  NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), &na_error_));

  // Ensure that the next call to GetNext() will signal the end of the stream
  helper_.ClearResult();

  // Canonically return zero-size results as an empty stream
  if (tmp->length == 0) {
    out->release = nullptr;
    return NANOARROW_OK;
  }

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

AdbcStatusCode PqResultArrayReader::Initialize(struct AdbcError* error) {
  helper_.set_output_format(PqResultHelper::Format::kBinary);
  RAISE_ADBC(helper_.Execute(error));

  ArrowSchemaInit(schema_.get());
  CHECK_NA_DETAIL(INTERNAL, ArrowSchemaSetTypeStruct(schema_.get(), helper_.NumColumns()),
                  &na_error_, error);

  for (int i = 0; i < helper_.NumColumns(); i++) {
    PostgresType child_type;
    CHECK_NA_DETAIL(INTERNAL,
                    type_resolver_->Find(helper_.FieldType(i), &child_type, &na_error_),
                    &na_error_, error);

    CHECK_NA(INTERNAL, child_type.SetSchema(schema_->children[i]), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(schema_->children[i], helper_.FieldName(i)),
             error);

    std::unique_ptr<PostgresCopyFieldReader> child_reader;
    CHECK_NA_DETAIL(
        INTERNAL,
        MakeCopyFieldReader(child_type, schema_->children[i], &child_reader, &na_error_),
        &na_error_, error);

    child_reader->Init(child_type);
    CHECK_NA_DETAIL(INTERNAL, child_reader->InitSchema(schema_->children[i]), &na_error_,
                    error);

    field_readers_.push_back(std::move(child_reader));
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultArrayReader::ToArrayStream(int64_t* affected_rows,
                                                  struct ArrowArrayStream* out,
                                                  struct AdbcError* error) {
  if (out == nullptr) {
    // If there is no output requested, we still need to execute and set
    // affected_rows if needed. We don't need an output schema or to set
    // up a copy reader, so we can skip those steps by going straight
    // to Execute(). This also enables us to support queries with multiple
    // statements because we can call PQexec() instead of PQexecParams().
    RAISE_ADBC(helper_.Execute(error));

    if (affected_rows != nullptr) {
      *affected_rows = helper_.AffectedRows();
    }

    return ADBC_STATUS_OK;
  }

  // Execute eagerly. We need this to provide row counts for DELETE and
  // CREATE TABLE queries as well as to provide more informative errors
  // until this reader class is wired up to provide extended AdbcError
  // information.
  RAISE_ADBC(Initialize(error));
  if (affected_rows != nullptr) {
    *affected_rows = helper_.AffectedRows();
  }

  nanoarrow::ArrayStreamFactory<PqResultArrayReader>::InitArrayStream(
      new PqResultArrayReader(this), out);

  return ADBC_STATUS_OK;
}

}  // namespace adbcpq

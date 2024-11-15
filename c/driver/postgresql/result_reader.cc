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

#include <memory>
#include <utility>

#include "copy/reader.h"
#include "driver/framework/status.h"

namespace adbcpq {

int PqResultArrayReader::GetSchema(struct ArrowSchema* out) {
  ResetErrors();

  if (schema_->release == nullptr) {
    Status status = Initialize(nullptr);
    if (!status.ok()) {
      status.ToAdbc(&error_);
      return EINVAL;
    }
  }

  return ArrowSchemaDeepCopy(schema_.get(), out);
}

int PqResultArrayReader::GetNext(struct ArrowArray* out) {
  ResetErrors();

  Status status;
  if (schema_->release == nullptr) {
    status = Initialize(nullptr);
    if (!status.ok()) {
      status.ToAdbc(&error_);
      return EINVAL;
    }
  }

  // If don't already have a result, populate it by binding the next row
  // in the bind stream. If this is the first call to GetNext(), we have
  // already populated the result.
  if (!helper_.HasResult()) {
    // If there was no bind stream provided or the existing bind stream has been
    // exhausted, we are done.
    if (!bind_stream_) {
      out->release = nullptr;
      return NANOARROW_OK;
    }

    // Keep binding and executing until we have a result to return
    status = BindNextAndExecute(nullptr);
    if (!status.ok()) {
      status.ToAdbc(&error_);
      return EIO;
    }

    // It's possible that there is still nothing to do here
    if (!helper_.HasResult()) {
      out->release = nullptr;
      return NANOARROW_OK;
    }
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

      NANOARROW_RETURN_NOT_OK(field_readers_[i]->Read(
          &item, static_cast<int32_t>(item.size_bytes), tmp->children[i], &na_error_));
    }
  }

  for (int i = 0; i < helper_.NumColumns(); i++) {
    NANOARROW_RETURN_NOT_OK(field_readers_[i]->FinishArray(tmp->children[i], &na_error_));
  }

  tmp->length = helper_.NumRows();
  tmp->null_count = 0;
  NANOARROW_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(tmp.get(), &na_error_));

  // Signal that the next call to GetNext() will have to populate the result again
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

Status PqResultArrayReader::Initialize(int64_t* rows_affected) {
  helper_.set_output_format(PqResultHelper::Format::kBinary);
  helper_.set_param_format(PqResultHelper::Format::kBinary);

  // If we have to do binding, set up the bind stream an execute until
  // there is a result with more than zero rows to populate.
  if (bind_stream_) {
    UNWRAP_STATUS(bind_stream_->Begin([] { return Status::Ok(); }));

    UNWRAP_STATUS(bind_stream_->SetParamTypes(conn_, *type_resolver_, autocommit_));
    UNWRAP_STATUS(helper_.Prepare(bind_stream_->param_types));
    UNWRAP_STATUS(BindNextAndExecute(nullptr));

    // If there were no arrays in the bind stream, we still need a result
    // to populate the schema. If there were any arrays in the bind stream,
    // the last one will still be in helper_ even if it had zero rows.
    if (!helper_.HasResult()) {
      UNWRAP_STATUS(helper_.DescribePrepared());
    }

    // We can't provide affected row counts if there is a bind stream and
    // an output because we don't know how many future bind arrays/rows there
    // might be.
    if (rows_affected != nullptr) {
      *rows_affected = -1;
    }
  } else {
    UNWRAP_STATUS(helper_.Execute());
    if (rows_affected != nullptr) {
      *rows_affected = helper_.AffectedRows();
    }
  }

  // Build the schema for which we are about to build results
  ArrowSchemaInit(schema_.get());
  UNWRAP_NANOARROW(na_error_, Internal,
                   ArrowSchemaSetTypeStruct(schema_.get(), helper_.NumColumns()));

  for (int i = 0; i < helper_.NumColumns(); i++) {
    PostgresType child_type;
    UNWRAP_ERRNO(Internal,
                 type_resolver_->FindWithDefault(helper_.FieldType(i), &child_type));

    UNWRAP_ERRNO(Internal, child_type.SetSchema(schema_->children[i], vendor_name_));
    UNWRAP_ERRNO(Internal,
                 ArrowSchemaSetName(schema_->children[i], helper_.FieldName(i)));

    std::unique_ptr<PostgresCopyFieldReader> child_reader;
    UNWRAP_NANOARROW(
        na_error_, Internal,
        MakeCopyFieldReader(child_type, schema_->children[i], &child_reader, &na_error_));

    child_reader->Init(child_type);
    UNWRAP_NANOARROW(na_error_, Internal, child_reader->InitSchema(schema_->children[i]));

    field_readers_.push_back(std::move(child_reader));
  }

  return Status::Ok();
}

Status PqResultArrayReader::ToArrayStream(int64_t* affected_rows,
                                          struct ArrowArrayStream* out) {
  if (out == nullptr) {
    // If there is no output requested, we still need to execute and
    // set affected_rows if needed. We don't need an output schema or to set up a copy
    // reader, so we can skip those steps by going straight to Execute(). This also
    // enables us to support queries with multiple statements because we can call PQexec()
    // instead of PQexecParams().
    UNWRAP_STATUS(ExecuteAll(affected_rows));
    return Status::Ok();
  }

  // Otherwise, execute until we have a result to return. We need this to provide row
  // counts for DELETE and CREATE TABLE queries as well as to provide more informative
  // errors until this reader class is wired up to provide extended AdbcError information.
  UNWRAP_STATUS(Initialize(affected_rows));

  nanoarrow::ArrayStreamFactory<PqResultArrayReader>::InitArrayStream(
      new PqResultArrayReader(this), out);

  return Status::Ok();
}

Status PqResultArrayReader::BindNextAndExecute(int64_t* affected_rows) {
  // Keep pulling from the bind stream and executing as long as
  // we receive results with zero rows.
  do {
    UNWRAP_STATUS(bind_stream_->EnsureNextRow());

    if (!bind_stream_->current->release) {
      UNWRAP_STATUS(bind_stream_->Cleanup(conn_));
      bind_stream_.reset();
      return Status::Ok();
    }

    PGresult* result;
    UNWRAP_STATUS(bind_stream_->BindAndExecuteCurrentRow(
        conn_, &result, /*result_format*/ kPgBinaryFormat));
    helper_.SetResult(result);
    if (affected_rows) {
      (*affected_rows) += helper_.AffectedRows();
    }
  } while (helper_.NumRows() == 0);

  return Status::Ok();
}

Status PqResultArrayReader::ExecuteAll(int64_t* affected_rows) {
  // For the case where we don't need a result, we either need to exhaust the bind
  // stream (if there is one) or execute the query without binding.
  if (bind_stream_) {
    UNWRAP_STATUS(bind_stream_->Begin([] { return Status::Ok(); }));
    UNWRAP_STATUS(bind_stream_->SetParamTypes(conn_, *type_resolver_, autocommit_));
    UNWRAP_STATUS(helper_.Prepare(bind_stream_->param_types));

    // Reset affected rows to zero before binding and executing any
    if (affected_rows) {
      (*affected_rows) = 0;
    }

    do {
      UNWRAP_STATUS(BindNextAndExecute(affected_rows));
    } while (bind_stream_);
  } else {
    UNWRAP_STATUS(helper_.Execute());

    if (affected_rows != nullptr) {
      *affected_rows = helper_.AffectedRows();
    }
  }

  return Status::Ok();
}

}  // namespace adbcpq

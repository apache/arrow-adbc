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

#include <memory>

#include "copy/reader.h"
#include "driver/common/utils.h"
#include "error.h"

namespace adbcpq {

PqResultHelper::~PqResultHelper() {
  if (result_ != nullptr) {
    PQclear(result_);
  }
}

AdbcStatusCode PqResultHelper::Prepare(int n_params) {
  // TODO: make stmtName a unique identifier?
  PGresult* result = PQprepare(conn_, /*stmtName=*/"", query_.c_str(), n_params, NULL);
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

AdbcStatusCode PqResultHelper::DescribePrepared() {
  PQclear(result_);
  result_ = PQdescribePrepared(conn_, /*stmtName=*/"");
  if (PQresultStatus(result_) != PGRES_COMMAND_OK) {
    AdbcStatusCode code =
        SetError(error_, result_,
                 "[libpq] Failed  to describe prepared statement: %s\nQuery was:%s",
                 PQerrorMessage(conn_), query_.c_str());
    PQclear(result_);
    return code;
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::ExecutePrepared(const std::vector<std::string>& params) {
  std::vector<const char*> param_c_strs;

  for (size_t index = 0; index < params.size(); index++) {
    param_c_strs.push_back(params[index].c_str());
  }

  PQclear(result_);
  result_ = PQexecPrepared(conn_, "", param_c_strs.size(), param_c_strs.data(), NULL,
                           NULL, static_cast<int>(format_));

  ExecStatusType status = PQresultStatus(result_);
  if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
    AdbcStatusCode error =
        SetError(error_, result_, "[libpq] Failed to execute query '%s': %s",
                 query_.c_str(), PQerrorMessage(conn_));
    return error;
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::ResolveParamTypes(PostgresTypeResolver& type_resolver,
                                                 PostgresType* param_types) {
  struct ArrowError na_error;
  ArrowErrorInit(&na_error);

  const int num_params = PQnparams(result_);
  PostgresType root_type(PostgresTypeId::kRecord);

  for (int i = 0; i < num_params; i++) {
    const Oid pg_oid = PQparamtype(result_, i);
    PostgresType pg_type;
    if (type_resolver.Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
      SetError(error_, "%s%d%s%s%s%d", "[libpq] Parameter #", i + 1, " (\"",
               PQfname(result_, i), "\") has unknown type code ", pg_oid);
      PQclear(result_);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    root_type.AppendChild(PQfname(result_, i), pg_type);
  }

  *param_types = root_type;

  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::ResolveOutputTypes(PostgresTypeResolver& type_resolver,
                                                  PostgresType* result_types) {
  struct ArrowError na_error;
  ArrowErrorInit(&na_error);

  const int num_fields = PQnfields(result_);
  PostgresType root_type(PostgresTypeId::kRecord);

  for (int i = 0; i < num_fields; i++) {
    const Oid pg_oid = PQftype(result_, i);
    PostgresType pg_type;
    if (type_resolver.Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
      SetError(error_, "%s%d%s%s%s%d", "[libpq] Column #", i + 1, " (\"",
               PQfname(result_, i), "\") has unknown type code ", pg_oid);
      PQclear(result_);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    root_type.AppendChild(PQfname(result_, i), pg_type);
  }

  *result_types = root_type;
  return ADBC_STATUS_OK;
}

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

  nanoarrow::UniqueArray tmp;
  NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(tmp.get(), schema_.get(), &na_error_));
  for (int i = 0; i < helper_.NumColumns(); i++) {
    NANOARROW_RETURN_NOT_OK(field_readers_[i]->InitArray(tmp->children[i]));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(tmp.get()));
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

AdbcStatusCode PqResultArrayReader::Initialize(struct AdbcError* error) {
  helper_.set_output_format(PqResultHelper::Format::kBinary);
  RAISE_ADBC(helper_.Prepare());
  RAISE_ADBC(helper_.ExecutePrepared());

  ArrowSchemaInit(schema_.get());
  CHECK_NA_DETAIL(INTERNAL, ArrowSchemaSetTypeStruct(schema_.get(), helper_.NumColumns()),
                  &na_error_, error);

  for (int i = 0; i < helper_.NumColumns(); i++) {
    PostgresType child_type;
    CHECK_NA_DETAIL(INTERNAL,
                    type_resolver_->Find(helper_.FieldType(i), &child_type, &na_error_),
                    &na_error_, error);

    CHECK_NA(INTERNAL, child_type.SetSchema(schema_->children[i]), error);

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

}  // namespace adbcpq

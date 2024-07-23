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

AdbcStatusCode PqResultHelper::Prepare(int n_params, PostgresType* param_types) {
  std::vector<Oid> param_oids;
  const Oid* param_oids_ptr = nullptr;
  if (param_types != nullptr) {
    param_oids.resize(n_params);
    for (int i = 0; i < n_params; i++) {
      param_oids[i] = param_types->child(i).oid();
    }
    param_oids_ptr = param_oids.data();
  }

  // TODO: make stmtName a unique identifier?
  PGresult* result =
      PQprepare(conn_, /*stmtName=*/"", query_.c_str(), n_params, param_oids_ptr);
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
                 "[libpq] Failed to describe prepared statement: %s\nQuery was:%s",
                 PQerrorMessage(conn_), query_.c_str());
    PQclear(result_);
    result_ = nullptr;
    return code;
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::ExecutePrepared(const std::vector<std::string>& params) {
  std::vector<const char*> param_values;
  std::vector<int> param_lengths;
  std::vector<int> param_formats;

  for (const auto& param : params) {
    param_values.push_back(param.data());
    param_lengths.push_back(static_cast<int>(param.size()));
    param_formats.push_back(static_cast<int>(param_format_));
  }

  PQclear(result_);
  result_ = PQexecPrepared(conn_, "", param_values.size(), param_values.data(),
                           param_lengths.data(), param_formats.data(),
                           static_cast<int>(output_format_));

  ExecStatusType status = PQresultStatus(result_);
  if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
    AdbcStatusCode error =
        SetError(error_, result_, "[libpq] Failed to execute query '%s': %s",
                 query_.c_str(), PQerrorMessage(conn_));
    return error;
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::Execute(const std::vector<std::string>& params,
                                       PostgresType* param_types) {
  if (params.size() == 0 && param_types == nullptr && output_format_ == Format::kText) {
    PQclear(result_);
    result_ = PQexec(conn_, query_.c_str());
  } else {
    std::vector<const char*> param_values;
    std::vector<int> param_lengths;
    std::vector<int> param_formats;

    for (const auto& param : params) {
      param_values.push_back(param.data());
      param_lengths.push_back(static_cast<int>(param.size()));
      param_formats.push_back(static_cast<int>(param_format_));
    }

    std::vector<Oid> param_oids;
    const Oid* param_oids_ptr = nullptr;
    if (param_types != nullptr) {
      param_oids.resize(params.size());
      for (size_t i = 0; i < params.size(); i++) {
        param_oids[i] = param_types->child(i).oid();
      }
      param_oids_ptr = param_oids.data();
    }

    PQclear(result_);
    result_ = PQexecParams(conn_, query_.c_str(), param_values.size(), param_oids_ptr,
                           param_values.data(), param_lengths.data(),
                           param_formats.data(), static_cast<int>(output_format_));
  }

  ExecStatusType status = PQresultStatus(result_);
  if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
    AdbcStatusCode error =
        SetError(error_, result_, "[libpq] Failed to execute query '%s': %s",
                 query_.c_str(), PQerrorMessage(conn_));
    return error;
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::ExecuteCopy() {
  // Remove trailing semicolon(s) from the query before feeding it into COPY
  while (!query_.empty() && query_.back() == ';') {
    query_.pop_back();
  }

  std::string copy_query = "COPY (" + query_ + ") TO STDOUT (FORMAT binary)";
  result_ = PQexecParams(conn_, copy_query.c_str(), /*nParams=*/0,
                         /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                         /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                         static_cast<int>(Format::kBinary));

  if (PQresultStatus(result_) != PGRES_COPY_OUT) {
    AdbcStatusCode code = SetError(
        error_, result_,
        "[libpq] Failed to execute query: could not begin COPY: %s\nQuery was: %s",
        PQerrorMessage(conn_), copy_query.c_str());
    PQclear(result_);
    return code;
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

PGresult* PqResultHelper::ReleaseResult() {
  PGresult* out = result_;
  result_ = nullptr;
  return out;
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
  RAISE_ADBC(helper_.Execute());

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

AdbcStatusCode PqResultArrayReader::ToArrayStream(struct ArrowArrayStream* out,
                                                  struct AdbcError* error) {
  if (out == nullptr) {
    // If there is no output requested, we still need to execute
    RAISE_ADBC(helper_.Execute());
    return ADBC_STATUS_OK;
  }

  RAISE_ADBC(Initialize(error));

  nanoarrow::UniqueSchema schema;
  CHECK_NA(INTERNAL, GetSchema(schema.get()), error);

  nanoarrow::UniqueArray array;
  CHECK_NA(INTERNAL, GetNext(array.get()), error);

  nanoarrow::VectorArrayStream(schema.get(), array.get()).ToArrayStream(out);
  return ADBC_STATUS_OK;
}
}  // namespace adbcpq

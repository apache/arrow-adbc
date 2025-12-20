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

#include <charconv>
#include <memory>
#include <string>
#include <vector>

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/status.h"
#include "error.h"

namespace adbcpq {

PqResultHelper::~PqResultHelper() { ClearResult(); }

Status PqResultHelper::PrepareInternal(int n_params, const Oid* param_oids) const {
  // TODO: make stmtName a unique identifier?
  PGresult* result =
      PQprepare(conn_, /*stmtName=*/"", query_.c_str(), n_params, param_oids);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    auto status = MakeStatus(result, "Failed to prepare query: {}\nQuery was:{}",
                             PQerrorMessage(conn_), query_.c_str());
    PQclear(result);
    return status;
  }

  PQclear(result);
  return Status::Ok();
}

Status PqResultHelper::Prepare() const { return PrepareInternal(0, nullptr); }

Status PqResultHelper::Prepare(const std::vector<Oid>& param_oids) const {
  return PrepareInternal(static_cast<int>(param_oids.size()), param_oids.data());
}

Status PqResultHelper::DescribePrepared() {
  ClearResult();
  result_ = PQdescribePrepared(conn_, /*stmtName=*/"");
  if (PQresultStatus(result_) != PGRES_COMMAND_OK) {
    Status status = MakeStatus(
        result_, "[libpq] Failed to describe prepared statement: {}\nQuery was:{}",
        PQerrorMessage(conn_), query_.c_str());
    ClearResult();
    return status;
  }

  return Status::Ok();
}

Status PqResultHelper::Execute(const std::vector<std::string>& params,
                               PostgresType* param_types) {
  if (params.size() == 0 && param_types == nullptr && output_format_ == Format::kText) {
    ClearResult();
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

    ClearResult();
    result_ = PQexecParams(conn_, query_.c_str(), static_cast<int>(param_values.size()),
                           param_oids_ptr, param_values.data(), param_lengths.data(),
                           param_formats.data(), static_cast<int>(output_format_));
  }

  ExecStatusType status = PQresultStatus(result_);
  if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
    return MakeStatus(result_, "[libpq] Failed to execute query '{}': {}", query_.c_str(),
                      PQerrorMessage(conn_));
  }

  return Status::Ok();
}

Status PqResultHelper::ExecuteCopy() {
  // Remove trailing semicolon(s) from the query before feeding it into COPY
  while (!query_.empty() && query_.back() == ';') {
    query_.pop_back();
  }

  std::string copy_query = "COPY (" + query_ + ") TO STDOUT (FORMAT binary)";
  ClearResult();
  result_ = PQexecParams(conn_, copy_query.c_str(), /*nParams=*/0,
                         /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                         /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                         static_cast<int>(Format::kBinary));

  if (PQresultStatus(result_) != PGRES_COPY_OUT) {
    Status status = MakeStatus(
        result_,
        "[libpq] Failed to execute query: could not begin COPY: {}\nQuery was: {}",
        PQerrorMessage(conn_), copy_query.c_str());
    ClearResult();
    return status;
  }

  return Status::Ok();
}

Status PqResultHelper::ResolveParamTypes(PostgresTypeResolver& type_resolver,
                                         PostgresType* param_types) {
  struct ArrowError na_error;
  ArrowErrorInit(&na_error);

  const int num_params = PQnparams(result_);
  PostgresType root_type(PostgresTypeId::kRecord);

  for (int i = 0; i < num_params; i++) {
    const Oid pg_oid = PQparamtype(result_, i);
    PostgresType pg_type;
    if (type_resolver.Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
      std::string param_name = "$" + std::to_string(i + 1);
      Status status = Status::NotImplemented("[libpq] Parameter #", i + 1, " (\"",
                                             param_name,
                                             "\") has unknown type code ", pg_oid);
      ClearResult();
      return status;
    }

    std::string param_name = "$" + std::to_string(i + 1);
    root_type.AppendChild(param_name.c_str(), pg_type);
  }

  *param_types = root_type;
  return Status::Ok();
}

Status PqResultHelper::ResolveOutputTypes(PostgresTypeResolver& type_resolver,
                                          PostgresType* result_types) {
  struct ArrowError na_error;
  ArrowErrorInit(&na_error);

  const int num_fields = PQnfields(result_);
  PostgresType root_type(PostgresTypeId::kRecord);

  for (int i = 0; i < num_fields; i++) {
    const Oid pg_oid = PQftype(result_, i);
    PostgresType pg_type;
    if (type_resolver.Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
      // We couldn't look up the OID.
      // TODO(apache/arrow-adbc#1243): issue a warning (maybe reloading the
      // connection will load the OIDs if it was a newly created type)
      pg_type = PostgresType::Unnamed(pg_oid);
    }

    root_type.AppendChild(PQfname(result_, i), pg_type);
  }

  *result_types = root_type;
  return Status::Ok();
}

PGresult* PqResultHelper::ReleaseResult() {
  PGresult* out = result_;
  result_ = nullptr;
  return out;
}

int64_t PqResultHelper::AffectedRows() const {
  if (result_ == nullptr) {
    return -1;
  }

  char* first = PQcmdTuples(result_);
  char* last = first + strlen(first);
  if ((last - first) == 0) {
    return -1;
  }

  int64_t out;
  auto result = std::from_chars(first, last, out);

  if (result.ec == std::errc() && result.ptr == last) {
    return out;
  } else {
    return -1;
  }
}

}  // namespace adbcpq

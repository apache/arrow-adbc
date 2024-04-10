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

#include "statement.h"

#include <cinttypes>
#include <memory>

#include <adbc.h>
#include <arrow/c/bridge.h>
#include <google/cloud/bigquery/storage/v1/bigquery_read_client.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_client.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_options.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_request.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_response.h>
#include <nanoarrow/nanoarrow.hpp>
#include <nlohmann/json.hpp>

#include "common/options.h"
#include "common/utils.h"
#include "connection.h"
#include "database.h"
#include "readrowsiterator.h"
#include "utils.h"

namespace adbc_bigquery {
AdbcStatusCode BigqueryStatement::Bind(struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::Bind(struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::Cancel(struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::ExecuteQuery(struct ::ArrowArrayStream* stream,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  if (stream) {
    ::google::cloud::bigquery_v2_minimal_internal::JobConfigurationQuery job_configuration_query;

    if (auto query = GetQueryRequestOption("query"); query) {
      job_configuration_query.query = *query;
    } else {
      SetError(error, "[bigquery] Missing SQL query");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    GetAndAssignQueryRequestOption("create_disposition", job_configuration_query.create_disposition);
    GetAndAssignQueryRequestOption("write_disposition", job_configuration_query.write_disposition);
    GetAndAssignQueryRequestOption("priority", job_configuration_query.priority);
    GetAndAssignQueryRequestOption("parameter_mode", job_configuration_query.parameter_mode);
    GetAndAssignQueryRequestOption("preserve_nulls", job_configuration_query.preserve_nulls);
    GetAndAssignQueryRequestOption("allow_large_results", job_configuration_query.allow_large_results);
    GetAndAssignQueryRequestOption("use_query_cache", job_configuration_query.use_query_cache);
    GetAndAssignQueryRequestOption("flatten_results", job_configuration_query.flatten_results);
    GetAndAssignQueryRequestOption("use_legacy_sql", job_configuration_query.use_legacy_sql);
    GetAndAssignQueryRequestOption("create_session", job_configuration_query.create_session);
    GetAndAssignQueryRequestOption("maximum_bytes_billed", job_configuration_query.maximum_bytes_billed);

    if (auto default_dataset = GetQueryRequestOption("default_dataset");
        default_dataset) {
      ::google::cloud::bigquery_v2_minimal_internal::DatasetReference dataset_reference;
      std::string default_dataset_str = *default_dataset;
      std::vector<std::string> dataset_parts = split(default_dataset_str, ".");

      if (dataset_parts.size() != 2) {
        SetError(error, "[bigquery] Invalid default dataset reference: %s",
                 default_dataset_str.c_str());
        return ADBC_STATUS_INVALID_ARGUMENT;
      }

      dataset_reference.project_id = dataset_parts[0];
      dataset_reference.dataset_id = dataset_parts[1];
      job_configuration_query.default_dataset = dataset_reference;
    }
    if (auto destination_table = GetQueryRequestOption("destination_table");
        destination_table) {
      ::google::cloud::bigquery_v2_minimal_internal::TableReference table_reference;
      std::string destination_table_str = *destination_table;
      std::vector<std::string> table_parts = split(destination_table_str, ".");

      if (table_parts.size() != 3) {
        SetError(error, "[bigquery] Invalid default dataset reference: %s",
                 destination_table_str.c_str());
        return ADBC_STATUS_INVALID_ARGUMENT;
      }

      table_reference.project_id = table_parts[0];
      table_reference.dataset_id = table_parts[1];
      table_reference.table_id = table_parts[2];
      job_configuration_query.destination_table = table_reference;
    }

    ::google::cloud::bigquery_v2_minimal_internal::JobConfiguration job_configuration;
    job_configuration.query = job_configuration_query;

    ::google::cloud::bigquery_v2_minimal_internal::Job job;
    job.configuration = job_configuration;

    ::google::cloud::bigquery_v2_minimal_internal::InsertJobRequest insert_job_request;
    insert_job_request.set_project_id(connection_->database_->project_id());
    insert_job_request.set_job(job);

    ::google::cloud::bigquery_v2_minimal_internal::JobClient job_client(
        ::google::cloud::bigquery_v2_minimal_internal::MakeBigQueryJobConnection());
    auto status = job_client.InsertJob(insert_job_request);

    if (!status.ok()) {
      SetError(error, "%s%" PRId32 ", %s",
               "[bigquery] Cannot execute query: code=", status.status().code(),
               status.status().message().c_str());
      return ADBC_STATUS_INVALID_STATE;
    }
    auto job_response = status.value();
    if (!job_response.configuration || !job_response.configuration->query || !job_response.configuration->query->destination_table) {
      SetError(error, "[bigquery] Missing destination table in response");
      return ADBC_STATUS_INVALID_STATE;
    }

    auto destination_table = job_response.configuration->query->destination_table;
    std::string project_name =
        "projects/" + destination_table->project_id;
    std::string table_name = project_name + "/datasets/" +
                             destination_table->dataset_id +
                             "/tables/" +
                             destination_table->table_id;
    auto iterator = std::make_shared<ReadRowsIterator>(project_name, table_name);
    int ret = iterator->init(error);
    if (ret != ADBC_STATUS_OK) {
      return ret;
    }

    stream->private_data = new std::shared_ptr<ReadRowsIterator>(iterator);
    stream->get_next = ReadRowsIterator::get_next;
    stream->get_schema = ReadRowsIterator::get_schema;
    stream->release = ReadRowsIterator::release;

    if (rows_affected) {
      *rows_affected = -1;
    }
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::ExecuteSchema(struct ArrowSchema* schema,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOption(const char* key, char* value, size_t* length,
                                            struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionBytes(const char* key, uint8_t* value,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionDouble(const char* key, double* value,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionInt(const char* key, int64_t* value,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetParameterSchema(struct ArrowSchema* schema,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::New(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection || !connection->private_data) {
    SetError(error, "[bigquery] Must provide an initialized AdbcConnection");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  connection_ =
      *reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  this->options_ = connection_->options_;
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::Prepare(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::Release(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::SetOption(const char* key, const char* value,
                                            struct AdbcError* error) {
  options_[key] = value;
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::SetOptionBytes(const char* key, const uint8_t* value,
                                                 size_t length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetOptionDouble(const char* key, double value,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetOptionInt(const char* key, int64_t value,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetSqlQuery(const char* query,
                                              struct AdbcError* error) {
  options_["query"] = query;
  return ADBC_STATUS_OK;
}

}  // namespace adbc_bigquery

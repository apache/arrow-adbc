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

#include <memory>
#include <mutex>
#include <string>

#include <arrow/c/bridge.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/string_builder.h>
#include <arrow/util/string_view.h>
#include "adbc.h"
#include "drivers/util.h"

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;
using arrow::Status;

namespace {

void ReleaseError(struct AdbcError* error) {
  if (error->message) {
    delete[] error->message;
    error->message = nullptr;
  }
}

template <typename... Args>
void SetError(struct AdbcError* error, Args&&... args) {
  if (!error) return;
  std::string message =
      arrow::util::StringBuilder("[Flight SQL] ", std::forward<Args>(args)...);
  if (error->message) {
    message.reserve(message.size() + 1 + std::strlen(error->message));
    message.append(1, '\n');
    message.append(error->message);
    delete[] error->message;
  }
  error->message = new char[message.size() + 1];
  message.copy(error->message, message.size());
  error->message[message.size()] = '\0';
  error->release = ReleaseError;
}

class FlightSqlDatabaseImpl {
 public:
  FlightSqlDatabaseImpl() : client_(nullptr), connection_count_(0) {}

  flightsql::FlightSqlClient* Connect() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (client_) ++connection_count_;
    return client_.get();
  }

  AdbcStatusCode Init(struct AdbcError* error) {
    if (client_) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_STATE;
    }
    auto it = options_.find("location");
    if (it == options_.end()) {
      SetError(error, "Must provide 'location' option");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    flight::Location location;
    arrow::Status status = flight::Location::Parse(it->second).Value(&location);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    std::unique_ptr<flight::FlightClient> flight_client;
    status = flight::FlightClient::Connect(location).Value(&flight_client);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }

    client_.reset(new flightsql::FlightSqlClient(std::move(flight_client)));
    options_.clear();
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error) {
    if (client_) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_STATE;
    }
    options_[key] = value;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Disconnect(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (--connection_count_ < 0) {
      SetError(error, "Connection count underflow");
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Release(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (connection_count_ > 0) {
      SetError(error, "Cannot release database with ", connection_count_,
               " open connections");
      return ADBC_STATUS_INTERNAL;
    }

    auto status = client_->Close();
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    return ADBC_STATUS_OK;
  }

 private:
  std::unique_ptr<flightsql::FlightSqlClient> client_;
  int connection_count_;
  std::unordered_map<std::string, std::string> options_;
  std::mutex mutex_;
};

class FlightInfoReader : public arrow::RecordBatchReader {
 public:
  explicit FlightInfoReader(flightsql::FlightSqlClient* client,
                            std::unique_ptr<flight::FlightInfo> info)
      : client_(client), info_(std::move(info)), next_endpoint_(0) {}

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    flight::FlightStreamChunk chunk;
    while (current_stream_ && !chunk.data) {
      ARROW_ASSIGN_OR_RAISE(chunk, current_stream_->Next());
      if (chunk.data) {
        *batch = chunk.data;
        break;
      }
      if (!chunk.data && !chunk.app_metadata) {
        RETURN_NOT_OK(NextStream());
      }
    }
    if (!current_stream_) *batch = nullptr;
    return Status::OK();
  }

  Status Close() override {
    if (current_stream_) {
      current_stream_->Cancel();
    }
    return Status::OK();
  }

  static AdbcStatusCode Export(flightsql::FlightSqlClient* client,
                               std::unique_ptr<flight::FlightInfo> info,
                               struct ArrowArrayStream* stream, struct AdbcError* error) {
    auto reader = std::make_shared<FlightInfoReader>(client, std::move(info));
    auto status = reader->NextStream();
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    if (!reader->schema_) {
      // Empty result set - fall back on schema in FlightInfo
      arrow::ipc::DictionaryMemo memo;
      status = info->GetSchema(&memo).Value(&reader->schema_);
      if (!status.ok()) {
        SetError(error, status);
        return ADBC_STATUS_INTERNAL;
      }
    }

    status = arrow::ExportRecordBatchReader(std::move(reader), stream);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  }

 private:
  Status NextStream() {
    if (next_endpoint_ >= info_->endpoints().size()) {
      current_stream_ = nullptr;
      return Status::OK();
    }
    // TODO: this needs to account for location
    flight::FlightCallOptions call_options;
    ARROW_ASSIGN_OR_RAISE(
        current_stream_,
        client_->DoGet(call_options, info_->endpoints()[next_endpoint_].ticket));
    next_endpoint_++;
    if (!schema_) {
      ARROW_ASSIGN_OR_RAISE(schema_, current_stream_->GetSchema());
    }
    return Status::OK();
  }

  flightsql::FlightSqlClient* client_;
  std::unique_ptr<flight::FlightInfo> info_;
  size_t next_endpoint_;
  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<flight::FlightStreamReader> current_stream_;
};

class FlightSqlConnectionImpl {
 public:
  FlightSqlConnectionImpl() : database_(nullptr), client_(nullptr) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  flightsql::FlightSqlClient* client() const { return client_; }

  AdbcStatusCode Init(struct AdbcDatabase* database, struct AdbcError* error) {
    if (!database->private_data) {
      SetError(error, "database is not initialized");
      return ADBC_STATUS_INVALID_STATE;
    }

    database_ = *reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(
        database->private_data);
    client_ = database_->Connect();
    if (!client_) {
      SetError(error, "Database not yet initialized!");
      return ADBC_STATUS_INVALID_STATE;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Close(struct AdbcError* error) { return database_->Disconnect(error); }

  //----------------------------------------------------------
  // Metadata
  //----------------------------------------------------------

  AdbcStatusCode GetTableTypes(struct ArrowArrayStream* stream, struct AdbcError* error) {
    flight::FlightCallOptions call_options;
    std::unique_ptr<flight::FlightInfo> flight_info;
    auto status = client_->GetTableTypes(call_options).Value(&flight_info);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    return FlightInfoReader::Export(client_, std::move(flight_info), stream, error);
  }

  //----------------------------------------------------------
  // Partitioned Results
  //----------------------------------------------------------

  AdbcStatusCode ReadPartition(const uint8_t* serialized_partition,
                               size_t serialized_length, struct ArrowArrayStream* out,
                               struct AdbcError* error) {
    std::vector<flight::FlightEndpoint> endpoints(1);
    endpoints[0].ticket.ticket = std::string(
        reinterpret_cast<const char*>(serialized_partition), serialized_length);
    auto maybe_info = flight::FlightInfo::Make(
        *arrow::schema({}), flight::FlightDescriptor::Command(""), endpoints,
        /*total_records=*/-1, /*total_bytes=*/-1);
    if (!maybe_info.ok()) {
      SetError(error, maybe_info.status());
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    std::unique_ptr<flight::FlightInfo> flight_info(
        new flight::FlightInfo(maybe_info.MoveValueUnsafe()));
    return FlightInfoReader::Export(client_, std::move(flight_info), out, error);
  }

 private:
  std::shared_ptr<FlightSqlDatabaseImpl> database_;
  flightsql::FlightSqlClient* client_;
};

class FlightSqlPartitionsImpl {
 public:
  explicit FlightSqlPartitionsImpl(const arrow::flight::FlightInfo& info) {
    partitions_.reserve(info.endpoints().size());
    pointers_.reserve(info.endpoints().size());
    lengths_.reserve(info.endpoints().size());
    for (const flight::FlightEndpoint& endpoint : info.endpoints()) {
      // TODO(lidavidm): ARROW-17052 we should be serializing the entire endpoint
      partitions_.push_back(endpoint.ticket.ticket);
      pointers_.push_back(reinterpret_cast<const uint8_t*>(partitions_.back().data()));
      lengths_.push_back(partitions_.back().size());
    }
  }

  static void Export(const arrow::flight::FlightInfo& info, struct AdbcPartitions* out) {
    FlightSqlPartitionsImpl* impl = new FlightSqlPartitionsImpl(info);
    out->num_partitions = info.endpoints().size();
    out->num_partitions = impl->partitions_.size();
    out->partitions = impl->pointers_.data();
    out->partition_lengths = impl->lengths_.data();
    out->private_data = impl;
    out->release = &Release;
  }

  static void Release(struct AdbcPartitions* partitions) {
    FlightSqlPartitionsImpl* impl =
        static_cast<FlightSqlPartitionsImpl*>(partitions->private_data);
    delete impl;
    partitions->num_partitions = 0;
    partitions->partitions = nullptr;
    partitions->partition_lengths = nullptr;
    partitions->private_data = nullptr;
    partitions->release = nullptr;
  }

 private:
  std::vector<std::string> partitions_;
  std::vector<const uint8_t*> pointers_;
  std::vector<size_t> lengths_;
};

class FlightSqlStatementImpl {
 public:
  explicit FlightSqlStatementImpl(std::shared_ptr<FlightSqlConnectionImpl> connection)
      : connection_(std::move(connection)) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  AdbcStatusCode Close(struct AdbcError* error) { return ADBC_STATUS_OK; }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  AdbcStatusCode ExecutePartitions(const std::shared_ptr<FlightSqlStatementImpl>& self,
                                   struct ArrowSchema* schema,
                                   struct AdbcPartitions* partitions,
                                   int64_t* rows_affected, struct AdbcError* error) {
    flight::FlightCallOptions call_options;
    if (!schema || !partitions) {
      SetError(error, "Must provide schema and partitions");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    std::unique_ptr<flight::FlightInfo> info;
    auto status = connection_->client()->Execute(call_options, query_).Value(&info);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    if (rows_affected) {
      if (info->total_records() >= 0) {
        *rows_affected = static_cast<size_t>(info->total_records());
      } else {
        *rows_affected = -1;
      }
    }

    arrow::ipc::DictionaryMemo memo;
    std::shared_ptr<arrow::Schema> arrow_schema;
    status = info->GetSchema(&memo).Value(&arrow_schema);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INTERNAL;
    }

    status = arrow::ExportSchema(*arrow_schema, schema);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INTERNAL;
    }

    FlightSqlPartitionsImpl::Export(*info, partitions);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecuteQuery(const std::shared_ptr<FlightSqlStatementImpl>& self,
                              struct ArrowArrayStream* out, int64_t* rows_affected,
                              struct AdbcError* error) {
    flight::FlightCallOptions call_options;
    if (!out) {
      SetError(error, "Must provide out");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    std::unique_ptr<flight::FlightInfo> info;
    auto status = connection_->client()->Execute(call_options, query_).Value(&info);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    if (rows_affected) {
      if (info->total_records() >= 0) {
        *rows_affected = static_cast<size_t>(info->total_records());
      } else {
        *rows_affected = -1;
      }
    }
    return FlightInfoReader::Export(connection_->client(), std::move(info), out, error);
  }

  AdbcStatusCode ExecuteUpdate(const std::shared_ptr<FlightSqlStatementImpl>& self,
                               int64_t* rows_affected, struct AdbcError* error) {
    // TODO: update query
    // TODO: bulk ingest isn't implemented
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode SetSqlQuery(const std::shared_ptr<FlightSqlStatementImpl>&,
                             const char* query, struct AdbcError* error) {
    query_ = query;
    return ADBC_STATUS_OK;
  }

 private:
  std::shared_ptr<FlightSqlConnectionImpl> connection_;
  std::string query_;
};

AdbcStatusCode FlightSqlDatabaseNew(struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  auto impl = std::make_shared<FlightSqlDatabaseImpl>();
  database->private_data = new std::shared_ptr<FlightSqlDatabaseImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode FlightSqlDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                          const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode FlightSqlDatabaseInit(struct AdbcDatabase* database,
                                     struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode FlightSqlDatabaseRelease(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

AdbcStatusCode FlightSqlConnectionReadPartition(struct AdbcConnection* connection,
                                                const uint8_t* serialized_partition,
                                                size_t serialized_length,
                                                struct ArrowArrayStream* out,
                                                struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->ReadPartition(serialized_partition, serialized_length, out, error);
}

AdbcStatusCode FlightSqlConnectionGetTableTypes(struct AdbcConnection* connection,
                                                struct ArrowArrayStream* stream,
                                                struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->GetTableTypes(stream, error);
}

AdbcStatusCode FlightSqlConnectionNew(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  auto impl = std::make_shared<FlightSqlConnectionImpl>();
  connection->private_data = new std::shared_ptr<FlightSqlConnectionImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode FlightSqlConnectionSetOption(struct AdbcConnection* connection,
                                            const char* key, const char* value,
                                            struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode FlightSqlConnectionInit(struct AdbcConnection* connection,
                                       struct AdbcDatabase* database,
                                       struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->Init(database, error);
}

AdbcStatusCode FlightSqlConnectionRelease(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

AdbcStatusCode FlightSqlStatementExecutePartitions(struct AdbcStatement* statement,
                                                   struct ArrowSchema* schema,
                                                   struct AdbcPartitions* partitions,
                                                   int64_t* rows_affected,
                                                   struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->ExecutePartitions(*ptr, schema, partitions, rows_affected, error);
}

AdbcStatusCode FlightSqlStatementExecuteQuery(struct AdbcStatement* statement,
                                              struct ArrowArrayStream* out,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->ExecuteQuery(*ptr, out, rows_affected, error);
}

AdbcStatusCode FlightSqlStatementExecuteUpdate(struct AdbcStatement* statement,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->ExecuteUpdate(*ptr, rows_affected, error);
}

AdbcStatusCode FlightSqlStatementNew(struct AdbcConnection* connection,
                                     struct AdbcStatement* statement,
                                     struct AdbcError* error) {
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  auto impl = std::make_shared<FlightSqlStatementImpl>(*ptr);
  statement->private_data = new std::shared_ptr<FlightSqlStatementImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode FlightSqlStatementRelease(struct AdbcStatement* statement,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

AdbcStatusCode FlightSqlStatementSetSqlQuery(struct AdbcStatement* statement,
                                             const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(*ptr, query, error);
}
}  // namespace

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return FlightSqlDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return FlightSqlDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return FlightSqlDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return FlightSqlDatabaseRelease(database, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return FlightSqlConnectionReadPartition(connection, serialized_partition,
                                          serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return FlightSqlConnectionGetTableTypes(connection, stream, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return FlightSqlConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return FlightSqlConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return FlightSqlConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return FlightSqlConnectionRelease(connection, error);
}

// XXX: cpplint gets confused if declared as struct ArrowSchema*
AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return FlightSqlStatementExecutePartitions(statement, schema, partitions, rows_affected,
                                             error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return FlightSqlStatementExecuteQuery(statement, out, rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteUpdate(struct AdbcStatement* statement,
                                          int64_t* rows_affected,
                                          struct AdbcError* error) {
  return FlightSqlStatementExecuteUpdate(statement, rows_affected, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return FlightSqlStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return FlightSqlStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return FlightSqlStatementSetSqlQuery(statement, query, error);
}

extern "C" {
ADBC_EXPORT
AdbcStatusCode AdbcDriverInit(size_t count, struct AdbcDriver* driver,
                              size_t* initialized, struct AdbcError* error) {
  if (count < ADBC_VERSION_0_0_1) return ADBC_STATUS_NOT_IMPLEMENTED;

  std::memset(driver, 0, sizeof(*driver));
  driver->DatabaseNew = FlightSqlDatabaseNew;
  driver->DatabaseSetOption = FlightSqlDatabaseSetOption;
  driver->DatabaseInit = FlightSqlDatabaseInit;
  driver->DatabaseRelease = FlightSqlDatabaseRelease;

  driver->ConnectionNew = FlightSqlConnectionNew;
  driver->ConnectionSetOption = FlightSqlConnectionSetOption;
  driver->ConnectionInit = FlightSqlConnectionInit;
  driver->ConnectionReadPartition = FlightSqlConnectionReadPartition;
  driver->ConnectionRelease = FlightSqlConnectionRelease;
  driver->ConnectionGetTableTypes = FlightSqlConnectionGetTableTypes;

  driver->StatementExecutePartitions = FlightSqlStatementExecutePartitions;
  driver->StatementExecuteQuery = FlightSqlStatementExecuteQuery;
  driver->StatementExecuteUpdate = FlightSqlStatementExecuteUpdate;
  driver->StatementNew = FlightSqlStatementNew;
  driver->StatementRelease = FlightSqlStatementRelease;
  driver->StatementSetSqlQuery = FlightSqlStatementSetSqlQuery;

  *initialized = ADBC_VERSION_0_0_1;
  return ADBC_STATUS_OK;
}
}

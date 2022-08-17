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

 private:
  std::shared_ptr<FlightSqlDatabaseImpl> database_;
  flightsql::FlightSqlClient* client_;
};

class FlightSqlStatementImpl {
 public:
  explicit FlightSqlStatementImpl(std::shared_ptr<FlightSqlConnectionImpl> connection)
      : connection_(std::move(connection)) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  // TODO: a lot of these could be implemented in a common mixin
  AdbcStatusCode Close(struct AdbcError* error) { return ADBC_STATUS_OK; }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  AdbcStatusCode Execute(const std::shared_ptr<FlightSqlStatementImpl>& self,
                         int output_type, void* out, int64_t* rows_affected,
                         struct AdbcError* error) {
    flight::FlightCallOptions call_options;
    switch (output_type) {
      case ADBC_OUTPUT_TYPE_ARROW:
      case ADBC_OUTPUT_TYPE_PARTITIONS: {
        if (!out) {
          SetError(error, "Must provide out for output type ", output_type);
          return ADBC_STATUS_INVALID_ARGUMENT;
        }
        auto status = connection_->client()->Execute(call_options, query_).Value(&info_);
        if (!status.ok()) {
          SetError(error, status);
          return ADBC_STATUS_IO;
        }
        if (rows_affected) {
          if (info_->total_records() >= 0) {
            *rows_affected = static_cast<size_t>(info_->total_records());
          } else {
            *rows_affected = -1;
          }
        }

        if (output_type == ADBC_OUTPUT_TYPE_ARROW) {
          auto* stream = static_cast<struct ArrowArrayStream*>(out);
          return FlightInfoReader::Export(connection_->client(), std::move(info_), stream,
                                          error);
        } else {
          *static_cast<size_t*>(out) = info_->endpoints().size();
        }
        break;
      }
      case ADBC_OUTPUT_TYPE_UPDATE:
        // TODO: update query
        // TODO: bulk ingest isn't implemented
        return ADBC_STATUS_NOT_IMPLEMENTED;
      default:
        SetError(error, "Unknown output type ", output_type);
        return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetSqlQuery(const std::shared_ptr<FlightSqlStatementImpl>&,
                             const char* query, struct AdbcError* error) {
    query_ = query;
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // Partitioned Results
  //----------------------------------------------------------

  AdbcStatusCode DeserializePartitionDesc(const uint8_t* serialized_partition,
                                          size_t serialized_length,
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
    // TODO:
    std::unique_ptr<flight::FlightInfo> flight_info(
        new flight::FlightInfo(maybe_info.MoveValueUnsafe()));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetPartitionDescSize(size_t index, size_t* length,
                                      struct AdbcError* error) const {
    if (!info_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_INVALID_STATE;
    }

    // TODO: we're only encoding the ticket, not the actual locations
    if (index >= info_->endpoints().size()) {
      SetError(error, "Index is out of bounds");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    *length = info_->endpoints()[index].ticket.ticket.size();
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetPartitionDesc(size_t index, uint8_t* partition_desc,
                                  struct AdbcError* error) {
    if (!info_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_INVALID_STATE;
    }

    if (index >= info_->endpoints().size()) {
      SetError(error, "Index is out of bounds");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    const std::string& ticket = info_->endpoints()[index].ticket.ticket;
    std::memcpy(partition_desc, ticket.data(), ticket.size());
    return ADBC_STATUS_OK;
  }

 private:
  std::shared_ptr<FlightSqlConnectionImpl> connection_;
  std::string query_;
  std::unique_ptr<flight::FlightInfo> info_;
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

AdbcStatusCode FlightSqlConnectionDeserializePartitionDesc(
    struct AdbcConnection* connection, const uint8_t* serialized_partition,
    size_t serialized_length, struct AdbcStatement* statement, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->DeserializePartitionDesc(serialized_partition, serialized_length, error);
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

AdbcStatusCode FlightSqlStatementExecute(struct AdbcStatement* statement, int output_type,
                                         void* out, int64_t* rows_affected,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->Execute(*ptr, output_type, out, rows_affected, error);
}

AdbcStatusCode FlightSqlStatementGetPartitionDesc(struct AdbcStatement* statement,
                                                  size_t index, uint8_t* partition_desc,
                                                  struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetPartitionDesc(index, partition_desc, error);
}

AdbcStatusCode FlightSqlStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                      size_t index, size_t* length,
                                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetPartitionDescSize(index, length, error);
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

AdbcStatusCode AdbcConnectionDeserializePartitionDesc(struct AdbcConnection* connection,
                                                      const uint8_t* serialized_partition,
                                                      size_t serialized_length,
                                                      struct AdbcStatement* statement,
                                                      struct AdbcError* error) {
  return FlightSqlConnectionDeserializePartitionDesc(connection, serialized_partition,
                                                     serialized_length, statement, error);
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

AdbcStatusCode AdbcStatementExecute(struct AdbcStatement* statement, int output_type,
                                    void* out, int64_t* rows_affected,
                                    struct AdbcError* error) {
  return FlightSqlStatementExecute(statement, output_type, out, rows_affected, error);
}

AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             size_t index, uint8_t* partition_desc,
                                             struct AdbcError* error) {
  return FlightSqlStatementGetPartitionDesc(statement, index, partition_desc, error);
}

AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t index, size_t* length,
                                                 struct AdbcError* error) {
  return FlightSqlStatementGetPartitionDescSize(statement, index, length, error);
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
AdbcStatusCode AdbcFlightSqlDriverInit(size_t count, struct AdbcDriver* driver,
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
  driver->ConnectionRelease = FlightSqlConnectionRelease;
  driver->ConnectionDeserializePartitionDesc =
      FlightSqlConnectionDeserializePartitionDesc;
  driver->ConnectionGetTableTypes = FlightSqlConnectionGetTableTypes;

  driver->StatementExecute = FlightSqlStatementExecute;
  driver->StatementGetPartitionDesc = FlightSqlStatementGetPartitionDesc;
  driver->StatementGetPartitionDescSize = FlightSqlStatementGetPartitionDescSize;
  driver->StatementNew = FlightSqlStatementNew;
  driver->StatementRelease = FlightSqlStatementRelease;
  driver->StatementSetSqlQuery = FlightSqlStatementSetSqlQuery;
  *initialized = ADBC_VERSION_0_0_1;
  return ADBC_STATUS_OK;
}
}

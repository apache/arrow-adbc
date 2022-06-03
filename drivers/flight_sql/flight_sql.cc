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

#include <mutex>
#include <string>

#include <arrow/c/bridge.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
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
  explicit FlightSqlDatabaseImpl() : client_(nullptr), connection_count_(0) {}

  flightsql::FlightSqlClient* Connect() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (client_) ++connection_count_;
    return client_.get();
  }

  AdbcStatusCode Init(struct AdbcError* error) {
    if (client_) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_ARGUMENT;
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
      return ADBC_STATUS_INVALID_ARGUMENT;
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

class FlightSqlStatementImpl : public arrow::RecordBatchReader {
 public:
  FlightSqlStatementImpl() : client_(nullptr), info_() {}

  void Init(flightsql::FlightSqlClient* client,
            std::unique_ptr<flight::FlightInfo> info) {
    client_ = client;
    info_ = std::move(info);
  }

  //----------------------------------------------------------
  // arrow::RecordBatchReader Methods
  //----------------------------------------------------------

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
    return Status::OK();
  }

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  // TODO: a lot of these could be implemented in a common mixin
  AdbcStatusCode Close(struct AdbcError* error) { return ADBC_STATUS_OK; }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  AdbcStatusCode GetStream(const std::shared_ptr<FlightSqlStatementImpl>& self,
                           struct ArrowArrayStream* out, struct AdbcError* error) {
    if (!client_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_UNINITIALIZED;
    }

    auto status = NextStream();
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    if (!schema_) {
      return ADBC_STATUS_UNKNOWN;
    }

    status = arrow::ExportRecordBatchReader(self, out);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetPartitionDescSize(size_t* length, struct AdbcError* error) const {
    if (!client_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_UNINITIALIZED;
    }

    // TODO: we're only encoding the ticket, not the actual locations
    if (next_endpoint_ >= info_->endpoints().size()) {
      *length = 0;
      return ADBC_STATUS_OK;
    }
    *length = info_->endpoints()[next_endpoint_].ticket.ticket.size();
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetPartitionDesc(uint8_t* partition_desc, struct AdbcError* error) {
    if (!client_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_UNINITIALIZED;
    }

    if (next_endpoint_ >= info_->endpoints().size()) {
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    const std::string& ticket = info_->endpoints()[next_endpoint_].ticket.ticket;
    std::memcpy(partition_desc, ticket.data(), ticket.size());
    next_endpoint_++;
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
  std::shared_ptr<arrow::Schema> schema_;
  size_t next_endpoint_ = 0;
  std::unique_ptr<flight::FlightStreamReader> current_stream_;
};

class FlightSqlConnectionImpl {
 public:
  explicit FlightSqlConnectionImpl(std::shared_ptr<FlightSqlDatabaseImpl> database)
      : database_(std::move(database)), client_(nullptr) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  AdbcStatusCode Init(struct AdbcError* error) {
    client_ = database_->Connect();
    if (!client_) {
      SetError(error, "Database not yet initialized!");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Close(struct AdbcError* error) { return database_->Disconnect(error); }

  //----------------------------------------------------------
  // Metadata
  //----------------------------------------------------------

  AdbcStatusCode GetTableTypes(struct AdbcStatement* out, struct AdbcError* error) {
    if (!out->private_data) {
      SetError(error, "Statement is uninitialized, use AdbcStatementInit");
      return ADBC_STATUS_UNINITIALIZED;
    }
    auto* ptr =
        reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(out->private_data);
    auto* impl = ptr->get();

    flight::FlightCallOptions call_options;
    std::unique_ptr<flight::FlightInfo> flight_info;
    auto status = client_->GetTableTypes(call_options).Value(&flight_info);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    impl->Init(client_, std::move(flight_info));
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // SQL Semantics
  //----------------------------------------------------------

  AdbcStatusCode SqlExecute(const char* query, struct AdbcStatement* out,
                            struct AdbcError* error) {
    if (!out->private_data) {
      SetError(error, "Statement is uninitialized, use AdbcStatementInit");
      return ADBC_STATUS_UNINITIALIZED;
    }
    auto* ptr =
        reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(out->private_data);
    auto* impl = ptr->get();

    flight::FlightCallOptions call_options;
    std::unique_ptr<flight::FlightInfo> flight_info;
    auto status = client_->Execute(call_options, std::string(query)).Value(&flight_info);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    impl->Init(client_, std::move(flight_info));
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // Partitioned Results
  //----------------------------------------------------------

  AdbcStatusCode DeserializePartitionDesc(const uint8_t* serialized_partition,
                                          size_t serialized_length,
                                          struct AdbcStatement* out,
                                          struct AdbcError* error) {
    if (!out->private_data) {
      SetError(error, "Statement is uninitialized, use AdbcStatementInit");
      return ADBC_STATUS_UNINITIALIZED;
    }
    auto* ptr =
        reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(out->private_data);
    auto* impl = ptr->get();

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

    impl->Init(client_, std::move(flight_info));
    return ADBC_STATUS_OK;
  }

 private:
  std::shared_ptr<FlightSqlDatabaseImpl> database_;
  flightsql::FlightSqlClient* client_;
};

}  // namespace

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  auto impl = std::make_shared<FlightSqlDatabaseImpl>();
  database->private_data = new std::shared_ptr<FlightSqlDatabaseImpl>(impl);
  return ADBC_STATUS_OK;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  return (*ptr)->Init(error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionDeserializePartitionDesc(struct AdbcConnection* connection,
                                                      const uint8_t* serialized_partition,
                                                      size_t serialized_length,
                                                      struct AdbcStatement* statement,
                                                      struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->DeserializePartitionDesc(serialized_partition, serialized_length,
                                          statement, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct AdbcStatement* statement,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->GetTableTypes(statement, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionNew(struct AdbcDatabase* database,
                                 struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  auto ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlDatabaseImpl>*>(database->private_data);
  auto impl = std::make_shared<FlightSqlConnectionImpl>(*ptr);
  connection->private_data = new std::shared_ptr<FlightSqlConnectionImpl>(impl);
  return ADBC_STATUS_OK;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->Init(error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionSqlExecute(struct AdbcConnection* connection,
                                        const char* query, struct AdbcStatement* out,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr = reinterpret_cast<std::shared_ptr<FlightSqlConnectionImpl>*>(
      connection->private_data);
  return (*ptr)->SqlExecute(query, out, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             uint8_t* partition_desc,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetPartitionDesc(partition_desc, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetPartitionDescSize(length, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                      struct ArrowArrayStream* out,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  return (*ptr)->GetStream(*ptr, out, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementInit(struct AdbcConnection* connection,
                                 struct AdbcStatement* statement,
                                 struct AdbcError* error) {
  auto impl = std::make_shared<FlightSqlStatementImpl>();
  statement->private_data = new std::shared_ptr<FlightSqlStatementImpl>(impl);
  return ADBC_STATUS_OK;
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<FlightSqlStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

extern "C" {
ARROW_EXPORT
AdbcStatusCode AdbcFlightSqlDriverInit(size_t count, struct AdbcDriver* driver,
                                       size_t* initialized, struct AdbcError* error) {
  if (count < ADBC_VERSION_0_0_1) return ADBC_STATUS_NOT_IMPLEMENTED;

  std::memset(driver, 0, sizeof(*driver));
  driver->DatabaseNew = AdbcDatabaseNew;
  driver->DatabaseSetOption = AdbcDatabaseSetOption;
  driver->DatabaseInit = AdbcDatabaseInit;
  driver->DatabaseRelease = AdbcDatabaseRelease;

  driver->ConnectionNew = AdbcConnectionNew;
  driver->ConnectionSetOption = AdbcConnectionSetOption;
  driver->ConnectionInit = AdbcConnectionInit;
  driver->ConnectionRelease = AdbcConnectionRelease;
  driver->ConnectionSqlExecute = AdbcConnectionSqlExecute;
  driver->ConnectionDeserializePartitionDesc = AdbcConnectionDeserializePartitionDesc;
  driver->ConnectionGetTableTypes = AdbcConnectionGetTableTypes;

  driver->StatementGetPartitionDesc = AdbcStatementGetPartitionDesc;
  driver->StatementGetPartitionDescSize = AdbcStatementGetPartitionDescSize;
  driver->StatementGetStream = AdbcStatementGetStream;
  driver->StatementInit = AdbcStatementInit;
  driver->StatementRelease = AdbcStatementRelease;
  *initialized = ADBC_VERSION_0_0_1;
  return ADBC_STATUS_OK;
}
}

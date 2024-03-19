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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "driver/common/options.h"
#include "driver/framework/base.h"
#include "driver/framework/status.h"

namespace adbc::driver {

/// One-value ArrowArrayStream used to unify the implementations of Bind
struct OneValueStream {
  struct ArrowSchema schema;
  struct ArrowArray array;

  static int GetSchema(struct ArrowArrayStream* self, struct ArrowSchema* out) {
    OneValueStream* stream = static_cast<OneValueStream*>(self->private_data);
    return ArrowSchemaDeepCopy(&stream->schema, out);
  }
  static int GetNext(struct ArrowArrayStream* self, struct ArrowArray* out) {
    OneValueStream* stream = static_cast<OneValueStream*>(self->private_data);
    *out = stream->array;
    stream->array.release = nullptr;
    return 0;
  }
  static const char* GetLastError(struct ArrowArrayStream* self) { return NULL; }
  static void Release(struct ArrowArrayStream* self) {
    OneValueStream* stream = static_cast<OneValueStream*>(self->private_data);
    if (stream->schema.release) {
      stream->schema.release(&stream->schema);
      stream->schema.release = nullptr;
    }
    if (stream->array.release) {
      stream->array.release(&stream->array);
      stream->array.release = nullptr;
    }
    delete stream;
    self->release = nullptr;
  }
};

/// \brief A base implementation of a statement.
template <typename Derived>
class StatementBase : public ObjectBase {
 public:
  using Base = StatementBase<Derived>;

  /// \brief What to do in ingestion when the table does not exist.
  enum class TableDoesNotExist {
    kCreate,
    kFail,
  };

  /// \brief What to do in ingestion when the table already exists.
  enum class TableExists {
    kAppend,
    kFail,
    kReplace,
  };

  /// \brief Statement state: initialized with no set query.
  struct EmptyState {};
  /// \brief Statement state: bulk ingestion.
  struct IngestState {
    std::optional<std::string> target_catalog;
    std::optional<std::string> target_schema;
    std::optional<std::string> target_table;
    bool temporary = false;
    TableDoesNotExist table_does_not_exist_ = TableDoesNotExist::kCreate;
    TableExists table_exists_ = TableExists::kFail;
  };
  /// \brief Statement state: prepared statement.
  struct PreparedState {
    std::string query;
  };
  /// \brief Statement state: ad-hoc query.
  struct QueryState {
    std::string query;
  };
  /// \brief Statement state: one of the above.
  using State = std::variant<EmptyState, IngestState, PreparedState, QueryState>;

  StatementBase() : ObjectBase() {
    std::memset(&bind_parameters_, 0, sizeof(bind_parameters_));
  }
  ~StatementBase() = default;

  AdbcStatusCode Bind(ArrowArray* values, ArrowSchema* schema, AdbcError* error) {
    if (!values || !values->release) {
      return status::InvalidArgument("{} Bind: must provide non-NULL array",
                                     Derived::kErrorPrefix)
          .ToAdbc(error);
    } else if (!schema || !schema->release) {
      return status::InvalidArgument("{} Bind: must provide non-NULL stream",
                                     Derived::kErrorPrefix)
          .ToAdbc(error);
    }
    if (bind_parameters_.release) bind_parameters_.release(&bind_parameters_);
    // Make a one-value stream
    bind_parameters_.private_data = new OneValueStream{*schema, *values};
    bind_parameters_.get_schema = &OneValueStream::GetSchema;
    bind_parameters_.get_next = &OneValueStream::GetNext;
    bind_parameters_.get_last_error = &OneValueStream::GetLastError;
    bind_parameters_.release = &OneValueStream::Release;
    std::memset(values, 0, sizeof(*values));
    std::memset(schema, 0, sizeof(*schema));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    if (!stream || !stream->release) {
      return status::InvalidArgument("{} BindStream: must provide non-NULL stream",
                                     Derived::kErrorPrefix)
          .ToAdbc(error);
    }
    if (bind_parameters_.release) bind_parameters_.release(&bind_parameters_);
    // Move stream
    bind_parameters_ = *stream;
    std::memset(stream, 0, sizeof(*stream));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Cancel(AdbcError* error) { return ADBC_STATUS_NOT_IMPLEMENTED; }

  AdbcStatusCode ExecutePartitions(struct ArrowSchema* schema,
                                   struct AdbcPartitions* partitions,
                                   int64_t* rows_affected, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                              AdbcError* error) {
    return std::visit(
        [&](auto&& state) -> AdbcStatusCode {
          using T = std::decay_t<decltype(state)>;
          if constexpr (std::is_same_v<T, EmptyState>) {
            return status::InvalidState(
                       "{} Cannot ExecuteQuery without setting the query",
                       Derived::kErrorPrefix)
                .ToAdbc(error);
          } else if constexpr (std::is_same_v<T, IngestState>) {
            if (stream) {
              return status::InvalidState("{} Cannot ingest with result set",
                                          Derived::kErrorPrefix)
                  .ToAdbc(error);
            }
            RAISE_RESULT(error, int64_t rows, impl().ExecuteIngestImpl(state));
            if (rows_affected) {
              *rows_affected = rows;
            }
            return ADBC_STATUS_OK;
          } else if constexpr (std::is_same_v<T, PreparedState> ||
                               std::is_same_v<T, QueryState>) {
            int64_t rows = 0;
            if (stream) {
              RAISE_RESULT(error, rows, impl().ExecuteQueryImpl(state, stream));
            } else {
              RAISE_RESULT(error, rows, impl().ExecuteUpdateImpl(state));
            }
            if (rows_affected) {
              *rows_affected = rows;
            }
            return ADBC_STATUS_OK;
          } else {
            static_assert(!sizeof(T), "case not implemented");
          }
        },
        state_);
  }

  AdbcStatusCode ExecuteSchema(ArrowSchema* schema, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetParameterSchema(struct ArrowSchema* schema, struct AdbcError* error) {
    return std::visit(
        [&](auto&& state) -> AdbcStatusCode {
          using T = std::decay_t<decltype(state)>;
          if constexpr (std::is_same_v<T, EmptyState>) {
            return status::InvalidState(
                       "{} Cannot GetParameterSchema without setting the query",
                       Derived::kErrorPrefix)
                .ToAdbc(error);
          } else if constexpr (std::is_same_v<T, IngestState>) {
            return status::InvalidState("{} Cannot GetParameterSchema in bulk ingestion",
                                        Derived::kErrorPrefix)
                .ToAdbc(error);
          } else if constexpr (std::is_same_v<T, PreparedState>) {
            return impl().GetParameterSchemaImpl(state, schema).ToAdbc(error);
          } else if constexpr (std::is_same_v<T, QueryState>) {
            return status::InvalidState(
                       "{} Cannot GetParameterSchema without calling Prepare",
                       Derived::kErrorPrefix)
                .ToAdbc(error);
          } else {
            static_assert(!sizeof(T), "case not implemented");
          }
        },
        state_);
  }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    lifecycle_state_ = LifecycleState::kInitialized;
    if (auto status = impl().InitImpl(parent); !status.ok()) {
      return status.ToAdbc(error);
    }
    return ObjectBase::Init(parent, error);
  }

  AdbcStatusCode Prepare(AdbcError* error) {
    RAISE_STATUS(error, std::visit(
                            [&](auto&& state) -> Status {
                              using T = std::decay_t<decltype(state)>;
                              if constexpr (std::is_same_v<T, EmptyState>) {
                                return status::InvalidState(
                                    "{} Cannot Prepare without setting the query",
                                    Derived::kErrorPrefix);
                              } else if constexpr (std::is_same_v<T, IngestState>) {
                                return status::InvalidState(
                                    "{} Cannot Prepare without setting the query",
                                    Derived::kErrorPrefix);
                              } else if constexpr (std::is_same_v<T, PreparedState>) {
                                // No-op
                                return status::Ok();
                              } else if constexpr (std::is_same_v<T, QueryState>) {
                                UNWRAP_STATUS(impl().PrepareImpl(state));
                                state_ = PreparedState{std::move(state.query)};
                                return status::Ok();
                              } else {
                                static_assert(!sizeof(T), "case not implemented");
                              }
                            },
                            state_));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Release(AdbcError* error) {
    if (bind_parameters_.release) {
      bind_parameters_.release(&bind_parameters_);
      bind_parameters_.release = nullptr;
    }
    return impl().ReleaseImpl().ToAdbc(error);
  }

  AdbcStatusCode SetOption(std::string_view key, Option value, AdbcError* error) {
    auto ensure_ingest = [&]() -> IngestState& {
      if (!std::holds_alternative<IngestState>(state_)) {
        state_ = IngestState{};
      }
      return std::get<IngestState>(state_);
    };
    if (key == ADBC_INGEST_OPTION_MODE) {
      RAISE_RESULT(error, auto mode, value.AsString());
      if (mode == ADBC_INGEST_OPTION_MODE_APPEND) {
        auto& state = ensure_ingest();
        state.table_does_not_exist_ = TableDoesNotExist::kFail;
        state.table_exists_ = TableExists::kAppend;
      } else if (mode == ADBC_INGEST_OPTION_MODE_CREATE) {
        auto& state = ensure_ingest();
        state.table_does_not_exist_ = TableDoesNotExist::kCreate;
        state.table_exists_ = TableExists::kFail;
      } else if (mode == ADBC_INGEST_OPTION_MODE_CREATE_APPEND) {
        auto& state = ensure_ingest();
        state.table_does_not_exist_ = TableDoesNotExist::kCreate;
        state.table_exists_ = TableExists::kAppend;
      } else if (mode == ADBC_INGEST_OPTION_MODE_REPLACE) {
        auto& state = ensure_ingest();
        state.table_does_not_exist_ = TableDoesNotExist::kCreate;
        state.table_exists_ = TableExists::kReplace;
      } else {
        return status::InvalidArgument("{} Invalid ingest mode '{}'",
                                       Derived::kErrorPrefix, key, value)
            .ToAdbc(error);
      }
      return ADBC_STATUS_OK;
    } else if (key == ADBC_INGEST_OPTION_TARGET_CATALOG) {
      if (value.has_value()) {
        RAISE_RESULT(error, auto catalog, value.AsString());
        ensure_ingest().target_catalog = catalog;
      } else {
        ensure_ingest().target_catalog = std::nullopt;
      }
      return ADBC_STATUS_OK;
    } else if (key == ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) {
      if (value.has_value()) {
        RAISE_RESULT(error, auto schema, value.AsString());
        ensure_ingest().target_schema = schema;
      } else {
        ensure_ingest().target_schema = std::nullopt;
      }
      return ADBC_STATUS_OK;
    } else if (key == ADBC_INGEST_OPTION_TARGET_TABLE) {
      RAISE_RESULT(error, auto table, value.AsString());
      ensure_ingest().target_table = table;
      return ADBC_STATUS_OK;
    } else if (key == ADBC_INGEST_OPTION_TEMPORARY) {
      RAISE_RESULT(error, auto temporary, value.AsBool());
      ensure_ingest().temporary = temporary;
      return ADBC_STATUS_OK;
    }
    return impl().SetOptionImpl(key, value).ToAdbc(error);
  }

  AdbcStatusCode SetSqlQuery(const char* query, AdbcError* error) {
    RAISE_STATUS(error, std::visit(
                            [&](auto&& state) -> Status {
                              using T = std::decay_t<decltype(state)>;
                              if constexpr (std::is_same_v<T, EmptyState>) {
                                state_ = QueryState{
                                    std::string(query),
                                };
                                return status::Ok();
                              } else if constexpr (std::is_same_v<T, IngestState>) {
                                state_ = QueryState{
                                    std::string(query),
                                };
                                return status::Ok();
                              } else if constexpr (std::is_same_v<T, PreparedState>) {
                                state_ = QueryState{
                                    std::string(query),
                                };
                                return status::Ok();
                              } else if constexpr (std::is_same_v<T, QueryState>) {
                                state.query = std::string(query);
                                return status::Ok();
                              } else {
                                static_assert(!sizeof(T),
                                              "info value type not implemented");
                              }
                            },
                            state_));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetSubstraitPlan(const uint8_t* plan, size_t length, AdbcError* error) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  Result<int64_t> ExecuteIngestImpl(IngestState& state) {
    return status::NotImplemented("{} Bulk ingest is not implemented",
                                  Derived::kErrorPrefix);
  }

  Result<int64_t> ExecuteQueryImpl(PreparedState& state, ArrowArrayStream* stream) {
    return status::NotImplemented("{} ExecuteQuery is not implemented",
                                  Derived::kErrorPrefix);
  }

  Result<int64_t> ExecuteQueryImpl(QueryState& state, ArrowArrayStream* stream) {
    return status::NotImplemented("{} ExecuteQuery is not implemented",
                                  Derived::kErrorPrefix);
  }

  Result<int64_t> ExecuteUpdateImpl(PreparedState& state) {
    return status::NotImplemented("{} ExecuteQuery (update) is not implemented",
                                  Derived::kErrorPrefix);
  }

  Result<int64_t> ExecuteUpdateImpl(QueryState& state) {
    return status::NotImplemented("{} ExecuteQuery (update) is not implemented",
                                  Derived::kErrorPrefix);
  }

  Status GetParameterSchemaImpl(PreparedState& state, ArrowSchema* schema) {
    return status::NotImplemented("{} GetParameterSchema is not implemented",
                                  Derived::kErrorPrefix);
  }

  Status InitImpl(void* parent) { return status::Ok(); }

  Status PrepareImpl(QueryState& state) {
    return status::NotImplemented("{} Prepare is not implemented", Derived::kErrorPrefix);
  }

  Status ReleaseImpl() { return status::Ok(); }

  Status SetOptionImpl(std::string_view key, Option value) {
    return status::NotImplemented("{} Unknown statement option {}={}",
                                  Derived::kErrorPrefix, key, value);
  }

 protected:
  ArrowArrayStream bind_parameters_;

 private:
  State state_ = State(EmptyState{});
  Derived& impl() { return static_cast<Derived&>(*this); }
};

}  // namespace adbc::driver

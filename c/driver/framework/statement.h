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

#include "driver/framework/base_driver.h"
#include "driver/framework/status.h"
#include "driver/framework/utility.h"

namespace adbc::driver {

/// \brief A base implementation of a statement.
template <typename Derived>
class Statement : public BaseStatement<Derived> {
 public:
  using Base = Statement<Derived>;

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

  Statement() : BaseStatement<Derived>() {
    std::memset(&bind_parameters_, 0, sizeof(bind_parameters_));
  }
  ~Statement() = default;

  AdbcStatusCode Bind(ArrowArray* values, ArrowSchema* schema, AdbcError* error) {
    if (!values || !values->release) {
      return status::InvalidArgument(Derived::kErrorPrefix,
                                     " Bind: must provide non-NULL array")
          .ToAdbc(error);
    } else if (!schema || !schema->release) {
      return status::InvalidArgument(Derived::kErrorPrefix,
                                     " Bind: must provide non-NULL stream")
          .ToAdbc(error);
    }
    if (bind_parameters_.release) bind_parameters_.release(&bind_parameters_);
    MakeArrayStream(schema, values, &bind_parameters_);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    if (!stream || !stream->release) {
      return status::InvalidArgument(Derived::kErrorPrefix,
                                     " BindStream: must provide non-NULL stream")
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
            return status::InvalidState(Derived::kErrorPrefix,
                                        " Cannot ExecuteQuery without setting the query")
                .ToAdbc(error);
          } else if constexpr (std::is_same_v<T, IngestState>) {
            if (stream) {
              return status::InvalidState(Derived::kErrorPrefix,
                                          " Cannot ingest with result set")
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
                       Derived::kErrorPrefix,
                       " Cannot GetParameterSchema without setting the query")
                .ToAdbc(error);
          } else if constexpr (std::is_same_v<T, IngestState>) {
            return status::InvalidState(Derived::kErrorPrefix,
                                        " Cannot GetParameterSchema in bulk ingestion")
                .ToAdbc(error);
          } else if constexpr (std::is_same_v<T, PreparedState>) {
            return impl().GetParameterSchemaImpl(state, schema).ToAdbc(error);
          } else if constexpr (std::is_same_v<T, QueryState>) {
            return status::InvalidState(
                       Derived::kErrorPrefix,
                       " Cannot GetParameterSchema without calling Prepare")
                .ToAdbc(error);
          } else {
            static_assert(!sizeof(T), "case not implemented");
          }
        },
        state_);
  }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    this->lifecycle_state_ = LifecycleState::kInitialized;
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
                                    Derived::kErrorPrefix,
                                    " Cannot Prepare without setting the query");
                              } else if constexpr (std::is_same_v<T, IngestState>) {
                                return status::InvalidState(
                                    Derived::kErrorPrefix,
                                    " Cannot Prepare without setting the query");
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
        return status::InvalidArgument(Derived::kErrorPrefix, " Invalid ingest mode '",
                                       key, "': ", value.Format())
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
    return status::NotImplemented(Derived::kErrorPrefix,
                                  " Bulk ingest is not implemented");
  }

  Result<int64_t> ExecuteQueryImpl(PreparedState& state, ArrowArrayStream* stream) {
    return status::NotImplemented(Derived::kErrorPrefix,
                                  " ExecuteQuery is not implemented");
  }

  Result<int64_t> ExecuteQueryImpl(QueryState& state, ArrowArrayStream* stream) {
    return status::NotImplemented(Derived::kErrorPrefix,
                                  " ExecuteQuery is not implemented");
  }

  Result<int64_t> ExecuteUpdateImpl(PreparedState& state) {
    return status::NotImplemented(Derived::kErrorPrefix,
                                  " ExecuteQuery (update) is not implemented");
  }

  Result<int64_t> ExecuteUpdateImpl(QueryState& state) {
    return status::NotImplemented(Derived::kErrorPrefix,
                                  " ExecuteQuery (update) is not implemented");
  }

  Status GetParameterSchemaImpl(PreparedState& state, ArrowSchema* schema) {
    return status::NotImplemented(Derived::kErrorPrefix,
                                  " GetParameterSchema is not implemented");
  }

  Status InitImpl(void* parent) { return status::Ok(); }

  Status PrepareImpl(QueryState& state) {
    return status::NotImplemented(Derived::kErrorPrefix, " Prepare is not implemented");
  }

  Status ReleaseImpl() { return status::Ok(); }

  Status SetOptionImpl(std::string_view key, Option value) {
    return status::NotImplemented(Derived::kErrorPrefix, " Unknown statement option ",
                                  key, "=", value.Format());
  }

 protected:
  ArrowArrayStream bind_parameters_;

 private:
  State state_ = State(EmptyState{});
  Derived& impl() { return static_cast<Derived&>(*this); }
};

}  // namespace adbc::driver

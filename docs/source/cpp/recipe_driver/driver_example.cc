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

// RECIPE STARTS HERE

/// Here we'll show the structure of building an ADBC driver in C++ using
/// the ADBC driver framework library. This is the same library that ADBC
/// uses to build its SQLite and PostgreSQL drivers and abstracts away
/// the details of C callables and catalog/metadata functions that can be
/// difficult to implement but are essential for efficiently leveraging
/// the rest of the ADBC ecosystem.
///
/// At a high level, we'll be building a driver whose "database" is a directory
/// where each "table" in the database is a file containing an Arrow IPC stream.
/// Tables can be written using the bulk ingest feature and tables can be read
/// with a simple query in the form ``SELECT * FROM (the file)``.
///
/// Installation
/// ============
///
/// This quickstart is actually a literate C++ file.  You can clone
/// the repository, build the sample, and follow along.
///
/// We'll assume you're using conda-forge_ for dependencies.  CMake, a
/// C++17 compiler, and the ADBC libraries are required.  They can be
/// installed as follows:
///
/// .. code-block:: shell
///
///    mamba install cmake compilers libadbc-driver-manager
///
/// .. _conda-forge: https://conda-forge.org/
///
/// Building
/// ========
///
/// We'll use CMake_ here.  From a source checkout of the ADBC repository:
///
/// .. code-block:: shell
///
///    mkdir build
///    cd build
///    cmake ../docs/source/cpp/recipe_driver -DADBC_DRIVER_EXAMPLE_BUILD_TESTS=ON
///    cmake --build .
///    ctest
///
/// .. _CMake: https://cmake.org/
///
/// Building an ADBC Driver using C++
/// =================================
///
/// Let's start with some includes. Notably, we'll need the driver framework
/// header files and nanoarrow_, which we'll use to create and consume the
/// Arrow C data interface structures in this example driver.

/// .. _nanoarrow: https://arrow.apache.org/nanoarrow

#include "driver_example.h"

#include <cstdio>
#include <string>

#include "driver/framework/connection.h"
#include "driver/framework/database.h"
#include "driver/framework/statement.h"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "arrow-adbc/adbc.h"

/// Next, we'll bring a few essential framework types into the namespace
/// to reduce the verbosity of the implementation:
///
/// * :cpp:class:`adbc::driver::Option` : Options can be set on an ADBC database,
///   connection, and statmenent. They can be strings, opaque binary, doubles, or
///   integers.  The ``Option`` class abstracts the details of how to get, set,
///   and parse these values.
/// * :cpp:class:`adbc::driver::Status`: The ``Status`` is the ADBC driver
///   framework's error handling mechanism: functions with no return value that
///   can fail return a ``Status``.  You can use ``UNWRAP_STATUS(some_call())`` as
///   shorthand for ``Status status = some_call(); if (!status.ok()) return
///   status;`` to succinctly propagate errors.
/// * :cpp:class:`adbc::driver::Result`: The ``Result<T>`` is used as a return
///   value for functions that on success return a value of type ``T`` and on
///   failure communicate their error using a ``Status``. You can use
///   ``UNWRAP_RESULT(some_type value, some_call())`` as shorthand for
///
///   .. code-block:: cpp
///
///      some_type value;
///      Result<some_type> maybe_value = some_call();
///      if (!maybe_value.status().ok()) {
///        return maybe_value.status();
///      } else {
///        value = *maybe_value;
///      }

using adbc::driver::Option;
using adbc::driver::Result;
using adbc::driver::Status;

namespace {

/// Next, we'll provide the database implementation. The driver framework uses
/// the Curiously Recurring Template Pattern (CRTP_). The details of this are
/// handled by the framework, but functionally this is still just overriding
/// methods from a base class that handles the details.
///
/// Here, our database implementation will simply record the ``uri`` passed
/// by the user. Our interpretation of this will be a ``file://`` uri to
/// a directory to which our IPC files should be written and/or IPC files
/// should be read. This is the role of the database in ADBC: a shared
/// handle to a database that potentially caches some shared state among
/// connections, but which still allows multiple connections to execute
/// against the database concurrently.
///
/// .. _CRTP: https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern

class DriverExampleDatabase : public adbc::driver::Database<DriverExampleDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[example]";

  Status SetOptionImpl(std::string_view key, Option value) override {
    // Handle and validate options implemented by this driver
    if (key == "uri") {
      UNWRAP_RESULT(std::string_view uri, value.AsString());

      if (uri.find("file://") != 0) {
        return adbc::driver::status::InvalidArgument(
            "[example] uri must start with 'file://'");
      }

      uri_ = uri;
      return adbc::driver::status::Ok();
    }

    // Defer to the base implementation to handle state managed by the base
    // class (and error for all other options).
    return Base::SetOptionImpl(key, value);
  }

  Result<Option> GetOption(std::string_view key) override {
    // Return the value of options implemented by this driver
    if (key == "uri") {
      return Option(uri_);
    }

    // Defer to the base implementation to handle state managed by the base
    // class (and error for all other options).
    return Base::GetOption(key);
  }

  // This is called after zero or more calls to SetOption() on
  Status InitImpl() override {
    if (uri_.empty()) {
      return adbc::driver::status::InvalidArgument(
          "[example] Must set uri to a non-empty value");
    }

    return Base::InitImpl();
  }

  // Getters for members needed by the connection and/or statement:
  const std::string& uri() { return uri_; }

 private:
  std::string uri_;
};

/// Next, we implement the connection. While the role of the database is typically
/// to store or cache information, the role of the connection is to provide
/// resource handles that might be expensive to obtain (e.g., negotiating authentication
/// when connecting to a database). Because our example "database" is just a directory, we
/// don't need to do much in our connection in terms of resource management except to
/// provide a way for child statements to access the database's uri.
///
/// Another role of the connection is to provide metadata about tables, columns,
/// statistics, and other catalog-like information a caller might want to know before
/// issuing a query. The driver framework base classes provide helpers to implement these
/// functions such that you can mostly implement them in terms of the C++17 standard
/// library (as opposed to building the C-level arrays yourself).

class DriverExampleConnection : public adbc::driver::Connection<DriverExampleConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[example]";

  // Get information from the database and/or store a reference if needed.
  Status InitImpl(void* parent) {
    auto& database = *reinterpret_cast<DriverExampleDatabase*>(parent);
    uri_ = database.uri();
    return Base::InitImpl(parent);
  }

  // Getters for members needed by the statement:
  const std::string& uri() { return uri_; }

 private:
  std::string uri_;
};

/// Next, we provide the statement implementation. The statement is where query execution
/// is managed. Because our data source is quite literally Arrow data, we don't have to
/// provide a layer that manages type or value conversion. The SQLite and PostgreSQL
/// drivers both dedicate many lines of code to implementing and testing these conversions
/// efficiently. The nanoarrow library can be used to implement conversions in both
/// directions and is the scope of a separate article.

class DriverExampleStatement : public adbc::driver::Statement<DriverExampleStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[example]";

  // Get information from the connection and/or store a reference if needed.
  Status InitImpl(void* parent) {
    auto& connection = *reinterpret_cast<DriverExampleConnection*>(parent);
    uri_ = connection.uri();
    return Base::InitImpl(parent);
  }

  // Our implementation of a bulk ingestion is to write an Arrow IPC stream as a file
  // using the target table as the filename.
  Result<int64_t> ExecuteIngestImpl(IngestState& state) {
    std::string directory = uri_.substr(strlen("file://"));
    std::string filename = directory + "/" + *state.target_table;

    nanoarrow::ipc::UniqueOutputStream output_stream;
    FILE* c_file = std::fopen(filename.c_str(), "wb");
    UNWRAP_ERRNO(Internal, ArrowIpcOutputStreamInitFile(output_stream.get(), c_file,
                                                        /*close_on_release*/ true));

    nanoarrow::ipc::UniqueWriter writer;
    UNWRAP_ERRNO(Internal, ArrowIpcWriterInit(writer.get(), output_stream.get()));

    ArrowError nanoarrow_error;
    ArrowErrorInit(&nanoarrow_error);
    UNWRAP_NANOARROW(nanoarrow_error, Internal,
                     ArrowIpcWriterWriteArrayStream(writer.get(), &bind_parameters_,
                                                    &nanoarrow_error));

    return -1;
  }

  // Our implementation of query execution is to accept a simple query in the form
  // SELECT * FROM (the filename).
  Result<int64_t> ExecuteQueryImpl(QueryState& state, ArrowArrayStream* stream) {
    std::string prefix("SELECT * FROM ");
    if (state.query.find(prefix) != 0) {
      return adbc::driver::status::InvalidArgument(
          "[example] Query must be in the form 'SELECT * FROM filename'");
    }

    std::string directory = uri_.substr(strlen("file://"));
    std::string filename = directory + "/" + state.query.substr(prefix.size());

    nanoarrow::ipc::UniqueInputStream input_stream;
    FILE* c_file = std::fopen(filename.c_str(), "rb");
    UNWRAP_ERRNO(Internal, ArrowIpcInputStreamInitFile(input_stream.get(), c_file,
                                                       /*close_on_release*/ true));

    UNWRAP_ERRNO(Internal,
                 ArrowIpcArrayStreamReaderInit(stream, input_stream.get(), nullptr));
    return -1;
  }

  // This path is taken when the user calls Prepare() first.
  Result<int64_t> ExecuteQueryImpl(PreparedState& state, ArrowArrayStream* stream) {
    QueryState query_state{state.query};
    return ExecuteQueryImpl(query_state, stream);
  }

 private:
  std::string uri_;
};

}  // namespace

/// Finally, we create the driver initializer function, which is what the driver
/// manager needs to provide implementations for the ``Adbc**()`` functions that
/// comprise the ADBC C API. The name of this function matters: this file will
/// be built into a shared library named ``libdriver_example.(so|dll|dylib)``,
/// so the driver manager will look for the symbol ``AdbcDriverExampleInit()``
/// as the default entry point when asked to load the driver ``"driver_example"``.

extern "C" AdbcStatusCode AdbcDriverExampleInit(int version, void* raw_driver,
                                                AdbcError* error) {
  using ExampleDriver =
      adbc::driver::Driver<DriverExampleDatabase, DriverExampleConnection,
                           DriverExampleStatement>;
  return ExampleDriver::Init(version, raw_driver, error);
}

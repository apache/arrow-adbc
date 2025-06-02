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

/// Here we'll briefly tour basic features of ADBC with the SQLite
/// driver in C++17.

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
///    mamba install cmake compilers libadbc-driver-manager libadbc-driver-sqlite
///
/// .. _conda-forge: https://conda-forge.org/

/// Building
/// ========
///
/// We'll use CMake_ here.  From a source checkout of the ADBC repository:
///
/// .. code-block:: shell
///
///    mkdir build
///    cd build
///    cmake ../docs/source/cpp/recipe
///    cmake --build . --target recipe-quickstart
///    ./recipe-quickstart
///
/// .. _CMake: https://cmake.org/

/// Using ADBC
/// ==========
///
/// Let's start with some includes:

// For EXIT_SUCCESS
#include <cstdlib>
// For strerror
#include <cstring>
#include <iostream>

#include <arrow-adbc/adbc.h>
#include <nanoarrow.h>

/// Then we'll add some (very basic) error checking helpers.

// Error-checking helper for ADBC calls.
// Assumes that there is an AdbcError named `error` in scope.
#define CHECK_ADBC(EXPR)                                          \
  if (AdbcStatusCode status = (EXPR); status != ADBC_STATUS_OK) { \
    if (error.message != nullptr) {                               \
      std::cerr << error.message << std::endl;                    \
    }                                                             \
    return EXIT_FAILURE;                                          \
  }

// Error-checking helper for ArrowArrayStream.
#define CHECK_STREAM(STREAM, EXPR)                            \
  if (int status = (EXPR); status != 0) {                     \
    std::cerr << "(" << std::strerror(status) << "): ";       \
    const char* message = (STREAM).get_last_error(&(STREAM)); \
    if (message != nullptr) {                                 \
      std::cerr << message << std::endl;                      \
    } else {                                                  \
      std::cerr << "(no error message)" << std::endl;         \
    }                                                         \
    return EXIT_FAILURE;                                      \
  }

// Error-checking helper for Nanoarrow.
#define CHECK_NANOARROW(EXPR)                                              \
  if (int status = (EXPR); status != 0) {                                  \
    std::cerr << "(" << std::strerror(status) << "): failed" << std::endl; \
    return EXIT_FAILURE;                                                   \
  }

int main() {
  /// Loading the Driver
  /// ------------------
  ///
  /// We'll load the SQLite driver using the driver manager.  We don't
  /// have to explicitly link to the driver this way.

  AdbcError error = {};

  AdbcDatabase database = {};
  CHECK_ADBC(AdbcDatabaseNew(&database, &error));
  /// The way the driver manager knows what driver we want is via the
  /// ``driver`` option.
  CHECK_ADBC(AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", &error));
  CHECK_ADBC(AdbcDatabaseInit(&database, &error));

  /// Creating a Connection
  /// ---------------------
  ///
  /// ADBC distinguishes between ":term:`databases <database>`",
  /// ":term:`connections <connection>`", and ":term:`statements
  /// <statement>`".  A "database" holds shared state across multiple
  /// connections.  For example, in the SQLite driver, it holds the actual
  /// instance of SQLite.  A "connection" is one connection to the database.

  AdbcConnection connection = {};
  CHECK_ADBC(AdbcConnectionNew(&connection, &error));
  CHECK_ADBC(AdbcConnectionInit(&connection, &database, &error));

  /// Creating a Statement
  /// --------------------
  ///
  /// A statement lets us execute queries.  They are used for both
  /// prepared and non-prepared ("ad-hoc") queries.

  AdbcStatement statement = {};
  CHECK_ADBC(AdbcStatementNew(&connection, &statement, &error));

  /// Executing a Query
  /// -----------------
  ///
  /// We execute a query by setting the query on the statement, then
  /// calling :c:func:`AdbcStatementExecuteQuery`.  The results come
  /// back through the `Arrow C Data Interface`_.
  ///
  /// .. _Arrow C Data Interface: https://arrow.apache.org/docs/format/CDataInterface.html

  struct ArrowArrayStream stream = {};
  int64_t rows_affected = -1;

  CHECK_ADBC(AdbcStatementSetSqlQuery(&statement, "SELECT 42 AS THEANSWER", &error));
  CHECK_ADBC(AdbcStatementExecuteQuery(&statement, &stream, &rows_affected, &error));

  /// While the API gives us the number of rows, the SQLite driver
  /// can't actually know how many rows there are in the result set
  /// ahead of time, so this value will actually just be ``-1`` to
  /// indicate that the value is not known.
  std::cout << "Got " << rows_affected << " rows" << std::endl;
  // Output: Got -1 rows

  /// We need an Arrow implementation to read the actual results.  We
  /// can use `Arrow C++`_ or `Nanoarrow`_ for that.  For simplicity,
  /// we'll use Nanoarrow here.  (The CMake configuration for this
  /// example downloads and builds Nanoarrow from source as part of
  /// the build.)
  ///
  /// .. _Arrow C++: https://arrow.apache.org/docs/cpp/index.html
  /// .. _Nanoarrow: https://github.com/apache/arrow-nanoarrow

  /// First we'll get the schema of the data:
  ArrowSchema schema = {};
  CHECK_STREAM(stream, stream.get_schema(&stream, &schema));

  /// Then we can use Nanoarrow to print it:
  char buf[1024] = {};
  ArrowSchemaToString(&schema, buf, sizeof(buf), /*recursive=*/1);
  std::cout << "Result schema: " << buf << std::endl;
  // Output:
  // Result schema: struct<THEANSWER: int64>

  /// Now we can read the data.  The data comes as a stream of Arrow
  /// record batches.
  while (true) {
    ArrowArray batch = {};
    CHECK_STREAM(stream, stream.get_next(&stream, &batch));

    if (batch.release == nullptr) {
      // Stream has ended
      break;
    }

    /// We can use Nanoarrow to print out the data, too.
    ArrowArrayView view = {};
    CHECK_NANOARROW(ArrowArrayViewInitFromSchema(&view, &schema, nullptr));
    CHECK_NANOARROW(ArrowArrayViewSetArray(&view, &batch, nullptr));
    std::cout << "Got a batch with " << batch.length << " rows" << std::endl;
    for (int64_t i = 0; i < batch.length; i++) {
      std::cout << "THEANSWER[" << i
                << "] = " << view.children[0]->buffer_views[1].data.as_int64[i]
                << std::endl;
    }
    ArrowArrayViewReset(&view);
  }
  // Output:
  // Got a batch with 1 rows
  // THEANSWER[0] = 42

  stream.release(&stream);

  /// Cleanup
  /// -------
  /// At the end, we must release all our resources.

  CHECK_ADBC(AdbcStatementRelease(&statement, &error));
  CHECK_ADBC(AdbcConnectionRelease(&connection, &error));
  CHECK_ADBC(AdbcDatabaseRelease(&database, &error));
  return EXIT_SUCCESS;
}

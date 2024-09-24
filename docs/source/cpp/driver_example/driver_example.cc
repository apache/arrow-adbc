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

#include "driver/framework/connection.h"
#include "driver/framework/database.h"
#include "driver/framework/statement.h"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "arrow-adbc/adbc.h"

using adbc::driver::Option;
using adbc::driver::Result;
using adbc::driver::Status;

class DriverExampleDatabase : public adbc::driver::Database<DriverExampleDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[example]";

  Status SetOptionImpl(std::string_view key, Option value) override {
    if (key == "uri") {
      UNWRAP_RESULT(uri_, value.AsString());
      return adbc::driver::status::Ok();
    }

    return Base::SetOptionImpl(key, value);
  }

  Result<Option> GetOption(std::string_view key) override {
    if (key == "uri") {
      return Option(uri_);
    }

    return Base::GetOption(key);
  }

 private:
  std::string uri_;
};

class DriverExampleConnection : public adbc::driver::Connection<DriverExampleConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[example]";
};

class DriverExampleStatement : public adbc::driver::Statement<DriverExampleStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[example]";
};

static AdbcStatusCode ExampleDriverInitFunc(int version, void* raw_driver,
                                            AdbcError* error) {
  using ExampleDriver =
      adbc::driver::Driver<DriverExampleDatabase, DriverExampleConnection,
                           DriverExampleStatement>;
  return ExampleDriver::Init(version, raw_driver, error);
}

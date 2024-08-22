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

#include <string_view>
#include <utility>

#include <arrow-adbc/adbc.h>

#include "driver/framework/base_driver.h"
#include "driver/framework/status.h"

namespace adbc::driver {
/// \brief The CRTP base implementation of an AdbcDatabase.
///
/// Derived should override and implement the Impl methods, but not others.
/// Overridden methods should defer to the superclass version at the end.
/// (The Base typedef is provided to make this easier.)  Derived should also
/// define a constexpr static symbol called kErrorPrefix that is used to
/// construct error messages.
template <typename Derived>
class Database : public BaseDatabase<Derived> {
 public:
  using Base = Database<Derived>;

  Database() : BaseDatabase<Derived>() {}
  ~Database() = default;

  /// \brief Initialize the database.
  virtual Status InitImpl() { return BaseDatabase<Derived>::InitImpl(); }

  /// \brief Release the database.
  virtual Status ReleaseImpl() { return BaseDatabase<Derived>::ReleaseImpl(); }

  /// \brief Set an option.  May be called prior to InitImpl.
  virtual Status SetOptionImpl(std::string_view key, Option value) {
    return BaseDatabase<Derived>::SetOptionImpl(key, value);
  }

 private:
  Derived& impl() { return static_cast<Derived&>(*this); }
};
}  // namespace adbc::driver

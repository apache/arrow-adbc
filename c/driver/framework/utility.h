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

#include <arrow-adbc/adbc.h>

#include "driver/framework/status.h"

namespace adbc::driver {

void MakeEmptyStream(ArrowSchema* schema, ArrowArrayStream* out);

void MakeArrayStream(ArrowSchema* schema, ArrowArray* array, ArrowArrayStream* out);

Status MakeTableTypesStream(const std::vector<std::string>& table_types,
                            ArrowArrayStream* out);

struct InfoValue {
  uint32_t code;
  std::variant<std::string, int64_t> value;

  InfoValue(uint32_t code, std::variant<std::string, int64_t> value)
      : code(code), value(std::move(value)) {}
  InfoValue(uint32_t code, const char* value) : InfoValue(code, std::string(value)) {}
};

Status MakeGetInfoStream(const std::vector<InfoValue>& infos, ArrowArrayStream* out);

}  // namespace adbc::driver

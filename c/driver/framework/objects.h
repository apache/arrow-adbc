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

#include <optional>
#include <string_view>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "driver/framework/catalog.h"
#include "driver/framework/status.h"
#include "driver/framework/type_fwd.h"

namespace adbc::driver {
/// \brief A helper that implements GetObjects.
/// The schema/array/helper lifetime are caller-managed.
Status BuildGetObjects(GetObjectsHelper* helper, GetObjectsDepth depth,
                       std::optional<std::string_view> catalog_filter,
                       std::optional<std::string_view> schema_filter,
                       std::optional<std::string_view> table_filter,
                       std::optional<std::string_view> column_filter,
                       const std::vector<std::string_view>& table_types,
                       struct ArrowSchema* schema, struct ArrowArray* array);
}  // namespace adbc::driver

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

#include "error.h"

#include <stdarg.h>
#include <stdio.h>
#include <cstring>
#include <string>
#include <vector>

#include <libpq-fe.h>

#include "driver/common/utils.h"

namespace adbcpq {

AdbcStatusCode SetError(struct AdbcError* error, PGresult* result, const char* format,
                        ...) {
  if (error && error->release) {
    // TODO: combine the errors if possible
    error->release(error);
  }

  va_list args;
  va_start(args, format);
  std::string message;
  message.resize(1024);
  int chars_needed = vsnprintf(message.data(), message.size(), format, args);
  va_end(args);

  if (chars_needed > 0) {
    message.resize(chars_needed);
  } else {
    message.resize(0);
  }

  return MakeStatus(result, "{}", message).ToAdbc(error);
}

}  // namespace adbcpq

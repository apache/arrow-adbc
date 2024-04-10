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

#include "database.h"

#include <cinttypes>
#include <cstring>

#include <adbc.h>

#include "common/utils.h"

namespace adbc_bigquery {

BigqueryDatabase::~BigqueryDatabase() = default;

AdbcStatusCode BigqueryDatabase::GetOption(const char* option, char* value,
                                           size_t* length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode BigqueryDatabase::GetOptionBytes(const char* option, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode BigqueryDatabase::GetOptionInt(const char* option, int64_t* value,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode BigqueryDatabase::GetOptionDouble(const char* option, double* value,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode BigqueryDatabase::Init(struct AdbcError* error) { return ADBC_STATUS_OK; }

AdbcStatusCode BigqueryDatabase::Release(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryDatabase::SetOption(const char* key, const char* value,
                                           struct AdbcError* error) {
  if (strcmp(key, "project_id") == 0) {
    project_id_ = value;
    return ADBC_STATUS_OK;
  }
  SetError(error, "%s%s", "[bigquery] Unknown database option ", key);
  return ADBC_STATUS_INVALID_ARGUMENT;
}

AdbcStatusCode BigqueryDatabase::SetOptionBytes(const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  SetError(error, "%s%s", "[bigquery] Unknown option ", key);
  return ADBC_STATUS_INVALID_ARGUMENT;
}

AdbcStatusCode BigqueryDatabase::SetOptionDouble(const char* key, double value,
                                                 struct AdbcError* error) {
  SetError(error, "%s%s", "[bigquery] Unknown option ", key);
  return ADBC_STATUS_INVALID_ARGUMENT;
}

AdbcStatusCode BigqueryDatabase::SetOptionInt(const char* key, int64_t value,
                                              struct AdbcError* error) {
  SetError(error, "%s%s", "[bigquery] Unknown option ", key);
  return ADBC_STATUS_INVALID_ARGUMENT;
}

}  // namespace adbc_bigquery

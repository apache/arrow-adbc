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

#include "adbc_version_100.h"

#include <string.h>

struct Version100Database {
  int dummy;
};

static struct Version100Database kDatabase;

struct Version100Connection {
  int dummy;
};

static struct Version100Connection kConnection;

struct Version100Statement {
  int dummy;
};

static struct Version100Statement kStatement;

AdbcStatusCode Version100DatabaseInit(struct AdbcDatabase* database,
                                      struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100DatabaseNew(struct AdbcDatabase* database,
                                     struct AdbcError* error) {
  database->private_data = &kDatabase;
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100DatabaseRelease(struct AdbcDatabase* database,
                                         struct AdbcError* error) {
  database->private_data = NULL;
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100ConnectionInit(struct AdbcConnection* connection,
                                        struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100ConnectionNew(struct AdbcConnection* connection,
                                       struct AdbcError* error) {
  connection->private_data = &kConnection;
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100StatementExecuteQuery(struct AdbcStatement* statement,
                                               struct ArrowArrayStream* stream,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode Version100StatementNew(struct AdbcConnection* connection,
                                      struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  statement->private_data = &kStatement;
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100StatementRelease(struct AdbcStatement* statement,
                                          struct AdbcError* error) {
  statement->private_data = NULL;
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100ConnectionRelease(struct AdbcConnection* connection,
                                           struct AdbcError* error) {
  connection->private_data = NULL;
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version100DriverInit(int version, void* raw_driver,
                                    struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  struct AdbcDriverVersion100* driver = (struct AdbcDriverVersion100*)raw_driver;
  memset(driver, 0, sizeof(struct AdbcDriverVersion100));

  driver->DatabaseInit = &Version100DatabaseInit;
  driver->DatabaseNew = &Version100DatabaseNew;
  driver->DatabaseRelease = &Version100DatabaseRelease;

  driver->ConnectionInit = &Version100ConnectionInit;
  driver->ConnectionNew = &Version100ConnectionNew;
  driver->ConnectionRelease = &Version100ConnectionRelease;

  driver->StatementExecuteQuery = &Version100StatementExecuteQuery;
  driver->StatementNew = &Version100StatementNew;
  driver->StatementRelease = &Version100StatementRelease;

  return ADBC_STATUS_OK;
}

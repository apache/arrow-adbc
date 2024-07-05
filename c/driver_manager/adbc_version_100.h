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

// A dummy version 1.0.0 ADBC driver to test compatibility.

#include <arrow-adbc/adbc.h>

#ifdef __cplusplus
extern "C" {
#endif

struct AdbcErrorVersion100 {
  char* message;
  int32_t vendor_code;
  char sqlstate[5];
  void (*release)(struct AdbcError* error);
};

struct AdbcDriverVersion100 {
  void* private_data;
  void* private_manager;
  AdbcStatusCode (*release)(struct AdbcDriver* driver, struct AdbcError* error);

  AdbcStatusCode (*DatabaseInit)(struct AdbcDatabase*, struct AdbcError*);
  AdbcStatusCode (*DatabaseNew)(struct AdbcDatabase*, struct AdbcError*);
  AdbcStatusCode (*DatabaseSetOption)(struct AdbcDatabase*, const char*, const char*,
                                      struct AdbcError*);
  AdbcStatusCode (*DatabaseRelease)(struct AdbcDatabase*, struct AdbcError*);

  AdbcStatusCode (*ConnectionCommit)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionGetInfo)(struct AdbcConnection*, uint32_t*, size_t,
                                      struct ArrowArrayStream*, struct AdbcError*);
  AdbcStatusCode (*ConnectionGetObjects)(struct AdbcConnection*, int, const char*,
                                         const char*, const char*, const char**,
                                         const char*, struct ArrowArrayStream*,
                                         struct AdbcError*);
  AdbcStatusCode (*ConnectionGetTableSchema)(struct AdbcConnection*, const char*,
                                             const char*, const char*,
                                             struct ArrowSchema*, struct AdbcError*);
  AdbcStatusCode (*ConnectionGetTableTypes)(struct AdbcConnection*,
                                            struct ArrowArrayStream*, struct AdbcError*);
  AdbcStatusCode (*ConnectionInit)(struct AdbcConnection*, struct AdbcDatabase*,
                                   struct AdbcError*);
  AdbcStatusCode (*ConnectionNew)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionSetOption)(struct AdbcConnection*, const char*, const char*,
                                        struct AdbcError*);
  AdbcStatusCode (*ConnectionReadPartition)(struct AdbcConnection*, const uint8_t*,
                                            size_t, struct ArrowArrayStream*,
                                            struct AdbcError*);
  AdbcStatusCode (*ConnectionRelease)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionRollback)(struct AdbcConnection*, struct AdbcError*);

  AdbcStatusCode (*StatementBind)(struct AdbcStatement*, struct ArrowArray*,
                                  struct ArrowSchema*, struct AdbcError*);
  AdbcStatusCode (*StatementBindStream)(struct AdbcStatement*, struct ArrowArrayStream*,
                                        struct AdbcError*);
  AdbcStatusCode (*StatementExecuteQuery)(struct AdbcStatement*, struct ArrowArrayStream*,
                                          int64_t*, struct AdbcError*);
  AdbcStatusCode (*StatementExecutePartitions)(struct AdbcStatement*, struct ArrowSchema*,
                                               struct AdbcPartitions*, int64_t*,
                                               struct AdbcError*);
  AdbcStatusCode (*StatementGetParameterSchema)(struct AdbcStatement*,
                                                struct ArrowSchema*, struct AdbcError*);
  AdbcStatusCode (*StatementNew)(struct AdbcConnection*, struct AdbcStatement*,
                                 struct AdbcError*);
  AdbcStatusCode (*StatementPrepare)(struct AdbcStatement*, struct AdbcError*);
  AdbcStatusCode (*StatementRelease)(struct AdbcStatement*, struct AdbcError*);
  AdbcStatusCode (*StatementSetOption)(struct AdbcStatement*, const char*, const char*,
                                       struct AdbcError*);
  AdbcStatusCode (*StatementSetSqlQuery)(struct AdbcStatement*, const char*,
                                         struct AdbcError*);
  AdbcStatusCode (*StatementSetSubstraitPlan)(struct AdbcStatement*, const uint8_t*,
                                              size_t, struct AdbcError*);
};

AdbcStatusCode Version100DriverInit(int version, void* driver, struct AdbcError* error);

#ifdef __cplusplus
}
#endif

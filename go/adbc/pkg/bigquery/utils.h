// Code generated by _tmpl/utils.h.tmpl. DO NOT EDIT.

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

// clang-format off
//go:build driverlib
// clang-format on

#pragma once

#include <stdlib.h>
#include "../../drivermgr/arrow-adbc/adbc.h"

struct AdbcError* BigQueryErrorFromArrayStream(struct ArrowArrayStream*, AdbcStatusCode*);
AdbcStatusCode BigQueryDatabaseGetOption(struct AdbcDatabase*, const char*, char*,
                                         size_t*, struct AdbcError*);
AdbcStatusCode BigQueryDatabaseGetOptionBytes(struct AdbcDatabase*, const char*, uint8_t*,
                                              size_t*, struct AdbcError*);
AdbcStatusCode BigQueryDatabaseGetOptionDouble(struct AdbcDatabase*, const char*, double*,
                                               struct AdbcError*);
AdbcStatusCode BigQueryDatabaseGetOptionInt(struct AdbcDatabase*, const char*, int64_t*,
                                            struct AdbcError*);
AdbcStatusCode BigQueryDatabaseInit(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode BigQueryDatabaseNew(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode BigQueryDatabaseRelease(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode BigQueryDatabaseSetOption(struct AdbcDatabase* db, const char* key,
                                         const char* value, struct AdbcError* err);
AdbcStatusCode BigQueryDatabaseSetOptionBytes(struct AdbcDatabase*, const char*,
                                              const uint8_t*, size_t, struct AdbcError*);
AdbcStatusCode BigQueryDatabaseSetOptionDouble(struct AdbcDatabase*, const char*, double,
                                               struct AdbcError*);
AdbcStatusCode BigQueryDatabaseSetOptionInt(struct AdbcDatabase*, const char*, int64_t,
                                            struct AdbcError*);

AdbcStatusCode BigQueryConnectionCancel(struct AdbcConnection*, struct AdbcError*);
AdbcStatusCode BigQueryConnectionCommit(struct AdbcConnection* cnxn,
                                        struct AdbcError* err);
AdbcStatusCode BigQueryConnectionGetInfo(struct AdbcConnection* cnxn,
                                         const uint32_t* codes, size_t len,
                                         struct ArrowArrayStream* out,
                                         struct AdbcError* err);
AdbcStatusCode BigQueryConnectionGetObjects(struct AdbcConnection* cnxn, int depth,
                                            const char* catalog, const char* dbSchema,
                                            const char* tableName, const char** tableType,
                                            const char* columnName,
                                            struct ArrowArrayStream* out,
                                            struct AdbcError* err);
AdbcStatusCode BigQueryConnectionGetOption(struct AdbcConnection*, const char*, char*,
                                           size_t*, struct AdbcError*);
AdbcStatusCode BigQueryConnectionGetOptionBytes(struct AdbcConnection*, const char*,
                                                uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode BigQueryConnectionGetOptionDouble(struct AdbcConnection*, const char*,
                                                 double*, struct AdbcError*);
AdbcStatusCode BigQueryConnectionGetOptionInt(struct AdbcConnection*, const char*,
                                              int64_t*, struct AdbcError*);
AdbcStatusCode BigQueryConnectionGetStatistics(struct AdbcConnection*, const char*,
                                               const char*, const char*, char,
                                               struct ArrowArrayStream*,
                                               struct AdbcError*);
AdbcStatusCode BigQueryConnectionGetStatisticNames(struct AdbcConnection*,
                                                   struct ArrowArrayStream*,
                                                   struct AdbcError*);
AdbcStatusCode BigQueryConnectionGetTableSchema(struct AdbcConnection* cnxn,
                                                const char* catalog, const char* dbSchema,
                                                const char* tableName,
                                                struct ArrowSchema* schema,
                                                struct AdbcError* err);
AdbcStatusCode BigQueryConnectionGetTableTypes(struct AdbcConnection* cnxn,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* err);
AdbcStatusCode BigQueryConnectionInit(struct AdbcConnection* cnxn,
                                      struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode BigQueryConnectionNew(struct AdbcConnection* cnxn, struct AdbcError* err);
AdbcStatusCode BigQueryConnectionReadPartition(struct AdbcConnection* cnxn,
                                               const uint8_t* serialized,
                                               size_t serializedLen,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* err);
AdbcStatusCode BigQueryConnectionRelease(struct AdbcConnection* cnxn,
                                         struct AdbcError* err);
AdbcStatusCode BigQueryConnectionRollback(struct AdbcConnection* cnxn,
                                          struct AdbcError* err);
AdbcStatusCode BigQueryConnectionSetOption(struct AdbcConnection* cnxn, const char* key,
                                           const char* val, struct AdbcError* err);
AdbcStatusCode BigQueryConnectionSetOptionBytes(struct AdbcConnection*, const char*,
                                                const uint8_t*, size_t,
                                                struct AdbcError*);
AdbcStatusCode BigQueryConnectionSetOptionDouble(struct AdbcConnection*, const char*,
                                                 double, struct AdbcError*);
AdbcStatusCode BigQueryConnectionSetOptionInt(struct AdbcConnection*, const char*,
                                              int64_t, struct AdbcError*);

AdbcStatusCode BigQueryStatementBind(struct AdbcStatement* stmt,
                                     struct ArrowArray* values,
                                     struct ArrowSchema* schema, struct AdbcError* err);
AdbcStatusCode BigQueryStatementBindStream(struct AdbcStatement* stmt,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* err);
AdbcStatusCode BigQueryStatementCancel(struct AdbcStatement*, struct AdbcError*);
AdbcStatusCode BigQueryStatementExecuteQuery(struct AdbcStatement* stmt,
                                             struct ArrowArrayStream* out,
                                             int64_t* affected, struct AdbcError* err);
AdbcStatusCode BigQueryStatementExecutePartitions(struct AdbcStatement* stmt,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcPartitions* partitions,
                                                  int64_t* affected,
                                                  struct AdbcError* err);
AdbcStatusCode BigQueryStatementExecutePartitionsTrampoline(
    struct AdbcStatement* stmt, struct ArrowSchema* schema,
    struct AdbcPartitions* partitions, int64_t* affected, struct AdbcError* err);
AdbcStatusCode BigQueryStatementExecuteSchema(struct AdbcStatement*, struct ArrowSchema*,
                                              struct AdbcError*);
AdbcStatusCode BigQueryStatementGetOption(struct AdbcStatement*, const char*, char*,
                                          size_t*, struct AdbcError*);
AdbcStatusCode BigQueryStatementGetOptionBytes(struct AdbcStatement*, const char*,
                                               uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode BigQueryStatementGetOptionDouble(struct AdbcStatement*, const char*,
                                                double*, struct AdbcError*);
AdbcStatusCode BigQueryStatementGetOptionInt(struct AdbcStatement*, const char*, int64_t*,
                                             struct AdbcError*);
AdbcStatusCode BigQueryStatementGetParameterSchema(struct AdbcStatement* stmt,
                                                   struct ArrowSchema* schema,
                                                   struct AdbcError* err);
AdbcStatusCode BigQueryStatementNew(struct AdbcConnection* cnxn,
                                    struct AdbcStatement* stmt, struct AdbcError* err);
AdbcStatusCode BigQueryStatementPrepare(struct AdbcStatement* stmt,
                                        struct AdbcError* err);
AdbcStatusCode BigQueryStatementRelease(struct AdbcStatement* stmt,
                                        struct AdbcError* err);
AdbcStatusCode BigQueryStatementSetOption(struct AdbcStatement* stmt, const char* key,
                                          const char* value, struct AdbcError* err);
AdbcStatusCode BigQueryStatementSetOptionBytes(struct AdbcStatement*, const char*,
                                               const uint8_t*, size_t, struct AdbcError*);
AdbcStatusCode BigQueryStatementSetOptionDouble(struct AdbcStatement*, const char*,
                                                double, struct AdbcError*);
AdbcStatusCode BigQueryStatementSetOptionInt(struct AdbcStatement*, const char*, int64_t,
                                             struct AdbcError*);
AdbcStatusCode BigQueryStatementSetSqlQuery(struct AdbcStatement* stmt, const char* query,
                                            struct AdbcError* err);
AdbcStatusCode BigQueryStatementSetSubstraitPlan(struct AdbcStatement* stmt,
                                                 const uint8_t* plan, size_t length,
                                                 struct AdbcError* err);

AdbcStatusCode AdbcDriverBigQueryInit(int version, void* rawDriver,
                                      struct AdbcError* err);

static inline void BigQueryerrRelease(struct AdbcError* error) {
  if (error->release) {
    error->release(error);
    error->release = NULL;
  }
}

void BigQuery_release_error(struct AdbcError* error);

struct BigQueryError {
  char* message;
  char** keys;
  uint8_t** values;
  size_t* lengths;
  int count;
};

void BigQueryReleaseErrWithDetails(struct AdbcError* error);

int BigQueryErrorGetDetailCount(const struct AdbcError* error);
struct AdbcErrorDetail BigQueryErrorGetDetail(const struct AdbcError* error, int index);

int BigQueryArrayStreamGetSchemaTrampoline(struct ArrowArrayStream* stream,
                                           struct ArrowSchema* out);
int BigQueryArrayStreamGetNextTrampoline(struct ArrowArrayStream* stream,
                                         struct ArrowArray* out);

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

#ifndef ADBC_VALIDATION_H
#define ADBC_VALIDATION_H

#ifdef __cplusplus
extern "C" {
#endif

#include <adbc.h>

struct AdbcValidateTestContext {
  int total;
  int passed;
  int failed;
  AdbcStatusCode (*setup_database)(struct AdbcDatabase* database,
                                   struct AdbcError* error);
};

void AdbcValidateDatabaseNewRelease(struct AdbcValidateTestContext* adbc_context);
void AdbcValidateConnectionNewRelease(struct AdbcValidateTestContext* adbc_context);
void AdbcValidateConnectionAutocommit(struct AdbcValidateTestContext* adbc_context);
void AdbcValidateStatementNewRelease(struct AdbcValidateTestContext* adbc_context);
void AdbcValidateStatementSqlExecute(struct AdbcValidateTestContext* adbc_context);
void AdbcValidateStatementSqlIngest(struct AdbcValidateTestContext* adbc_context);
void AdbcValidateStatementSqlPrepare(struct AdbcValidateTestContext* adbc_context);

#ifdef __cplusplus
}
#endif

#endif  // ADBC_VALIDATION_H

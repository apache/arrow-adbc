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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/driver/bigquery.h>
#include <arrow-adbc/driver/flightsql.h>
#include <arrow-adbc/driver/postgresql.h>
#include <arrow-adbc/driver/snowflake.h>
#include <arrow-adbc/driver/sqlite.h>

int main(int argc, char** argv) {
  struct AdbcError error;

  struct AdbcDriver flightsql;
  struct AdbcDriver postgresql;
  struct AdbcDriver sqlite;

  memset(&error, 0, sizeof(error));
  memset(&flightsql, 0, sizeof(flightsql));
  memset(&postgresql, 0, sizeof(postgresql));
  memset(&sqlite, 0, sizeof(sqlite));

  AdbcStatusCode status;

  status = AdbcDriverFlightsqlInit(ADBC_VERSION_1_1_0, &flightsql, &error);
  if (status != ADBC_STATUS_OK) {
    if (error.release) {
      fprintf(stderr, "AdbcDriverFlightsqlInit failed: %s\n", error.message);
      error.release(&error);
    } else {
      fprintf(stderr, "AdbcDriverFlightsqlInit failed\n");
    }
    return EXIT_FAILURE;
  }

  status = AdbcDriverPostgresqlInit(ADBC_VERSION_1_1_0, &postgresql, &error);
  if (status != ADBC_STATUS_OK) {
    if (error.release) {
      fprintf(stderr, "AdbcDriverPostgresqlInit failed: %s\n", error.message);
      error.release(&error);
    } else {
      fprintf(stderr, "AdbcDriverPostgresqlInit failed\n");
    }
    return EXIT_FAILURE;
  }

  status = AdbcDriverSqliteInit(ADBC_VERSION_1_1_0, &sqlite, &error);
  if (status != ADBC_STATUS_OK) {
    if (error.release) {
      fprintf(stderr, "AdbcDriverSqliteInit failed: %s\n", error.message);
      error.release(&error);
    } else {
      fprintf(stderr, "AdbcDriverSqliteInit failed\n");
    }
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

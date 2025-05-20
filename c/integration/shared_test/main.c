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
#include <arrow-adbc/adbc_driver_manager.h>

int TryDriver(const char* driver_name) {
  struct AdbcError error;
  struct AdbcDriver driver;

  memset(&error, 0, sizeof(error));
  memset(&driver, 0, sizeof(driver));

  AdbcStatusCode status =
      AdbcLoadDriver(driver_name, NULL, ADBC_VERSION_1_1_0, &driver, &error);
  if (status != ADBC_STATUS_OK) {
    if (error.release) {
      fprintf(stderr, "AdbcLoadDriver failed: %s\n", error.message);
      error.release(&error);
    } else {
      fprintf(stderr, "AdbcLoadDriver failed\n");
    }
    return EXIT_FAILURE;
  }

  if (driver.release) {
    status = driver.release(&driver, &error);

    if (status != ADBC_STATUS_OK) {
      if (error.release) {
        fprintf(stderr, "AdbcDriver.release failed: %s\n", error.message);
        error.release(&error);
      } else {
        fprintf(stderr, "AdbcDriver.release failed\n");
      }
      return EXIT_FAILURE;
    }
  }

  return EXIT_SUCCESS;
}

int main(int argc, char** argv) {
  int rc = 0;

  rc = TryDriver("adbc_driver_bigquery");
  if (rc != EXIT_SUCCESS) {
    return rc;
  }
  printf("Loaded BigQuery driver\n");

  rc = TryDriver("adbc_driver_flightsql");
  if (rc != EXIT_SUCCESS) {
    return rc;
  }
  printf("Loaded FlightSQL driver\n");

  rc = TryDriver("adbc_driver_postgresql");
  if (rc != EXIT_SUCCESS) {
    return rc;
  }
  printf("Loaded PostgreSQL driver\n");

  rc = TryDriver("adbc_driver_snowflake");
  if (rc != EXIT_SUCCESS) {
    return rc;
  }
  printf("Loaded Snowflake driver\n");

  rc = TryDriver("adbc_driver_sqlite");
  if (rc != EXIT_SUCCESS) {
    return rc;
  }
  printf("Loaded SQLite driver\n");

  return EXIT_SUCCESS;
}

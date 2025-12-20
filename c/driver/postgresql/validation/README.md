<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# PostgreSQL ADBC Driver Validation Suite

This directory contains a Python-based validation suite for the PostgreSQL ADBC driver.

## Testing

A running instance of PostgreSQL is required.  For example, using Docker:

```shell
$ docker run -it --rm \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=postgres \
    -p 5432:5432 \
    postgres
```

Alternatively use the `docker compose` provided by ADBC to manage the test
database container.

```shell
$ docker compose up postgres-test
# When finished:
# docker compose down postgres-test
```

Then, to run the tests, set the environment variable specifying the
PostgreSQL URI before running tests:

```shell
$ export ADBC_POSTGRESQL_TEST_URI=postgresql://localhost:5432/postgres?user=postgres&password=password
```

### 5. Run Tests

#### Using pixi:

```bash
cd c/driver/postgresql/validation
pixi run validate
```
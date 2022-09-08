<!---
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

# ADBC libpq Driver for Python

This package contains bindings for the [libpq
driver](../../c/drivers/postgres/README.md), using the [driver
manager](../adbc_driver_manager/README.md) to provide a [DBAPI 2.0/PEP
249-compatible][dbapi] interface on top.

[dbapi]: https://peps.python.org/pep-0249/

## Building

Dependencies: a build of the libpq driver.

Set the environment variable `ADBC_POSTGRES_LIBRARY` to the directory
containing `libadbc_driver_postgres.so` (this library does not yet
support Windows/MacOS) before running `poetry build`.

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details on the
general build process.

## Testing

A running instance of Postgres is required.  For example, using Docker:

```shell
$ docker run -it --rm \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=tempdb \
    -p 5432:5432 \
    postgres
```

Then, to run the tests, set the environment variable specifying the
Postgres URI before running tests:

```shell
$ export ADBC_POSTGRES_TEST_URI=postgres://localhost:5432/postgres?user=postgres&password=password
$ pytest -vvx
```

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details on the
general test process.

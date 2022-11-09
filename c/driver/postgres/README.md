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

# ADBC libpq Driver

With credit to 0x0L's [pgeon](https://github.com/0x0L/pgeon) for the
overall approach.

This implements an ADBC driver that wraps [libpq][libpq].  This is
still a work in progress.

[libpq]: https://www.postgresql.org/docs/14/libpq.html

## Building

Dependencies: libpq itself. This can be installed with your favorite
package manager; however, you may need to set the `PKG_CONFIG_PATH`
environment variable such that `pkg-config` can find libpq.

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

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
$ ctest
```

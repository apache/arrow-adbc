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

# ADBC Flight SQL Driver

This implements an ADBC driver that wraps Arrow Flight SQL.  This is
still a work in progress.

## Building

Dependencies: Flight SQL itself.  This can be installed with your
favorite package manager, by installing the Arrow C++ libraries.

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

## Testing

A running instance of the Flight SQL test server from the Arrow source
tree is required.  This means [building Arrow with
tests](https://arrow.apache.org/docs/developers/cpp/building.html):

```shell
# Using a checkout of Arrow
$ cd arrow/
$ mkdir build && cd build
$ cmake ../cpp -DARROW_FLIGHT=ON -DARROW_FLIGHT_SQL=ON -DARROW_BUILD_TESTS=ON
$ cmake --build .
$ ./debug/flight-sql-test-server
Server listening on localhost:31337
```

Then, to run the tests, set the environment variable specifying the
Flight location before running tests:

```shell
# From a build of the driver
$ export ADBC_FLIGHT_SQL_LOCATION=grpc://localhost:31337
$ ctest
```

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

# ADBC Apache Arrow Flight SQL Driver for Python

This package contains bindings for the [Golang Apache Arrow Flight SQL
driver][flightsql], using the [driver manager][driver-manager] to provide a
[DBAPI 2.0/PEP 249-compatible][dbapi] interface on top.

[dbapi]: https://peps.python.org/pep-0249/
[driver-manager]: https://arrow.apache.org/adbc/current/python/driver_manager.html
[flightsql]: https://arrow.apache.org/adbc/current/driver/flight_sql.html

## Building

The cmake configuration option ``-DADBC_BUILD_PYTHON=ON`` will define a custom ``python`` target that can build and install this library, as long as ``-DADBC_DRIVER_FLIGHTSQL=ON`` is also specified.

Assuming you run cmake from the project root:

```shell
cmake -S -c -B build --preset debug -DADBC_BUILD_PYTHON=ON
cmake --build build --target python
```

will properly build and install the Python library for you.

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details on the
general build process.

## Testing

To run the tests, use pytest:

```shell
$ pytest -vvx
```

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details on the
general test process.

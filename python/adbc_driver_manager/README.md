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

# ADBC Driver Manager for Python

This package contains bindings for the ADBC Driver Manager, as well as a
[DBAPI 2.0/PEP 249-compatible][dbapi] interface on top.  This can be used to
load ADBC drivers at runtime and use them from Python.  Backend-specific
packages like `adbc_driver_postgresql` wrap this package in a more convenient
interface, and should be preferred where they exist.

The DBAPI 2.0 interface requires PyArrow, and exposes a number of extensions
mimicking those of [Turbodbc][turbodbc] or [DuckDB][duckdb]'s Python packages
to allow you to retrieve Arrow Table objects instead of being limited to the
row-oriented API of the base DBAPI interface.

[dbapi]: https://peps.python.org/pep-0249/
[duckdb]: https://duckdb.org/docs/api/python/overview
[turbodbc]: https://turbodbc.readthedocs.io/en/latest/

## Building

Dependencies: a C++ compiler.

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

The cmake configuration option ``-DADBC_BUILD_PYTHON=ON`` will define a custom ``python`` target that can build and install this library, as long as ``-DADBC_DRIVER_MANAGER=ON`` is also specified.

Assuming you run cmake from the project root:

```shell
cmake -S -c -B build --preset debug -DADBC_BUILD_PYTHON=ON
cmake --build build --target python
```

will properly build and install the Python library for you.

## Testing

The [SQLite driver](../../c/driver/sqlite/README.md) must be loadable
at runtime (e.g. it must be on `LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`,
or `PATH`).

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

```shell
$ export LD_LIBRARY_PATH=path/to/sqlite/driver/
$ pytest -vvx
```

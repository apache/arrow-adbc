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

# Contributing to ADBC

## Reporting Issues

Please file issues on the GitHub issue tracker:
https://github.com/apache/arrow-adbc/issues

## Building

### C/C++

The libraries here are all **individual** CMake projects.

_Note:_ unlike the Arrow C++ build system, the CMake projects will
**not** automatically download and build dependencies—you should
configure CMake appropriately to find dependencies in system or
package manager locations.

For example, the driver manager is built as follows:

```shell
$ mkdir -p build/driver_manager
$ cd build/driver_manager
$ cmake ../../c/driver_manager
$ make -j
```

The SQLite3 and Apache Arrow Flight SQL drivers can be built
similarly.  Both drivers require an installation of the Arrow C++
libraries (in the case of the Flight SQL driver, with Flight SQL
enabled), and of course, the SQLite3 driver requires an installation
of SQLite.

To find dependencies, use CMake options such as `CMAKE_PREFIX_PATH`.
A list of dependencies for Conda (conda-forge) is included, and can be
used as follows:

```shell
$ conda create -n adbc -c conda-forge --file ci/conda_env_cpp.txt
$ conda activate adbc
```

Some of Arrow's build options are supported (under a different prefix):

- `ADBC_BUILD_SHARED`, `ADBC_BUILD_STATIC`: build the shared/static libraries.
- `ADBC_BUILD_TESTS`: build the unit tests (requires googletest/gmock).
- `ADBC_INSTALL_NAME_RPATH`: set `install_name` to `@rpath` on MacOS.
  Usually it is more convenient to explicitly set this to `OFF` for
  development.

For example, to build and run tests for the SQLite3 driver:

```shell
$ mkdir -p build/sqlite
$ cd build/sqlite
# You may need to set -DCMAKE_PREFIX_PATH such that googletest can be found
$ cmake ../../c/drivers/sqlite -DADBC_BUILD_TESTS=ON
$ make -j
$ ctest
```

### Python

The Python libraries require Cython during build time; they have no
other build or runtime dependencies.  (The C++ sources for the driver
manager are inlined into the Cython compilation process—so there is no
need to separately build the driver manager.)

```shell
$ cd python/adbc_driver_manager
$ pip install -r requirements-dev.txt
$ python setup.py build_ext --inplace
```

To run tests, you will need a build of the SQLite3 driver above.

```shell
$ export LD_LIBRARY_PATH=path/to/sqlite/driver/
$ export PYTHONPATH=$(pwd)
$ pytest -vvx
```

### Java

The Java components are a standard Maven project.

```shell
$ cd java/
$ mvn clean install
```

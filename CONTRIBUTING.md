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

In general: static checks, such as linting and formatting, are
enforced via [pre-commit](https://pre-commit.com/).

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

All libraries here can be built similarly.  For information on what
they do and their dependencies, see their individual READMEs.

To specify where dependencies are to the build, use standard CMake
options such as [`CMAKE_PREFIX_PATH`][cmake-prefix-path].  A list of
dependencies for Conda (conda-forge) is included, and can be used as
follows:

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

All libraries use the same build options to enable tests.
For example, to build and run tests for the SQLite3 driver:

```shell
$ mkdir -p build/sqlite
$ cd build/sqlite
# You may need to set -DCMAKE_PREFIX_PATH such that googletest can be found
$ cmake ../../c/drivers/sqlite -DADBC_BUILD_TESTS=ON
$ make -j
$ ctest
```

Tests use [Googletest][gtest].  Some libraries may have additional
test-time dependencies.  For instance, the Postgres and Flight SQL
drivers require servers to test against.  See their individual READMEs
for details.

[cmake-prefix-path]: https://cmake.org/cmake/help/latest/variable/CMAKE_PREFIX_PATH.html
[gtest]: https://github.com/google/googletest/

### GLib

The GLib bindings use the [Meson][meson] build system.

A build of the [driver manager](./c/driver_manager/README.md) is
required.  For example, if the libraries are installed to
`$HOME/local`:

```shell
$ meson setup \
    --pkg-config-path=$HOME/local/lib/pkgconfig \
    --prefix=$HOME/local \
    build/glib \
    glib
$ meson install -C build/glib
```

A list of dependencies for Conda (conda-forge) is included, and can be
used as follows:

```shell
$ conda create -n adbc -c conda-forge --file ci/conda_env_glib.txt
$ conda activate adbc
```


[meson]: https://mesonbuild.com/

### Go

Go libraries are a standard Go project.

```shell
$ cd go/adbc
$ go build -v ./...
$ go test -v ./...
```

### Java

The Java components are a standard Maven project.

```shell
$ cd java/
# Build and run tests
$ mvn clean install
```

### Python

Python libraries are managed with [Poetry][poetry].  See individual
READMEs for additional dependencies.  In general, that means all
projects can be built as follows:

```shell
$ cd python
$ pip install -e adbc_driver_manager
```

Or directly via poetry:

```shell
$ cd python/adbc_driver_manager
$ poetry install
```

When adding/updating dependencies, please regenerate the lockfile:

```shell
$ cd python/adbc_driver_manager
$ poetry update
$ poetry export --dev -o requirements-dev.txt
```

Tests use [pytest][pytest].  Some libraries may have additional
test-time dependencies.  See their individual READMEs for details.

```shell
$ pytest -vvx
```

[poetry]: https://python-poetry.org
[pytest]: https://docs.pytest.org/

### Ruby

The Ruby libraries are bindings around the GLib libraries.

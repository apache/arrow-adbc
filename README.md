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

# ADBC: Arrow Database Connectivity

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/apache/arrow-adbc/blob/master/LICENSE.txt)

EXPERIMENTAL. Please see the [mailing list discussion](https://lists.apache.org/thread/gnz1kz2rj3rb8rh8qz7l0mv8lvzq254w).

## Building

The libraries here are all **individual** CMake projects.

```shell
$ mkdir -p build/driver_manager
$ cd build/driver_manager
$ cmake ../../adbc_driver_manager
$ make
```

Some of Arrow's build options are supported (under a different prefix):

- `ADBC_BUILD_SHARED`, `ADBC_BUILD_STATIC`: build the shared/static libraries
- `ADBC_BUILD_TESTS`: build the unit tests
- `ADBC_INSTALL_NAME_RPATH`: set `install_name` to `@rpath` on MacOS

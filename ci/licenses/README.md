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

This directory contains license appendices for binary distributions. The main
LICENSE.txt covers the ADBC project and any vendored sources. The files here
are specific to binary distributions, including the Python wheels, which
statically link their dependencies, and thus must propagate the licenses of
their (transitive) dependencies.

For the Flight SQL driver, which is written in Go, we use
[`go-licenses`](https://github.com/google/go-licenses) to assemble the
licenses of the transitive dependencies and generate a combined file. First,
install the tool:

```
go install github.com/google/go-licenses@latest
```

Then generate the license:

```
cd go/adbc
go-licenses report ./... \
  --ignore github.com/apache/arrow-adbc/go/adbc \
  --ignore github.com/apache/arrow/go/v18 \
  --template ../../ci/licenses/flightsql.tpl > ../../ci/licenses/flightsql.txt 2> /dev/null
```

You may have to manually fix up the license, since some packages do not
fill out their metadata correctly and things like READMEs may end up in
the license. Then check in the result.

There is not a separate file for the driver manager, as all of its
dependencies are vendored, and hence covered by the root LICENSE.txt.

After updating, copy the combined license files to the Python wheel
directories and commit the result:

```
cat LICENSE.txt ci/licenses/flightsql.txt > python/adbc_driver_flightsql/LICENSE.txt
cat LICENSE.txt ci/licenses/postgresql.txt > python/adbc_driver_postgresql/LICENSE.txt
cat LICENSE.txt ci/licenses/sqlite.txt > python/adbc_driver_sqlite/LICENSE.txt
```

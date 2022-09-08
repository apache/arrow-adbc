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

# ADBC Driver Manager

The driver manager provides a library that implements the ADBC API,
but dynamically loads and manages the actual driver libraries
underneath.  Applications can use this to work with multiple drivers
that would otherwise clash.  Language-specific bindings can target
this library to avoid having to manage individual libraries for every
driver.

## Building

Dependencies: none.

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

## Testing

The test suite requires the SQLite driver to be available.  On
Linux/MacOS, you can build the driver, and then install it, or set
`LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH` before running the test suite.
Similarly, on Windows, you can set `PATH`.

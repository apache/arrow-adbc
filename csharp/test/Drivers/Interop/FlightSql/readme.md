<!--

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# Flight SQL
The Flight SQL tests leverage the interop nature of the C# ADBC library. These require the use of the [Flight SQL Go driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/flightsql). You will need to compile the Go driver for your platform and place the driver in the correct path in order for the tests to execute correctly.

To compile, navigate to the `go/adbc/pkg` directory of the cloned [arrow-adbc](https://github.com/apache/arrow-adbc) repository then run the `make` command.  If you encounter compilation errors, please ensure that Go, [GCC and C++](https://code.visualstudio.com/docs/cpp/config-mingw) tools are installed. And following [Contributing to ADBC](https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md#environment-setup).

## Setup
The environment variable `FLIGHTSQL_TEST_CONFIG_FILE` must be set to a configuration JSON file for the tests to execute. If it is not, the tests will show as passed with an output message that they are skipped. A template configuration file can be found in the Resources directory.

## Configuration
Multiple data sources can support a Flight SQL interface. The configuration file supports targeting multiple data sources
simultaneously. To use multiple data sources, you can configure comma delimited names in:

- The `testEnvironments` field in the configuration file
- The `FLIGHTSQL_TEST_ENV_NAMES` environment variable

If neither is specified, the first configured environment is used.

## Data
This project contains SQL scripts to generate Flight SQL data in the `sqlFile` file specified for each environment. This can be used to populate a table in your Flight SQL instance with data.

The tests included assume this script has been run.

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

# Snowflake
The Snowflake tests leverage the interop nature of the C# ADBC library. These require the use of the [Snowflake Go driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/snowflake). You will need to compile the Go driver for your platform and place the driver in the correct path in order for the tests to execute correctly.

To compile, navigate to the `go/adbc/pkg` directory of the cloned [arrow-adbc](https://github.com/apache/arrow-adbc) repository then run the `make` command.  If you encounter compilation errors, please ensure that Go, [GCC and C++](https://code.visualstudio.com/docs/cpp/config-mingw) tools are installed. And following [Contributing to ADBC](https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md#environment-setup).

## Setup
The environment variable `SNOWFLAKE_TEST_CONFIG_FILE` must be set to a configuration JSON file for the tests to execute. If it is not, the tests will show as passed with an output message that they are skipped. A template configuration file can be found in the Resources directory.

## Configuration

The following values can be setup in the configuration:

- **driverPath** - The path for the Go library. Can be a relative path if using .NET 5.0 or greater, otherwise, it is an absolute path.
- **driverEntryPoint** - The driver entry point. For Snowflake, this is `AdbcDriverSnowflakeInit`.
- **account** - The `adbc.snowflake.sql.account` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **user** - The Snowflake user name.
- **password** - The Snowflake password, if using `auth_snowflake` for the auth type.
- **warehouse** - The `adbc.snowflake.sql.warehouse` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **authenticationType** - The `adbc.snowflake.sql.auth_type` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **authenticationTokenPath** - The path to the authentication token file, if using `auth_jwt` for the auth type.
- metadata
  - **catalog** - Used by metadata tests for which database to target.
  - **schema** - Used by metadata tests for which schema to target.
  - **table** - Used by metadata tests for which table to target.
  - **expectedColumnCount** - Used by metadata tests to validate the number of columns that are returned.
- **query** - The query to use.
- **expectedResults** - The expected number of results from the query.

## Data
This project contains a SQL script to generate Snowflake data in the `resources/SnowflakeData.sql` file. This can be used to populate a table in your Snowflake instance with data.

The tests included assume this script has been run.

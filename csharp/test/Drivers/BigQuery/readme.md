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

# BigQuery
The BigQuery tests are used to validate both the driver and the ADO.NET client library for use with the BigQuery ADBC driver. This includes the ability to read metadata, execute queries and updates, and validate the return types of the data.

## Configuration

The following values can be setup in the configuration

- **driverPath** - The path for the Go library. Can be a relative path if using .NET 5.0 or greater, otherwise, it is an absolute path.
- **driverEntryPoint** - The driver entry point. For Snowflake, this is `SnowflakeDriverInit`.
- **account** - The `adbc.snowflake.sql.account` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **user** - The Snowflake user name.
- **password** - The Snowflake password, if using `auth_snowflake` for the auth type.
- **warehouse** - The `adbc.snowflake.sql.warehouse` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **authenticationType** - The `adbc.snowflake.sql.auth_type` value from the [Snowflake Client Options](https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html#client-options).
- **authenticationTokenPath** - The path to the authentication token file, if using `auth_jwt` for the auth type.
- metadata
  - **database** - Used by metadata tests for which database to target.
  - **schema** - Used by metadata tests for which schema to target.
  - **table** - Used by metadata tests for which table to target.
  - **expectedColumnCount** - Used by metadata tests to validate the number of columns that are returned.
- **query** - The query to use.
- **expectedResults** - The expected number of results from the query.

## Data
This project contains a SQL script to generate BigQuery data in the `resources/BigQueryData.sql` file. This can be used to populate a table in your BigQuery instance with data.

The tests included assume this script has been run.

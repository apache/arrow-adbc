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

# Databricks

The Databricks ADBC driver is built on top of the Spark ADBC driver and inherits many of it's [properties](../Apache/Spark/readme.md).

The Databricks ADBC driver supports the following authentication methods:

1. **Token-based authentication** using a [Databricks personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html)
   - Set `adbc.spark.auth_type` to `oauth`
   - Set `adbc.databricks.oauth.grant_type` to `access_token` (this is the default if not specified)
   - Set `adbc.spark.oauth.access_token` to your Databricks personal access token

2. **OAuth Client Credentials Flow** for m2m authentication
   - Set `adbc.spark.auth_type` to `oauth`
   - Set `adbc.databricks.oauth.grant_type` to `client_credentials`
   - Set `adbc.databricks.oauth.client_id` to your OAuth client ID
   - Set `adbc.databricks.oauth.client_secret` to your OAuth client secret
   - Set `adbc.databricks.oauth.scope` to your auth scope (defaults to `"sql"`)
   - The driver will automatically handle token acquisition, renewal, and authentication with the Databricks service

Basic (username and password) authentication is not supported at this time.

## Data Types

The following table depicts how the Databricks ADBC driver converts a Databricks type to an Arrow type and a .NET type:


| Spark Type           | Arrow Type | C# Type |
| :---                 | :---:      | :---:   |
| ARRAY*               | String     | string  |
| BIGINT               | Int64      | long |
| BINARY               | Binary     | byte[] |
| BOOLEAN              | Boolean    | bool |
| CHAR                 | String     | string |
| DATE                 | Date32     | DateTime |
| DECIMAL              | Decimal128 | SqlDecimal |
| DOUBLE               | Double     | double |
| FLOAT                | Float      | float |
| INT                  | Int32      | int |
| INTERVAL_DAY_TIME+   | String     | string |
| INTERVAL_YEAR_MONTH+ | String     | string |
| MAP*                 | String     | string |
| NULL                 | Null       | null |
| SMALLINT             | Int16      | short |
| STRING               | String     | string |
| STRUCT*              | String     | string |
| TIMESTAMP            | Timestamp  | DateTimeOffset |
| TINYINT              | Int8       | sbyte |
| UNION                | String     | string |
| USER_DEFINED         | String     | string |
| VARCHAR              | String     | string |

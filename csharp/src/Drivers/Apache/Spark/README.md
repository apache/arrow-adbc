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

# Spark Driver

## Database and Connection Properties

Properties should be passed in the call to `SparkDriver.Open`,
but can also be passed in the call to `AdbcDatabase.Connect`.

| Property            | Description | Default |
| :---                | :---        | :---    |
| `adbc.spark.host`   | Host name for the data source. Do no include scheme or port number. Example: `sparkserver.region.cloudapp.azure.com` |  |
| `adbc.spark.port`   | The port number the data source is listen on for new connections. | `443` |
| `adbc.spark.path`   | The URI path on the data source server. Example: `sql/protocolv1/o/0123456789123456/01234-0123456-source` | |
| `adbc.spark.token`  | For token-based authentication, the token to be authenticated on the data source. Example: `abcdef0123456789` | |
<!-- Add these properties when basic authentication is available.
| `adbc.spark.scheme` | The HTTP or HTTPS scheme to use. Allowed values: `http`, `https`. | `https` - when port is 443 or empty, `http`, otherwise. |
| `auth_type`         | An indicator of the intended type of authentication. Allowed values: `basic`, `token`. This property is optional. The authentication type can be inferred from `token`, `username`, and `password`. If a `token` value is provided, token authentication is used. Otherwise, if both `username` and `password` values are provided, basic authentication is used. | |
| `username`          | The user name used for basic authentication | |
| `password`          | The password for the user name used for basic authentication. | |
-->

## Spark Types

The following table depicts how the Spark ADBC driver converts a Spark type to an Arrow type and a .NET type:

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

\* Complex types are returned as strings<br>
\+ Interval types are returned as strings

## Supported Variants

### Spark on Databricks

Support for Spark on Databricks is the most mature.

The Spark ADBC driver supports token-based authentiation using the
[Databricks personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html).
Basic (username and password) authenication is not supported, at this time.

### Native Apache Spark

This is currently unsupported.

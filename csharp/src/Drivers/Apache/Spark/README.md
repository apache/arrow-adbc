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

| Property               | Description | Default |
| :---                   | :---        | :---    |
| `adbc.spark.type`      | (Required) Indicates the Spark server type. One of `databricks`, `http` (future: `standard`) | |
| `adbc.spark.auth_type` | An indicator of the intended type of authentication. Allowed values: `none`, `username_only`, `basic`, and `token`. This property is optional. The authentication type can be inferred from `token`, `username`, and `password`. If a `token` value is provided, token authentication is used. Otherwise, if both `username` and `password` values are provided, basic authentication is used. | |
| `adbc.spark.host`      | Host name for the data source. Do not include scheme or port number. Example: `sparkserver.region.cloudapp.azure.com` |  |
| `adbc.spark.port`      | The port number the data source listens on for a new connections. | `443` |
| `adbc.spark.path`      | The URI path on the data source server. Example: `sql/protocolv1/o/0123456789123456/01234-0123456-source` | |
| `adbc.spark.token`     | For token-based authentication, the token to be authenticated on the data source. Example: `abcdef0123456789` | |
| `uri`                  | The full URI that includes scheme, host, port and path. If set, this property takes precedence over `adbc.spark.host`, `adbc.spark.port` and `adbc.spark.path`. | |
| `username`             | The user name used for basic authentication | |
| `password`             | The password for the user name used for basic authentication. | |
| `adbc.spark.data_type_conv` | Comma-separated list of data conversion options. Each option indicates the type of conversion to perform on data returned from the Spark server. <br><br>Allowed values: `none`. <br><br>Option `none` indicates there is no conversion from Spark type to native type (i.e., no conversion from String to Timestamp for Apache Spark over HTTP). Example `adbc.spark.conv_data_type=none`. <br><br>(_Planned supported values_: `scalar`. Option `scalar` will perform conversion (if necessary) from the Spark data type to corresponding Arrow data types for types `DATE/Date32/DateTime`, `DECIMAL/Decimal128/SqlDecimal`, and `TIMESTAMP/Timestamp/DateTimeOffset`. Example `adbc.spark.conv_data_type=scalar`) | `scalar` |
| `adbc.statement.batch_size` | Sets the maximum number of rows to retrieve in a single batch request. | `50000` |
| `adbc.statement.polltime_milliseconds` | If polling is necessary to get a result, this option sets the length of time (in milliseconds) to wait between polls. | `500` |

## Spark Types

The following table depicts how the Spark ADBC driver converts a Spark type to an Arrow type and a .NET type:

### Spark on Databricks

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

### Apache Spark over HTTP (when: adbc.spark.data_type_conv = none)

| Spark Type           | Arrow Type | C# Type |
| :---                 | :---:      | :---:   |
| ARRAY*               | String     | string  |
| BIGINT               | Int64      | long |
| BINARY               | Binary     | byte[] |
| BOOLEAN              | Boolean    | bool |
| CHAR                 | String     | string |
| DATE*                | *String*   | *string* |
| DECIMAL*             | *String*   | *string* |
| DOUBLE               | Double     | double |
| FLOAT                | *Double*   | *double* |
| INT                  | Int32      | int |
| INTERVAL_DAY_TIME+   | String     | string |
| INTERVAL_YEAR_MONTH+ | String     | string |
| MAP*                 | String     | string |
| NULL                 | String     | string |
| SMALLINT             | Int16      | short |
| STRING               | String     | string |
| STRUCT*              | String     | string |
| TIMESTAMP*           | *String*   | *string* |
| TINYINT              | Int8       | sbyte |
| UNION                | String     | string |
| USER_DEFINED         | String     | string |
| VARCHAR              | String     | string |

\* Types are returned as strings instead of "native" types<br>
\+ Interval types are returned as strings

## Supported Variants

### Spark on Databricks

Support for Spark on Databricks is the most mature.

The Spark ADBC driver supports token-based authentiation using the
[Databricks personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html).
Basic (username and password) authenication is not supported, at this time.

### Apache Spark over HTPP

This is currently unsupported. (Under development)

### Apache Spark Standard

This is currently unsupported.

### Azure Spark HDInsight

To read data from Azure HDInsight Spark Cluster, use the following parameters:
adbc.spark.type = "http"
adbc.spark.port = "443"
adbc.spark.path = "/sparkhive2"
adbc.spark.host = $"{clusterHostName}"
username = $"{clusterUserName}"
password = $"{clusterPassword}"

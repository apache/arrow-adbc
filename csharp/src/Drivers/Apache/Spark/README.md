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
| `adbc.spark.type`      | (Required) Indicates the Spark server type. Currently only `http` (future: `standard`) | |
| `adbc.spark.auth_type` | An indicator of the intended type of authentication. Allowed values: `none`, `username_only`, `basic`, and `token`. This property is optional. The authentication type can be inferred from `token`, `username`, and `password`. If a `token` value is provided, token authentication is used. Otherwise, if both `username` and `password` values are provided, basic authentication is used. | |
| `adbc.spark.host`      | Host name for the data source. Do not include scheme or port number. Example: `sparkserver.region.cloudapp.azure.com` |  |
| `adbc.spark.port`      | The port number the data source listens on for a new connections. | `443` |
| `adbc.spark.path`      | The URI path on the data source server. Example: `sql/protocolv1/o/0123456789123456/01234-0123456-source` | |
| `adbc.spark.token`     | For token-based authentication, the token to be authenticated on the data source. Example: `abcdef0123456789` | |
| `uri`                  | The full URI that includes scheme, host, port and path. Only one of options `uri` or `adbc.hive.host` can be provided. | |
| `username`             | The user name used for basic authentication | |
| `password`             | The password for the user name used for basic authentication. | |
| `adbc.spark.data_type_conv` | Comma-separated list of data conversion options. Each option indicates the type of conversion to perform on data returned from the Spark server. <br><br>Allowed values: `none`, `scalar`. <br><br>Option `none` indicates there is no conversion from Spark type to native type (i.e., no conversion from String to Timestamp for Apache Spark over HTTP). Example `adbc.spark.conv_data_type=none`. <br><br>Option `scalar` will perform conversion (if necessary) from the Spark data type to corresponding Arrow data types for types `DATE/Date32/DateTime`, `DECIMAL/Decimal128/SqlDecimal`, and `TIMESTAMP/Timestamp/DateTimeOffset`. Example `adbc.spark.conv_data_type=scalar` | `scalar` |
| `adbc.spark.connect_timeout_ms` | Sets the timeout (in milliseconds) to open a new session. Values can be 0 (infinite) or greater than zero. | `30000` |
| `adbc.apache.statement.batch_size` | Sets the maximum number of rows to retrieve in a single batch request. | `50000` |
| `adbc.apache.statement.polltime_ms` | If polling is necessary to get a result, this option sets the length of time (in milliseconds) to wait between polls. | `500` |
| `adbc.apache.statement.query_timeout_s` | Sets the maximum time (in seconds) for a query to complete. Values can be 0 (infinite) or greater than zero. | `60` |
| `adbc.apache.statement.is_metadata_command` | Indicate that the value of `AdbcStatement.SqlQuery` contains the name of a native metadata command. If set to `True`, it indicates a metadata command query whereas a value of `False` indicates a SQL command query. <br><br>Supported metadata commands include: `GetPrimaryKeys`, `GetCrossReference`, `GetCatalogs`, `GetSchemas`, `GetTables`, and `GetColumns`. | `False` |
| `adbc.get_metadata.target_catalog` | The catalog name (or pattern) when used with a metadata command query. <br><br>Supported metadata commands include: `GetPrimaryKeys`, `GetCrossReference`, `GetSchemas`, `GetTables`, and `GetColumns`. | |
| `adbc.get_metadata.target_db_schema` | The schema name (or pattern) when used with a metadata command query. <br><br>Supported metadata commands include: `GetPrimaryKeys`, `GetCrossReference`, `GetSchemas`, `GetTables`, and `GetColumns`. | |
| `adbc.get_metadata.target_table` | The table name (or pattern) when used with a metadata command query. <br><br>Supported metadata commands include: `GetPrimaryKeys`, `GetCrossReference`, `GetSchemas`, `GetTables`, and `GetColumns`. | |
| `adbc.get_metadata.target_table_types` | The comma-separated list of table types when used with a metadata command query. <br><br>Supported metadata commands include: `GetTables`. | |
| `adbc.get_metadata.target_column` | The column name (or pattern) when used with a metadata command query. <br><br>Supported metadata commands include: `GetColumns`.  | |
| `adbc.get_metadata.foreign_target_catalog` | The foreign (i.e., child) catalog name (or pattern) when used with a metadata command query. <br><br>Supported metadata commands include: `GetCrossReference`. | |
| `adbc.get_metadata.foreign_target_db_schema` | The foreign (i.e., child) schema name (or pattern) when used with a metadata command query. <br><br>Supported metadata commands include: `GetCrossReference`. | |
| `adbc.get_metadata.foreign_target_table` | The foreign (i.e., child) table name (or pattern) when used with a metadata command query. <br><br>Supported metadata commands include: `GetCrossReference`. | |
| `adbc.http_options.tls.enabled` | If tls needs to enabled or not. One of `True`, `False` | `True` |
| `adbc.http_options.tls.disable_server_certificate_validation` | If tls/ssl server certificate validation needs to enabled or not. One of `True`, `False`. If set to True, all certificate validation errors are ignored | `False` |
| `adbc.http_options.tls.allow_self_signed` | If self signed tls/ssl certificate needs to be allowed or not. One of `True`, `False` | `False` |
| `adbc.http_options.tls.allow_hostname_mismatch` | If hostname mismatch is allowed for ssl. One of `True`, `False` | `False` |
| `adbc.http_options.tls.trusted_certificate_path` | The full path of the tls/ssl certificate .pem file containing custom CA certificates for verifying the server when connecting over TLS | `` |

## Timeout Configuration

Timeouts have a hierarchy to their behavior. As specified above, the `adbc.spark.connect_timeout_ms` is analogous to a ConnectTimeout and used to initially establish a new session with the server.

The `adbc.apache.statement.query_timeout_s` is analogous to a CommandTimeout for any subsequent calls to the server for requests, including metadata calls and executing queries.

The `adbc.apache.statement.polltime_ms` specifies the time between polls to the service, up to the limit specifed by `adbc.apache.statement.query_timeout_s`.

## Spark Types

The following table depicts how the Spark ADBC driver converts a Spark type to an Arrow type and a .NET type:

### Apache Spark over HTTP (adbc.spark.data_type_conv = ?)

| Spark Type           | Arrow Type (`none`) | C# Type (`none`) | Arrow Type (`scalar`) | C# Type (`scalar`) |
| :---                 | :---:      | :---:   | :---:                 | :---:              |
| ARRAY*               | String     | string  | | |
| BIGINT               | Int64      | long | | |
| BINARY               | Binary     | byte[] | | |
| BOOLEAN              | Boolean    | bool | | |
| CHAR                 | String     | string | | |
| DATE*                | *String*   | *string* | Date32 | DateTime |
| DECIMAL*             | *String*   | *string* | Decimal128 | SqlDecimal |
| DOUBLE               | Double     | double | | |
| FLOAT                | *Double*   | *double* | Float | float |
| INT                  | Int32      | int | | |
| INTERVAL_DAY_TIME+   | String     | string | | |
| INTERVAL_YEAR_MONTH+ | String     | string | | |
| MAP*                 | String     | string | | |
| NULL                 | String     | string | | |
| SMALLINT             | Int16      | short | | |
| STRING               | String     | string | | |
| STRUCT*              | String     | string | | |
| TIMESTAMP*           | *String*   | *string* | Timestamp | DateTimeOffset |
| TINYINT              | Int8       | sbyte | | |
| UNION                | String     | string | | |
| USER_DEFINED         | String     | string | | |
| VARCHAR              | String     | string | | |

\* Types are returned as strings instead of "native" types<br>
\+ Interval types are returned as strings

## Supported Variants

### Apache Spark over HTPP

Support for Spark over HTTP is initial.

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

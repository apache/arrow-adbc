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

# Hive Driver

## Database and Connection Properties

Properties should be passed in the call to `HiveServer2Driver.Open`,
but can also be passed in the call to `AdbcDatabase.Connect`.

| Property               | Description | Default |
| :---                   | :---        | :---    |
| `adbc.hive.transport_type`      | (Required) Indicates the Hive transport type. `http` | |
| `adbc.hive.auth_type` | An indicator of the intended type of authentication. Allowed values: `none`, `username_only` and `basic`. This property is optional. The authentication type can be inferred from `username`, and `password`. | |
| `adbc.hive.host`      | Host name for the data source. Do not include scheme or port number. Example: `hiveserver.region.cloudapp.azure.com` |  |
| `adbc.hive.port`      | The port number the data source listens on for a new connections. | `443` |
| `adbc.hive.path`      | The URI path on the data source server. Example: `/hive2` | |
| `uri`                  | The full URI that includes scheme, host, port and path. Only one of options `uri` or `adbc.hive.host` can be provided. | |
| `username`             | The user name used for basic authentication | |
| `password`             | The password for the user name used for basic authentication. | |
| `adbc.hive.data_type_conv` | Comma-separated list of data conversion options. Each option indicates the type of conversion to perform on data returned from the Hive server. <br><br>Allowed values: `none`, `scalar`. <br><br>Option `none` indicates there is no conversion from Hive type to native type (i.e., no conversion from String to Timestamp for Apache Hive over HTTP). Example `adbc.hive.conv_data_type=none`. <br><br>Option `scalar` will perform conversion (if necessary) from the Hive data type to corresponding Arrow data types for types `DATE/Date32/DateTime`, `DECIMAL/Decimal128/SqlDecimal`, and `TIMESTAMP/Timestamp/DateTimeOffset`. Example `adbc.hive.conv_data_type=scalar` | `scalar` |
| `adbc.hive.tls_options` | Comma-separated list of TLS/SSL options. Each option indicates the TLS/SSL option when connecting to a Hive server. <br><br>Allowed values: `allow_self_signed`, `allow_hostname_mismatch`. <br><br>Option `allow_self_signed` allows certificate errors due to an unknown certificate authority, typically when using a self-signed certificate. Option `allow_hostname_mismatch` allow certificate errors due to a mismatch of the hostname. (e.g., when connecting through an SSH tunnel). Example `adbc.hive.tls_options=allow_self_signed` | |
| `adbc.hive.connect_timeout_ms` | Sets the timeout (in milliseconds) to open a new session. Values can be 0 (infinite) or greater than zero. | `30000` |
| `adbc.apache.statement.batch_size` | Sets the maximum number of rows to retrieve in a single batch request. | `50000` |
| `adbc.apache.statement.polltime_ms` | If polling is necessary to get a result, this option sets the length of time (in milliseconds) to wait between polls. | `500` |
| `adbc.apache.statement.query_timeout_s` | Sets the maximum time (in seconds) for a query to complete. Values can be 0 (infinite) or greater than zero. | `60` |

## Timeout Configuration

Timeouts have a hierarchy to their behavior. As specified above, the `adbc.hive.connect_timeout_ms` is analogous to a ConnectTimeout and used to initially establish a new session with the server.

The `adbc.apache.statement.query_timeout_s` is analogous to a CommandTimeout for any subsequent calls to the server for requests, including metadata calls and executing queries.

The `adbc.apache.statement.polltime_ms` specifies the time between polls to the service, up to the limit specifed by `adbc.apache.statement.query_timeout_s`.

## Hive Data Types

The following table depicts how the Hive ADBC driver converts a Hive type to an Arrow type and a .NET type:

### Apache Hive over HTTP (adbc.hive.data_type_conv = ?)

| Hive Type           | Arrow Type (`none`) | C# Type (`none`) | Arrow Type (`scalar`) | C# Type (`scalar`) |
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
| VARCHAR              | String     | string | | |

\* Types are returned as strings instead of "native" types<br>
\+ Interval types are returned as strings

## Supported Variants

### Apache Hive over HTTP

Support for Hive over HTTP is the most mature.

### Azure Hive HDInsight

To read data from Azure HDInsight Hive Cluster, use the following parameters:
adbc.hive.type = "http"
adbc.hive.port = "443"
adbc.hive.path = "/hive2"
adbc.hive.host = $"{clusterHostName}"
username = $"{clusterUserName}"
password = $"{clusterPassword}"

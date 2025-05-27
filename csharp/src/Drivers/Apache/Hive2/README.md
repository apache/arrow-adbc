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
| `adbc.hive.connect_timeout_ms` | Sets the timeout (in milliseconds) to open a new session. Values can be 0 (infinite) or greater than zero. | `30000` |
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
| `adbc.proxy_options.use_proxy` | Whether to use a proxy for HTTP connections. Only feature-complete in Spark driver. One of `True`, `False` | `False` |
| `adbc.proxy_options.proxy_host` | Hostname or IP address of the proxy server. Only feature-complete in Spark driver. Required when use_proxy is True | |
| `adbc.proxy_options.proxy_port` | Port number of the proxy server. Only feature-complete in Spark driver. Required when use_proxy is True | |
| `adbc.proxy_options.proxy_ignore_list` | Comma-separated list of hosts or domains that should bypass the proxy. Only feature-complete in Spark driver. For example: "localhost,127.0.0.1,.internal.domain.com". Supports wildcard patterns like "*.internal.domain.com" | |
| `adbc.proxy_options.proxy_auth` | Whether to enable proxy authentication. Only feature-complete in Spark driver. One of `True`, `False` | `False` |
| `adbc.proxy_options.proxy_uid` | Username for proxy authentication. Only feature-complete in Spark driver. Required when proxy_auth is True | |
| `adbc.proxy_options.proxy_pwd` | Password for proxy authentication. Only feature-complete in Spark driver. Required when proxy_auth is True | |
| `adbc.telemetry.trace_parent` | The [trace parent](https://www.w3.org/TR/trace-context/#traceparent-header) identifier for an existing [trace context](https://www.w3.org/TR/trace-context/) \(span/activity\) in a tracing system. This option is most likely to be set using `Statement.SetOption` to set the trace parent for driver interaction with a specific `Statement`. However, it can also be set using `Driver.Open`, `Database.Connect` or `Connection.SetOption` to set the trace parent for all interactions with the driver on that specific `Connetion`. |  |

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

## Tracing Support

OpenTelemetry tracing is now supported.

One of the following exporters can be enabled via the environment variable `OTEL_TRACES_EXPORTER`.
If the environment variable is not set or empty, it behaves the same as for `none`.

| Exporter | Description |
| :--| : -- |
| `none` | No exporter is activated |
| `otlp` | The [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) exporter is activated with default settings. |
| `console` | The console exporter is activated which writes human-readable trace information to `stdout` |
| `adbcfile` | The ADBC file exporter is activated. It writes trace information into files stored in the local application data folder. This folder is `~\AppData\Local\Apache.Arrow.Adbc\traces` (Windows) or `~/.local/share/Apache.Arrow.Adbc/traces` (MacOS/Linus) |

### Configuring the OTLP Exporter

When the `otlp` exporter is activiated, it uses the default settings.

To override default settings, set the appropriate environement variables for *traces* as detailed in [OTLP Exporter Configuration](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/).

A typical use case scenario is to [install](https://opentelemetry.io/docs/collector/installation/) and [configure](https://opentelemetry.io/docs/collector/configuration/) an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/).
The Collector can be configure to receive trace messages from the driver and export them in various ways.

*Note*: By default, the OTL exporter and Collector use unencrypted communication on `localhost`.
Ensure to set the [environment variable](https://opentelemetry.io/docs/specs/otel/protocol/exporter/) `OTEL_EXPORTER_OTLP_INSECURE` to `true`, in this scenario.

Ensure to follow [Collector configuration best practices](https://opentelemetry.io/docs/security/config-best-practices/).

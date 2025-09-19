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

![Vendor: Apache Spark](https://img.shields.io/badge/vendor-Apache%20Spark-blue?style=flat-square)
![Implementation: C#](https://img.shields.io/badge/language-C%23-violet?style=flat-square)
![Status: Experimental](https://img.shields.io/badge/status-experimental-red?style=flat-square)

## Database and Connection Properties

Properties should be passed in the call to `SparkDriver.Open`,
but can also be passed in the call to `AdbcDatabase.Connect`.

| Property               | Description | Default |
| :---                   | :---        | :---    |
| `adbc.spark.type`      | (Required) Indicates the Spark server type. Currently only `http` (future: `standard`) | |
| `adbc.spark.auth_type` | An indicator of the intended type of authentication. Allowed values: `none`, `username_only`, `basic`, `token`, and `auth_type`. This property is optional. The authentication type can be inferred from `token`, `username`, and `password`. If a `token` value is provided, token authentication is used. Otherwise, if both `username` and `password` values are provided, basic authentication is used. If `auth_type` is provided, token authentication is used by default, unless the driver or configuration explicitly overrides this behavior (e.g., in drivers like DatabricksDriver, which may support other grant types) | |
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
| `adbc.telemetry.trace_parent` | The [trace parent](https://www.w3.org/TR/trace-context/#traceparent-header) identifier for an existing [trace context](https://www.w3.org/TR/trace-context/) \(span/activity\) in a tracing system. This option is most likely to be set using `Statement.SetOption` to set the trace parent for driver interaction with a specific `Statement`. However, it can also be set using `Driver.Open`, `Database.Connect` or `Connection.SetOption` to set the trace parent for all interactions with the driver on that specific `Connection`. |  |

## Timeout Configuration

Timeouts have a hierarchy to their behavior. As specified above, the `adbc.spark.connect_timeout_ms` is analogous to a ConnectTimeout and used to initially establish a new session with the server.

The `adbc.apache.statement.query_timeout_s` is analogous to a CommandTimeout for any subsequent calls to the server for requests, including metadata calls and executing queries.

The `adbc.apache.statement.polltime_ms` specifies the time between polls to the service, up to the limit specified by `adbc.apache.statement.query_timeout_s`.

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

## Tracing Support

OpenTelemetry tracing is now supported.

One of the following exporters can be enabled via the environment variable `OTEL_TRACES_EXPORTER`.
If the environment variable is not set or empty, it behaves the same as for `none`.

| Exporter | Description |
| :--| : -- |
| `none` | No exporter is activated |
| `otlp` | The [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) exporter is activated with default settings. |
| `console` | The console exporter is activated which writes human-readable trace information to `stdout` |
| `adbcfile` | The ADBC file exporter is activated. It writes trace information into files stored in the local application data folder. This folder is `~\AppData\Local\Apache.Arrow.Adbc\traces` (Windows) or `~/.local/share/Apache.Arrow.Adbc/traces` (MacOS/Linux) |

### Configuring the OTLP Exporter

When the `otlp` exporter is activated, it uses the default settings.

To override default settings, set the appropriate environment variables for *traces* as detailed in [OTLP Exporter Configuration](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/).

A typical use case scenario is to [install](https://opentelemetry.io/docs/collector/installation/) and [configure](https://opentelemetry.io/docs/collector/configuration/) an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/).
The Collector can be configure to receive trace messages from the driver and export them in various ways.

*Note*: By default, the OTL exporter and Collector use unencrypted communication on `localhost`.
Ensure to set the [environment variable](https://opentelemetry.io/docs/specs/otel/protocol/exporter/) `OTEL_EXPORTER_OTLP_INSECURE` to `true`, in this scenario.

Ensure to follow [Collector configuration best practices](https://opentelemetry.io/docs/security/config-best-practices/).

## Tracing

### Tracing Exporters

To enable tracing messages to be observed, a tracing exporter needs to be activated.
Use either the environment variable `OTEL_TRACES_EXPORTER` or the parameter `adbc.traces.exporter` to select one of the
supported exporters. The parameter has precedence over the environment variable.

The following exporters are supported:

| Exporter | Description |
| --- | --- |
| `adbcfile` | Exports traces to rotating files in a folder. |

Note: _The first connection to activate tracing will enable tracing for
any later connections that are created in that process._ (This behavior may change in future implementations.)

#### File Exporter (adbcfile)

Rotating trace files are written to a folder. The file names are created with the following pattern:
`apache.arrow.adbc.drivers.bigquery-<YYYY-MM-DD-HH-mm-ss-fff>-<process-id>.log`.

The folder used depends on the platform.

| Platform | Folder |
| --- | --- |
| Windows | `%LOCALAPPDATA%/Apache.Arrow.Adbc/Traces` |
| macOS   | `$HOME/Library/Application Support/Apache.Arrow.Adbc/Traces` |
| Linux   | `$HOME/.local/share/Apache.Arrow.Adbc/Traces` |

By default, up to 999 files of maximum size 1024 KB are written to
the trace folder.

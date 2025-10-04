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

# Databricks Driver

![Vendor: Databricks](https://img.shields.io/badge/vendor-Databricks-blue?style=flat-square)
![Implementation: C#](https://img.shields.io/badge/language-C%23-violet?style=flat-square)
![Status: Experimental](https://img.shields.io/badge/status-experimental-red?style=flat-square)

The Databricks ADBC driver is built on top of the Spark ADBC driver and inherits all of its [properties](../Apache/Spark/readme.md), plus additional Databricks-specific functionality.

## Database and Connection Properties

**Note**: The Databricks driver inherits all properties from the [Spark driver](../Apache/Spark/readme.md). The properties below are Databricks-specific additions.

## Configuration Methods

The Databricks driver supports multiple ways to configure properties:

### 1. Direct Property Configuration
Pass properties directly when creating the driver connection (traditional method).

### 2. Environment Variable Configuration
Configure properties using a JSON file loaded via environment variables:

1. **Create a JSON configuration file** with standard ADBC parameters:
```json
{
  "adbc.databricks.driver_config_take_precedence": "true",
  "adbc.databricks.enable_pk_fk": "false",
  "adbc.connection.catalog": "my_catalog",
  "adbc.connection.db_schema": "my_schema"
}
```
## Note

All values in the JSON configuration file **must be strings** (including numbers, booleans, and file paths). For example, use `"true"` instead of `true`, and `"4443"` instead of `4443`.

### Example: Using mitmproxy to Inspect Thrift Traffic

To inspect Thrift traffic using [mitmproxy](https://mitmproxy.org/), you can configure the Databricks driver to use a local proxy with TLS interception. Below is an example JSON configuration:
```json
{
  "adbc.databricks.driver_config_take_precedence": "true",
  "adbc.proxy_options.use_proxy" : "true",
  "adbc.proxy_options.proxy_host" : "localhost",
  "adbc.proxy_options.proxy_port" : "4443",
  "adbc.http_options.tls.enabled": "true",
  "adbc.http_options.tls.allow_self_signed" : "true",
  "adbc.http_options.tls.disable_server_certificate_validation" : "true",
  "adbc.http_options.tls.allow_hostname_mismatch" : "true",
  "adbc.http_options.tls.trusted_certificate_path" : "C:\\your-path-to\\mitmproxy-ca-cert.pem"
}
```

2. **Set the system environment variable** `DATABRICKS_CONFIG_FILE` to point to your JSON file:
   1. Open System Properties → Advanced → Environment Variables
   2. Add new system variable: Name=`DATABRICKS_CONFIG_FILE`, Value=`C:\path\to\your\config.json`

3. **Property Merging Behavior**:
   - By default: Constructor/code properties **override** environment config properties
   - With `"adbc.databricks.driver_config_take_precedence": "true"`: Environment config properties **override** constructor/code properties

### 3. Hybrid Configuration
You can combine both methods - the driver will automatically merge environment config with constructor properties based on the precedence setting.

**Use Cases**:
- **PowerBI Integration**: Set system-wide defaults via environment config while allowing connection-specific overrides


### Authentication Properties

| Property | Description | Default |
| :--- | :--- | :--- |
| `adbc.databricks.oauth.grant_type` | The OAuth grant type. Supported values: `access_token` (personal access token), `client_credentials` (OAuth client credentials flow) | `access_token` |
| `adbc.databricks.oauth.client_id` | The OAuth client ID (when using `client_credentials` grant type) | |
| `adbc.databricks.oauth.client_secret` | The OAuth client secret (when using `client_credentials` grant type) | |
| `adbc.databricks.oauth.scope` | The OAuth scope (when using `client_credentials` grant type) | `sql` |
| `adbc.databricks.token_renew_limit` | Minutes before token expiration to start renewing the token. Set to 0 to disable automatic renewal | `0` |
| `adbc.databricks.identity_federation_client_id` | The client ID of the service principal when using workload identity federation | |

### CloudFetch Properties

CloudFetch is Databricks' high-performance result retrieval system that downloads result data directly from cloud storage.

| Property | Description | Default |
| :--- | :--- | :--- |
| `adbc.databricks.cloudfetch.enabled` | Whether to use CloudFetch for retrieving results | `true` |
| `adbc.databricks.cloudfetch.lz4.enabled` | Whether the client can decompress LZ4 compressed results | `true` |
| `adbc.databricks.cloudfetch.max_bytes_per_file` | Maximum bytes per file for CloudFetch. Supports unit suffixes (B, KB, MB, GB). Examples: `20MB`, `1024KB`, `20971520` | `20MB` |
| `adbc.databricks.cloudfetch.parallel_downloads` | Maximum number of parallel downloads | `3` |
| `adbc.databricks.cloudfetch.prefetch_count` | Number of files to prefetch | `2` |
| `adbc.databricks.cloudfetch.memory_buffer_size_mb` | Maximum memory buffer size in MB for prefetched files | `200` |
| `adbc.databricks.cloudfetch.prefetch_enabled` | Whether CloudFetch prefetch functionality is enabled | `true` |
| `adbc.databricks.cloudfetch.max_retries` | Maximum number of retry attempts for downloads | `3` |
| `adbc.databricks.cloudfetch.retry_delay_ms` | Delay in milliseconds between retry attempts | `500` |
| `adbc.databricks.cloudfetch.timeout_minutes` | Timeout in minutes for HTTP operations | `5` |
| `adbc.databricks.cloudfetch.url_expiration_buffer_seconds` | Buffer time in seconds before URL expiration to trigger refresh | `60` |
| `adbc.databricks.cloudfetch.max_url_refresh_attempts` | Maximum number of URL refresh attempts | `3` |

### Databricks-Specific Properties

| Property | Description | Default |
| :--- | :--- | :--- |
| `adbc.connection.catalog` | Optional default catalog for the session | |
| `adbc.connection.db_schema` | Optional default schema for the session | |
| `adbc.databricks.enable_direct_results` | Whether to enable the use of direct results when executing queries | `true` |
| `adbc.databricks.apply_ssp_with_queries` | Whether to apply server-side properties (SSP) with queries. If false, SSP will be applied when opening the session | `false` |
| `adbc.databricks.ssp_*` | Server-side properties prefix. Properties with this prefix will be passed to the server by executing "set key=value" queries | |
| `adbc.databricks.enable_multiple_catalog_support` | Whether to use multiple catalogs | `true` |
| `adbc.databricks.enable_pk_fk` | Whether to enable primary key foreign key metadata calls | `true` |
| `adbc.databricks.use_desc_table_extended` | Whether to use DESC TABLE EXTENDED to get extended column metadata when supported by DBR | `true` |
| `adbc.databricks.enable_run_async_thrift` | Whether to enable RunAsync flag in Thrift operations | `true` |
| `adbc.databricks.driver_config_take_precedence` | Whether driver configuration overrides passed-in properties during configuration merging | `false` |

### Tracing Properties

| Property | Description | Default |
| :--- | :--- | :--- |
| `adbc.databricks.trace_propagation.enabled` | Whether to propagate trace parent headers in HTTP requests | `true` |
| `adbc.databricks.trace_propagation.header_name` | The name of the HTTP header to use for trace parent propagation | `traceparent` |
| `adbc.databricks.trace_propagation.state_enabled` | Whether to include trace state header in HTTP requests | `false` |

## Authentication Methods

The Databricks ADBC driver supports the following authentication methods:

### 1. Token-based Authentication
Using a [Databricks personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html):

- Set `adbc.spark.auth_type` to `oauth`
- Set `adbc.databricks.oauth.grant_type` to `access_token` (this is the default if not specified)
- Set `adbc.spark.oauth.access_token` to your Databricks personal access token

### 2. OAuth Client Credentials Flow
For machine-to-machine (m2m) authentication:

- Set `adbc.spark.auth_type` to `oauth`
- Set `adbc.databricks.oauth.grant_type` to `client_credentials`
- Set `adbc.databricks.oauth.client_id` to your OAuth client ID
- Set `adbc.databricks.oauth.client_secret` to your OAuth client secret
- Set `adbc.databricks.oauth.scope` to your auth scope (defaults to `sql`)

The driver will automatically handle token acquisition, renewal, and authentication with the Databricks service.

**Note**: Basic (username and password) authentication is not supported at this time.

## Server-Side Properties

Server-side properties allow you to configure Databricks session settings. Any property with the `adbc.databricks.ssp_` prefix will be passed to the server by executing `SET key=value` queries.

For example, setting `adbc.databricks.ssp_use_cached_result` to `true` will result in executing `SET use_cached_result=true` on the server when the session is opened.

The property name after the `ssp_` prefix becomes the server-side setting name.

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

## Tracing

### Tracing Exporters

To enable tracing messages to be observed, a tracing exporter needs to be activated.
Use either the environment variable `OTEL_TRACES_EXPORTER` or the parameter `adbc.traces.exporter` to select one of the
supported exporters. The parameter has precedence over the environment variable. The parameter must be set before
the connection is initialized.

The following exporters are supported:

| Exporter | Description |
| --- | --- |
| `adbcfile` | Exports traces to rotating files in a folder. |

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

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

## Configuration

Optional default catalog and default schema can be set for the session with `adbc.connection.catalog` and `adbc.connection.db_schema`.
The default catalog and schema will be used for subsequent metadata calls unless user specified different catalog/schema to use.

### Configuration File Support

The Databricks driver supports loading configuration parameters from a JSON file specified by an environment variable. This is useful when you need to configure multiple options but your application doesn't provide a way to set all of them directly.

To use a configuration file:

1. Create a JSON file with your configuration parameters. For example:

```json
{
  "adbc.connection.catalog": "my_catalog",
  "adbc.connection.db_schema": "my_schema",
  "adbc.databricks.cloudfetch.enabled": "true",
  "adbc.databricks.enable_direct_results": "true"
}
```

2. Set the `ADBC_CONFIG_FILE` environment variable to the path of your JSON file:

```bash
# Linux/macOS
export ADBC_CONFIG_FILE=/path/to/your/config.json

# Windows Command Prompt
set ADBC_CONFIG_FILE=C:\path\to\your\config.json

# Windows PowerShell
$env:ADBC_CONFIG_FILE="C:\path\to\your\config.json"
```

Parameters specified directly when creating the connection take precedence over parameters from the configuration file.

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

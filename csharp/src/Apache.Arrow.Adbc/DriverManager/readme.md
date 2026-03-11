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
# Apache.Arrow.Adbc.DriverManager

A .NET implementation of the ADBC Driver Manager, based on the C interface defined in `adbc_driver_manager.h`.

## Features

- **Driver discovery**: search for ADBC drivers by name across configurable directories (environment variable, user-level, system-level).
- **TOML manifest loading**: locate drivers via `.toml` manifest files that specify the shared library path.
- **Connection profiles**: load reusable connection configurations (driver + options) from `.toml` profile files.
- **Custom profile providers**: plug in your own `IConnectionProfileProvider` implementation.

## TOML Manifest / Profile Format

### Unmanaged Driver Example (Snowflake)

For unmanaged drivers loaded from native shared libraries:

```toml
version = 1
driver = "libadbc_driver_snowflake"
entrypoint = "AdbcDriverSnowflakeInit"

[options]
adbc.snowflake.sql.account = "myaccount"
adbc.snowflake.sql.warehouse = "mywarehouse"
adbc.snowflake.sql.auth_type = "auth_snowflake"
username = "myuser"
password = "env_var(SNOWFLAKE_PASSWORD)"
```

### Managed Driver Example (BigQuery)

For managed .NET drivers:

```toml
version = 1
driver = "C:\\path\\to\\Apache.Arrow.Adbc.Drivers.BigQuery.dll"
driver_type = "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"

[options]
adbc.bigquery.project_id = "my-project"
adbc.bigquery.auth_type = "service"
adbc.bigquery.json_credential = "env_var(BIGQUERY_JSON_CREDENTIAL)"
```

### Format Notes

- Boolean option values are converted to the string equivalents `"true"` or `"false"`.
- Values of the form `env_var(ENV_VAR_NAME)` are expanded from the named environment variable at connection time.
- For unmanaged drivers, use `driver` for the library path and `entrypoint` for the initialization function.
- For managed drivers, use `driver` for the assembly path and `driver_type` for the fully-qualified type name.

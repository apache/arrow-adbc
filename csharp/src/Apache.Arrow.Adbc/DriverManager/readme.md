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

## Security Features

The Driver Manager includes security features to protect against common attacks when loading drivers dynamically.

### Path Traversal Protection

The driver manager validates all paths from manifest files to prevent path traversal attacks:

- Paths containing `..` sequences are rejected
- Paths containing null bytes are rejected
- Relative paths in manifests are validated to ensure they don't escape the manifest directory

```csharp
// Manual path validation
DriverManagerSecurity.ValidatePathSecurity(userProvidedPath, "driverPath");

// Validate and resolve a relative path against a base directory
string resolvedPath = DriverManagerSecurity.ValidateAndResolveManifestPath(
    manifestDirectory, relativePath);
```

### Driver Allowlist

Restrict which drivers can be loaded by configuring an allowlist:

```csharp
// Only allow drivers from specific directories
DriverManagerSecurity.Allowlist = new DirectoryAllowlist(new[]
{
    @"C:\Program Files\ADBC\Drivers",
    @"C:\MyApp\TrustedDrivers"
});

// Only allow specific managed driver types
DriverManagerSecurity.Allowlist = new TypeAllowlist(new[]
{
    "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver",
    "Apache.Arrow.Adbc.Drivers.Snowflake.SnowflakeDriver"
});

// Combine multiple restrictions (all must pass)
DriverManagerSecurity.Allowlist = new CompositeAllowlist(
    new DirectoryAllowlist(trustedDirectories),
    new TypeAllowlist(trustedTypes)
);
```

### Audit Logging

Log all driver load attempts for security monitoring:

```csharp
public class MyAuditLogger : IDriverLoadAuditLogger
{
    public void LogDriverLoadAttempt(DriverLoadAttempt attempt)
    {
        Console.WriteLine($"[{attempt.TimestampUtc:O}] {attempt.LoadMethod}: " +
            $"{attempt.DriverPath} - {(attempt.Success ? "SUCCESS" : "FAILED: " + attempt.ErrorMessage)}");
    }
}

// Enable audit logging
DriverManagerSecurity.AuditLogger = new MyAuditLogger();
```

The `DriverLoadAttempt` class captures:
- `TimestampUtc` - When the load was attempted
- `DriverPath` - Path to the driver
- `TypeName` - Type name for managed drivers (null for native)
- `ManifestPath` - Path to manifest if used
- `Success` - Whether the load succeeded
- `ErrorMessage` - Error details if failed
- `LoadMethod` - Which method was used (LoadDriver, LoadManagedDriver, etc.)

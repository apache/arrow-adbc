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

### Connection Profile Example (Snowflake)

For unmanaged drivers loaded from native shared libraries:

```toml
profile_version = 1
driver = "libadbc_driver_snowflake"
entrypoint = "AdbcDriverSnowflakeInit"

[Options]
adbc.snowflake.sql.account = "myaccount"
adbc.snowflake.sql.warehouse = "mywarehouse"
adbc.snowflake.sql.auth_type = "auth_snowflake"
username = "myuser"
password = "env_var(SNOWFLAKE_PASSWORD)"
```

### Managed Driver Profile Example (BigQuery)

For managed .NET drivers:

```toml
profile_version = 1
driver = "C:\\path\\to\\Apache.Arrow.Adbc.Drivers.BigQuery.dll"
driver_type = "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"

[Options]
adbc.bigquery.project_id = "my-project"
adbc.bigquery.auth_type = "service"
adbc.bigquery.json_credential = "env_var(BIGQUERY_JSON_CREDENTIAL)"
```

### Format Notes

- Use `profile_version = 1` for the version field (legacy `version` is also supported for backward compatibility)
- Use `[Options]` for the options section (legacy `[options]` is also supported for backward compatibility)
- Boolean option values are converted to the string equivalents `"true"` or `"false"`.
- Values of the form `env_var(ENV_VAR_NAME)` are expanded from the named environment variable at connection time.
- For unmanaged drivers, use `driver` for the library path and `entrypoint` for the initialization function.
- For managed drivers, use `driver` for the assembly path and `driver_type` for the fully-qualified type name.

## Managed Driver Loading (.NET Core / .NET 8)

When loading managed .NET drivers using `LoadManagedDriver`, the driver manager uses `Assembly.LoadFrom()` which has different behavior on .NET Core/.NET 8 compared to .NET Framework:

### Dependency Resolution

On .NET Core/.NET 8, dependencies are resolved in the following order:

1. **The `.deps.json` file** - If present alongside the driver assembly, this file describes all dependencies and their locations. This is automatically generated when building with the .NET SDK.

2. **The assembly's directory** - Dependencies in the same directory as the driver are discovered automatically.

3. **The application's probing paths** - Standard .NET Core probing paths are searched.

### Best Practices for Driver Authors

1. **Include a `.deps.json` file** - Build your driver with the .NET SDK to automatically generate this file. It ensures all dependencies are properly resolved.

2. **Deploy dependencies alongside the driver** - Place all required DLLs in the same directory as your driver assembly.

3. **Consider self-contained deployment** - For maximum compatibility, publish your driver as a self-contained deployment with all dependencies included.

4. **Test on both .NET Framework and .NET 8** - Assembly loading behavior differs, so test on both platforms if you support both.

### Advanced Scenarios

For applications requiring isolated loading or explicit dependency resolution, consider using `AssemblyLoadContext` directly:

```csharp
// Example of isolated loading (requires .NET Core 3.0+)
var loadContext = new AssemblyLoadContext("MyDriverContext", isCollectible: true);
var assembly = loadContext.LoadFromAssemblyPath(driverPath);
// ... use the driver ...
loadContext.Unload(); // Unload when done (if collectible)
```

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

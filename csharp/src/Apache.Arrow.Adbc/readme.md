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
# Apache Arrow ADBC

An implementation of Arrow ADBC targeting .NET Standard 2.0 and .NET 6 or later.

## Driver Manager

The `Apache.Arrow.Adbc.DriverManager` namespace provides a .NET implementation of the ADBC Driver Manager, based on the C interface defined in `adbc_driver_manager.h`.

### Features

- **Driver discovery**: search for ADBC drivers by name across configurable directories (environment variable, user-level, system-level).
- **TOML driver manifests**: locate drivers via `.toml` manifest files that specify the shared library path per platform.
- **Connection profiles**: load reusable connection configurations (driver + options) from `.toml` profile files.
- **Managed (.NET) drivers**: load .NET drivers via a scheme-prefixed `entrypoint` (`dotnet:` for .NET 5+, `netfx:` for .NET Framework 4.x).
- **Custom profile providers**: plug in your own `IConnectionProfileProvider` implementation.

### Driver Manifest Format

A *driver manifest* is a TOML file describing where a driver lives and how to load it. The format is shared across all ADBC driver-manager implementations and documented in `docs/source/format/driver_manifests.rst`.

#### Native Driver Manifest Example (Snowflake)

```toml
manifest_version = 1

name = "Snowflake"
version = "1.5.2"
publisher = "snowflake.com"

[Driver]
entrypoint = "AdbcDriverSnowflakeInit"

[Driver.shared]
windows_amd64 = "C:\\path\\to\\adbc_driver_snowflake.dll"
linux_amd64   = "/usr/local/lib/libadbc_driver_snowflake.so"
macos_arm64   = "/opt/homebrew/lib/libadbc_driver_snowflake.dylib"
```

#### Managed Driver Manifest Example (BigQuery)

Managed .NET drivers use a scheme-prefixed `entrypoint`:

- `dotnet:` for modern .NET (.NET 5 and later, including .NET 8 / .NET 10)
- `netfx:` for .NET Framework 4.x

The host process rejects a manifest whose scheme doesn't match its runtime, so a `dotnet:` manifest on a .NET Framework process (or vice versa) fails with a clear error rather than mysteriously failing inside the assembly loader.

```toml
manifest_version = 1

name = "BigQuery"
version = "1.2.0"

[Driver]
entrypoint = "dotnet:Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"
shared = "Apache.Arrow.Adbc.Drivers.BigQuery.dll"
```

`shared` is relative to the manifest's directory. Managed .NET assemblies are platform-neutral, so the single-string form of `shared` is usually appropriate; the platform-tuple table is also accepted.

### Connection Profile Format

A *connection profile* points at a driver and supplies options to apply when opening a database. Profiles can name a driver by manifest name (resolved against the standard search paths), by direct path to a shared library, or by direct path to a manifest.

```toml
profile_version = 1
driver = "snowflake"

[Options]
adbc.snowflake.sql.account = "myaccount"
adbc.snowflake.sql.warehouse = "mywarehouse"
password = "{{ env_var(SNOWFLAKE_PASSWORD) }}"
```

If the profile points directly at a shared library that uses a non-default entrypoint (or at a managed assembly that needs a `dotnet:` / `netfx:` selector), supply it through the `entrypoint` option. The driver manager consumes that option and does not forward it to the driver:

```toml
profile_version = 1
driver = "C:\\path\\to\\Apache.Arrow.Adbc.Drivers.BigQuery.dll"

[Options]
entrypoint = "dotnet:Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"
adbc.bigquery.project_id = "my-project"
adbc.bigquery.json_credential = "{{ env_var(BIGQUERY_JSON_CREDENTIAL) }}"
```

#### Format Notes

- Use `profile_version = 1` for the version field (legacy `version` is also supported for backward compatibility)
- Use `[Options]` for the options section (legacy `[options]` is also supported for backward compatibility)
- Boolean option values are converted to the string equivalents `"true"` or `"false"`.
- String values may contain `{{ env_var(NAME) }}` placeholders, which are expanded from process environment variables when `ResolveEnvVars()` is called. The `{{` and `}}` delimiters serve as escapes: any text outside placeholders is treated literally. Placeholders may appear anywhere inside a value and may be repeated. A missing environment variable expands to an empty string. Only `env_var(NAME)` is recognized; other content inside a placeholder is an error.

### Managed Driver Loading (.NET Core / .NET 8)

When loading managed .NET drivers using `LoadManagedDriver`, the driver manager uses `Assembly.LoadFrom()` which has different behavior on .NET Core/.NET 8 compared to .NET Framework:

#### Dependency Resolution

On .NET Core/.NET 8, dependencies are resolved in the following order:

1. **The `.deps.json` file** - If present alongside the driver assembly, this file describes all dependencies and their locations. This is automatically generated when building with the .NET SDK.

2. **The assembly's directory** - Dependencies in the same directory as the driver are discovered automatically.

3. **The application's probing paths** - Standard .NET Core probing paths are searched.

#### Best Practices for Driver Authors

1. **Include a `.deps.json` file** - Build your driver with the .NET SDK to automatically generate this file. It ensures all dependencies are properly resolved.

2. **Deploy dependencies alongside the driver** - Place all required DLLs in the same directory as your driver assembly.

3. **Consider self-contained deployment** - For maximum compatibility, publish your driver as a self-contained deployment with all dependencies included.

4. **Test on both .NET Framework and .NET 8** - Assembly loading behavior differs, so test on both platforms if you support both.

#### Advanced Scenarios

For applications requiring isolated loading or explicit dependency resolution, consider using `AssemblyLoadContext` directly:

```csharp
// Example of isolated loading (requires .NET Core 3.0+)
var loadContext = new AssemblyLoadContext("MyDriverContext", isCollectible: true);
var assembly = loadContext.LoadFromAssemblyPath(driverPath);
// ... use the driver ...
loadContext.Unload(); // Unload when done (if collectible)
```

### Security Features

The Driver Manager includes security features to protect against common attacks when loading drivers dynamically.

#### Path Traversal Protection

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

#### Driver Allowlist

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

#### Audit Logging

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

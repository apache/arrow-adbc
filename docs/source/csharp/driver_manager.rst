.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

==============
Driver Manager
==============

The ``Apache.Arrow.Adbc`` package provides a :term:`driver manager` interface for .NET. The driver manager allows you to dynamically load ADBC drivers at runtime without directly referencing them in your project.

Installation
============

.. code-block:: shell

   dotnet add package Apache.Arrow.Adbc

Features
========

The C# Driver Manager provides the following capabilities:

- **Driver discovery**: Search for ADBC drivers by name across configurable directories (environment variable, user-level, system-level)
- **TOML driver manifests**: Locate drivers via ``.toml`` :term:`manifest <driver manifest>` files that specify the shared library path per platform
- **Connection profiles**: Load reusable :doc:`connection configurations <../connection_profiles>` (driver + options) from ``.toml`` profile files
- **Managed (.NET) drivers**: Load .NET drivers via a scheme-prefixed entrypoint (``dotnet:`` for .NET 5+, ``netfx:`` for .NET Framework 4.x)
- **Custom profile providers**: Plug in your own ``IConnectionProfileProvider`` implementation
- **Security features**: Path traversal protection, driver allowlists, and audit logging

Usage
=====

The driver manager is different from using driver-specific packages like ``Apache.Arrow.Adbc.Drivers.BigQuery`` or ``Apache.Arrow.Adbc.Drivers.FlightSql``.

With driver-specific packages, you directly instantiate the driver class. The driver assembly is referenced in your project and loaded automatically.

With the driver manager, you use a single API regardless of the database you're connecting to. However, the user must install the driver as a separate step. :term:`Driver manifests <driver manifest>` and :doc:`connection profiles <../connection_profiles>` make this experience more straightforward.

Loading a Driver
----------------

The driver manager can load drivers in several ways:

**By driver name** (searches standard locations for a manifest file):

.. code-block:: csharp

   using Apache.Arrow.Adbc;
   using Apache.Arrow.Adbc.DriverManager;

   // Load by name (searches for snowflake.toml in standard locations)
   AdbcDriver driver = AdbcDriverManager.LoadDriver("snowflake");
   AdbcDatabase db = driver.Open(new Dictionary<string, string>
   {
       ["adbc.snowflake.sql.account"] = "myaccount",
       ["adbc.snowflake.sql.warehouse"] = "mywarehouse"
   });

**By manifest path**:

.. code-block:: csharp

   // Load from a specific manifest file
   AdbcDriver driver = AdbcDriverManager.LoadDriver("C:\\Drivers\\snowflake.toml");

**By shared library path** (for native drivers):

.. code-block:: csharp

   // Load a native (C/C++) driver directly
   AdbcDriver driver = AdbcDriverLoader.LoadDriver(
       "C:\\Drivers\\libadbc_driver_postgresql.dll",
       "AdbcDriverPostgreSQLInit"
   );

**By loading managed (.NET) drivers**:

.. code-block:: csharp

   // Load a .NET driver assembly
   AdbcDriver driver = AdbcDriverManager.LoadManagedDriver(
       "C:\\Drivers\\Apache.Arrow.Adbc.Drivers.BigQuery.dll",
       "Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"
   );

Using Connection Profiles
--------------------------

:doc:`Connection profiles <../connection_profiles>` allow you to store connection configuration in TOML files and reference them by name:

**Create a profile file** (``~/.adbc/profiles/my_postgres.toml``):

.. code-block:: toml

   profile_version = 1
   driver = "postgresql"

   [Options]
   uri = "postgresql://localhost:5432/mydb"
   username = "myuser"
   password = "{{ env_var(POSTGRES_PASSWORD) }}"

**Load and use the profile**:

.. code-block:: csharp

   using Apache.Arrow.Adbc.DriverManager;

   // Load profile by name
   ConnectionProfile profile = FilesystemProfileProvider.LoadProfile("my_postgres");

   // Resolve environment variables in the profile
   profile.ResolveEnvVars();

   // Load the driver specified in the profile
   AdbcDriver driver = AdbcDriverManager.LoadDriver(profile.DriverName);

   // Open database with profile options
   AdbcDatabase db = driver.Open(profile.GetAllOptions());

Driver Manifests
----------------

:term:`Driver manifests <driver manifest>` describe where a driver lives and how to load it. See :doc:`../format/driver_manifests` for the full specification.

**Native driver manifest** (``snowflake.toml``):

.. code-block:: toml

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

**Managed driver manifest** (``bigquery.toml``):

.. code-block:: toml

   manifest_version = 1

   name = "BigQuery"
   version = "1.2.0"

   [Driver]
   entrypoint = "dotnet:Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver"
   shared = "Apache.Arrow.Adbc.Drivers.BigQuery.dll"

The ``dotnet:`` or ``netfx:`` scheme prefix indicates a managed .NET driver. The ``shared`` path is relative to the manifest's directory.

Security
========

The Driver Manager includes security features to protect against common attacks.

Path Traversal Protection
--------------------------

All paths from manifest files are validated to prevent path traversal attacks:

.. code-block:: csharp

   // Manual path validation
   DriverManagerSecurity.ValidatePathSecurity(userProvidedPath, "driverPath");

   // Validate and resolve a relative path against a base directory
   string resolvedPath = DriverManagerSecurity.ValidateAndResolveManifestPath(
       manifestDirectory, relativePath);

Driver Allowlist
----------------

Restrict which drivers can be loaded:

.. code-block:: csharp

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

   // Combine multiple restrictions
   DriverManagerSecurity.Allowlist = new CompositeAllowlist(
       new DirectoryAllowlist(trustedDirectories),
       new TypeAllowlist(trustedTypes)
   );

Audit Logging
-------------

Log all driver load attempts for security monitoring:

.. code-block:: csharp

   public class MyAuditLogger : IDriverLoadAuditLogger
   {
       public void LogAttempt(DriverLoadAuditEntry entry)
       {
           Console.WriteLine($"[{entry.Timestamp:O}] {entry.Action}: {entry.Path}");
           if (entry.Exception != null)
           {
               Console.WriteLine($"  Failed: {entry.Exception.Message}");
           }
       }
   }

   // Register the logger
   DriverManagerSecurity.AuditLogger = new MyAuditLogger();

Dependency Resolution
=====================

When loading managed .NET drivers, dependencies are resolved differently depending on the .NET runtime:

**.NET Core / .NET 5+**

Dependencies are resolved in this order:

1. The ``.deps.json`` file (automatically generated when building with the .NET SDK)
2. The assembly's directory
3. The application's probing paths

**Best practices for driver authors:**

- Include a ``.deps.json`` file by building with the .NET SDK
- Deploy dependencies alongside the driver DLL
- Consider self-contained deployment for maximum compatibility
- Test on both .NET Framework and modern .NET if supporting both

API Reference
=============

The driver manager API is documented in the ``Apache.Arrow.Adbc.DriverManager`` namespace:

**Core classes:**

- ``AdbcDriverManager`` - Main entry point for loading drivers
- ``AdbcDriverLoader`` - Low-level driver loading
- ``ConnectionProfile`` - Represents a connection configuration
- ``FilesystemProfileProvider`` - Loads profiles from the filesystem
- ``DriverManifest`` - Represents a driver manifest
- ``DriverManagerSecurity`` - Security configuration

See the `Apache.Arrow.Adbc API documentation <https://arrow.apache.org/adbc/current/api/csharp/>`_ for complete details.

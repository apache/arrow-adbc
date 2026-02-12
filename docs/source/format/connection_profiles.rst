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

==================================
Driver Manager Connection Profiles
==================================

Overview
========

There are two ways to pass connection options to driver managers:

1. Directly specifying all connection options as arguments to driver manager functions in your
   application code. (see the `SetOption` family of functions in :doc:`specification` for details)
2. Referring to a **connection profile** which contains connection options, and optionally overriding
   some options in your application code.

The ADBC driver manager supports **connection profiles** that specify a driver and connection options
in a reusable configuration. This allows users to:

- Define connection information in files or environment variables
- Share connection configurations across applications
- Distribute standardized connection settings
- Avoid hardcoding driver names and credentials in application code

Profiles are loaded during ``AdbcDatabaseInit()`` before initializing the driver. Options
from the profile are applied automatically but do not override options already set via ``AdbcDatabaseSetOption()``.

Quick Start
===========

Using a Profile via URI
-----------------------

The simplest way to use a profile is through a URI:

.. code-block:: c

   AdbcDatabase database;
   AdbcDatabaseNew(&database, &error);
   AdbcDatabaseSetOption(&database, "uri", "profile://my_snowflake_prod", &error);
   AdbcDatabaseInit(&database, &error);

Using a Profile via Option
---------------------------

Alternatively, specify the profile name directly:

.. code-block:: c

   AdbcDatabase database;
   AdbcDatabaseNew(&database, &error);
   AdbcDatabaseSetOption(&database, "profile", "my_snowflake_prod", &error);
   AdbcDatabaseInit(&database, &error);

Profile File Format
===================

Filesystem-based profiles use TOML format with the following structure:

.. code-block:: toml

   version = 1
   driver = "snowflake"

   [options]
   # String options
   adbc.snowflake.sql.account = "mycompany"
   adbc.snowflake.sql.warehouse = "COMPUTE_WH"
   adbc.snowflake.sql.database = "PRODUCTION"
   adbc.snowflake.sql.schema = "PUBLIC"

   # Integer options
   adbc.snowflake.sql.client_session_keep_alive_heartbeat_frequency = 3600

   # Double options
   adbc.snowflake.sql.client_timeout = 30.5

   # Boolean options (converted to "true" or "false" strings)
   adbc.snowflake.sql.client_session_keep_alive = true

version
-------

- **Required**: Yes
- **Type**: Integer
- **Supported values**: ``1``

The ``version`` field specifies the profile format version. Currently, only version 1 is supported.
This will enable future changes while maintaining backward compatibility.

driver
------

- **Required**: No
- **Type**: String

The ``driver`` field specifies which ADBC driver to load. This can be:

- A driver name (e.g., ``"snowflake"``)
- A path to a shared library (e.g., ``"/usr/local/lib/libadbc_driver_snowflake.so"``)
- A path to a driver manifest (e.g., ``"/etc/adbc/drivers/snowflake.toml"``)

If omitted, the driver must be specified through other means (e.g., the ``driver`` option or ``uri`` parameter).
The driver will be loaded identically to if it was specified via ``AdbcDatabaseSetOption("driver", "<driver>")``.
For more detils, see :doc:`driver_manifests`.

Options Section
---------------

The ``[options]`` section contains driver-specific configuration options. Options can be of the following types:

**String values**
   Applied using ``AdbcDatabaseSetOption()``

   .. code-block:: toml

      adbc.snowflake.sql.account = "mycompany"
      adbc.snowflake.sql.warehouse = "COMPUTE_WH"

**Integer values**
   Applied using ``AdbcDatabaseSetOptionInt()``

   .. code-block:: toml

      adbc.snowflake.sql.client_session_keep_alive_heartbeat_frequency = 3600

**Double values**
   Applied using ``AdbcDatabaseSetOptionDouble()``

   .. code-block:: toml

      adbc.snowflake.sql.client_timeout = 30.5

**Boolean values**
   Converted to strings ``"true"`` or ``"false"`` and applied using ``AdbcDatabaseSetOption()``

   .. code-block:: toml

      adbc.snowflake.sql.client_session_keep_alive = true

Value Substitution
------------------

Profile values support substitution of environment variables and other dynamic content.
This allows profiles to reference sensitive information (like passwords or tokens) without
hardcoding them in the profile file. Dynamic values can be injected by the presence of the ``{{ }}`` syntax,
similar to many templating engines. Within the double curly braces, the driver manager can
recognize certain functions to perform substitutions.

Currently, the only recognized function is ``env_var()`` for environment variable substitution,
but this may be extended in the future to support other types of dynamic content.

.. important::
   Dynamic content substitution only applies to option **values**, not **keys**.

Environment Variable Substitution
'''''''''''''''''''''''''''''''''

Profile values can reference environment variables using the ``{{ env_var() }}`` syntax:

.. code-block:: toml

   version = 1
   driver = "adbc_driver_snowflake"

   [options]
   adbc.snowflake.sql.account = "{{ env_var(SNOWFLAKE_ACCOUNT) }}""
   adbc.snowflake.sql.auth_token = "{{ env_var(SNOWFLAKE_TOKEN) }}"
   adbc.snowflake.sql.warehouse = "COMPUTE_WH"

When the driver manager encounters ``{{ env_var(VAR_NAME) }}``, it replaces the value with the contents of environment variable ``VAR_NAME``. If the environment variable is not set, the value becomes an empty string.

Profile Search Locations
=========================

When using a profile name (not an absolute path), the driver manager searches for ``<profile_name>.toml`` in the following locations:

1. **Additional Search Paths** (if configured via ``AdbcDriverManagerDatabaseSetAdditionalSearchPathList()``)
2. **ADBC_PROFILE_PATH** environment variable (colon-separated on Unix, semicolon-separated on Windows)
3. **Conda Environment** (if built with Conda support and ``CONDA_PREFIX`` is set):
   - ``$CONDA_PREFIX/etc/adbc/profiles/``
4. **User Configuration Directory**:
   - Linux: ``~/.config/adbc/profiles/``
   - macOS: ``~/Library/Application Support/ADBC/Profiles/``
   - Windows: ``%LOCALAPPDATA%\ADBC\Profiles\``

The driver manager searches locations in order and uses the first matching profile file found.

Using Absolute Paths
--------------------

To specify an absolute path to a profile file:

.. code-block:: c

   // Via profile option
   AdbcDatabaseSetOption(&database, "profile", "/etc/adbc/profiles/production.toml", &error);

   // Via URI (must have .toml extension)
   AdbcDatabaseSetOption(&database, "uri", "profile:///etc/adbc/profiles/production.toml", &error);

Examples
========

Example 1: Snowflake Production Profile
----------------------------------------

File: ``~/.config/adbc/profiles/snowflake_prod.toml``

.. code-block:: toml

   version = 1
   driver = "snowflake"

   [options]
   adbc.snowflake.sql.account = "{{ env_var(SNOWFLAKE_ACCOUNT) }}"
   adbc.snowflake.sql.auth_token = "{{ env_var(SNOWFLAKE_TOKEN) }}"
   adbc.snowflake.sql.warehouse = "PRODUCTION_WH"
   adbc.snowflake.sql.database = "PROD_DB"
   adbc.snowflake.sql.schema = "PUBLIC"
   adbc.snowflake.sql.client_session_keep_alive = true
   adbc.snowflake.sql.client_session_keep_alive_heartbeat_frequency = 3600

Usage:

.. code-block:: c

   // Set environment variables
   setenv("SNOWFLAKE_ACCOUNT", "mycompany", 1);
   setenv("SNOWFLAKE_TOKEN", "secret_token", 1);

   // Use profile
   AdbcDatabase database;
   AdbcDatabaseNew(&database, &error);
   AdbcDatabaseSetOption(&database, "uri", "profile://snowflake_prod", &error);
   AdbcDatabaseInit(&database, &error);

Example 2: PostgreSQL Development Profile
------------------------------------------

File: ``~/.config/adbc/profiles/postgres_dev.toml``

.. code-block:: toml

   version = 1
   driver = "postgresql"

   [options]
   uri = "postgresql://localhost:5432/dev_db?sslmode=disable"
   username = "dev_user"
   password = "{{ env_var(POSTGRES_DEV_PASSWORD) }}"

Example 3: Driver-Agnostic Profile
-----------------------------------

Profiles can omit the driver field for reusable configurations:

File: ``~/.config/adbc/profiles/default_timeouts.toml``

.. code-block:: toml

   version = 1
   # No driver specified - can be used with any driver

   [options]
   adbc.connection.timeout = 30.0
   adbc.statement.timeout = 60.0

Usage (driver specified separately):

.. code-block:: c

   AdbcDatabase database;
   AdbcDatabaseNew(&database, &error);
   AdbcDatabaseSetOption(&database, "driver", "adbc_driver_snowflake", &error);
   AdbcDatabaseSetOption(&database, "profile", "default_timeouts", &error);
   AdbcDatabaseInit(&database, &error);

Advanced Usage
==============

Option Precedence
-----------------

Options are applied in the following order (later overrides earlier):

1. Driver defaults
2. Profile options (from ``[options]`` section)
3. Options set via ``AdbcDatabaseSetOption()`` before ``AdbcDatabaseInit()``

Example:

.. code-block:: c

   AdbcDatabase database;
   AdbcDatabaseNew(&database, &error);

   // Profile sets warehouse = "COMPUTE_WH"
   AdbcDatabaseSetOption(&database, "profile", "snowflake_prod", &error);

   // This overrides the profile setting
   AdbcDatabaseSetOption(&database, "adbc.snowflake.sql.warehouse", "ANALYTICS_WH", &error);

   AdbcDatabaseInit(&database, &error);
   // Result: warehouse = "ANALYTICS_WH"

Custom Profile Providers
=========================

Applications can implement custom profile providers to load profiles from alternative sources (databases, key vaults, configuration services, etc.).

Interface Definition
--------------------

A profile provider must implement the ``AdbcConnectionProfile`` interface:

.. code-block:: c

   struct AdbcConnectionProfile {
       void* private_data;
       // this will be called by the driver manager after retrieving the necessary information from the profile.
       void (*release)(struct AdbcConnectionProfile* profile);
       AdbcStatusCode (*GetDriverName)(struct AdbcConnectionProfile* profile,
                                       const char** driver_name,
                                       AdbcDriverInit* init_func,
                                       struct AdbcError* error);
       AdbcStatusCode (*GetOptions)(struct AdbcConnectionProfile* profile,
                                    const char*** keys, const char*** values,
                                    size_t* num_options, struct AdbcError* error);
       AdbcStatusCode (*GetIntOptions)(struct AdbcConnectionProfile* profile,
                                       const char*** keys, const int64_t** values,
                                       size_t* num_options, struct AdbcError* error);
       AdbcStatusCode (*GetDoubleOptions)(struct AdbcConnectionProfile* profile,
                                          const char*** keys, const double** values,
                                          size_t* num_options, struct AdbcError* error);
   };

Provider Function
-----------------

The provider function signature:

.. code-block:: c

   typedef AdbcStatusCode (*AdbcConnectionProfileProvider)(
       const char* profile_name,
       const char* additional_search_path_list,
       struct AdbcConnectionProfile* out,
       struct AdbcError* error);

Example Implementation
----------------------

.. code-block:: c

   // Example: Load profiles from a key-value store
   AdbcStatusCode MyCustomProfileProvider(const char* profile_name,
                                          const char* additional_search_path_list,
                                          struct AdbcConnectionProfile* out,
                                          struct AdbcError* error) {
       // Fetch profile from custom source
       MyProfileData* data = LoadProfileFromKeyVault(profile_name);
       if (!data) {
           SetError(error, "Profile not found in key vault");
           return ADBC_STATUS_NOT_FOUND;
       }

       std::memset(out, 0, sizeof(struct AdbcConnectionProfile));
       // Populate profile structure
       out->private_data = data;
       out->release = MyProfileRelease;
       out->GetDriverName = MyGetDriverName;
       out->GetOptions = MyGetOptions;
       out->GetIntOptions = MyGetIntOptions;
       out->GetDoubleOptions = MyGetDoubleOptions;

       return ADBC_STATUS_OK;
   }

   // Register custom provider
   AdbcDatabase database;
   AdbcDatabaseNew(&database, &error);
   AdbcDriverManagerDatabaseSetProfileProvider(&database, MyCustomProfileProvider, &error);
   AdbcDatabaseSetOption(&database, "profile", "prod_config", &error);
   AdbcDatabaseInit(&database, &error);

Use Cases
=========

Development vs. Production
---------------------------

Maintain separate profiles for different environments:

.. code-block:: bash

   # Development
   export ADBC_PROFILE=snowflake_dev

   # Production
   export ADBC_PROFILE=snowflake_prod

Application code:

.. code-block:: c

   const char* profile = getenv("ADBC_PROFILE");
   if (!profile) profile = "default";

   AdbcDatabaseSetOption(&database, "profile", profile, &error);

Credential Management
---------------------

Store credentials separately from code:

.. code-block:: toml

   [options]
   adbc.snowflake.sql.account = "mycompany"
   adbc.snowflake.sql.auth_token = "env_var(SNOWFLAKE_TOKEN)"

Then set ``SNOWFLAKE_TOKEN`` via environment variable, secrets manager, or configuration service.

Multi-Tenant Applications
--------------------------

Use profiles to support different customer configurations:

.. code-block:: c

   char profile_name[256];
   snprintf(profile_name, sizeof(profile_name), "customer_%s", customer_id);

   AdbcDatabaseSetOption(&database, "profile", profile_name, &error);

Testing
-------

Use profiles to switch between mock and real databases:

.. code-block:: c

   #ifdef TESTING
   const char* profile = "mock_database";
   #else
   const char* profile = "production";
   #endif

   AdbcDatabaseSetOption(&database, "profile", profile, &error);

Error Handling
==============

Profile Not Found
-----------------

If a profile cannot be found, ``AdbcDatabaseInit()`` returns ``ADBC_STATUS_NOT_FOUND`` with a detailed error message listing all searched locations:

.. code-block:: text

   [Driver Manager] Profile not found: my_profile
   Also searched these paths for profiles:
       ADBC_PROFILE_PATH: /custom/path
       user config dir: /home/user/.config/adbc/profiles
       system config dir: /etc/adbc/profiles

Invalid Profile Format
----------------------

If a profile file exists but is malformed, ``AdbcDatabaseInit()`` returns ``ADBC_STATUS_INVALID_ARGUMENT``:

.. code-block:: text

   [Driver Manager] Could not open profile. Error at line 5: expected '=' after key.
   Profile: /home/user/.config/adbc/profiles/my_profile.toml

Missing Driver
--------------

If a profile doesn't specify a driver and none is provided via other means:

.. code-block:: text

   [Driver Manager] Must provide 'driver' parameter
   (or encode driver in 'uri' parameter)

Best Practices
==============

1. **Use environment variables for secrets**: Never store credentials directly in profile files.

   .. code-block:: toml

      # Good
      password = "{{ env_var(DB_PASSWORD) }}"

      # Bad
      password = "my_secret_password"

2. **Organize profiles hierarchically**: Group related profiles in subdirectories using additional search paths.

3. **Document profile schemas**: Maintain documentation of required environment variables for each profile.

4. **Version control without secrets**: Profile files can be version controlled when using ``{{ env_var(VAR_NAME) }}`` for sensitive values.

5. **Test profile loading**: Verify profiles load correctly in CI/CD pipelines.

6. **Use meaningful names**: Name profiles descriptively (e.g., ``snowflake_prod_analytics`` vs. ``profile1``).

7. **Validate environment variables**: Check that required environment variables are set before calling ``AdbcDatabaseInit()``.

API Reference
=============

Setting a Profile Provider
---------------------------

.. code-block:: c

   AdbcStatusCode AdbcDriverManagerDatabaseSetProfileProvider(
       struct AdbcDatabase* database,
       AdbcConnectionProfileProvider provider,
       struct AdbcError* error);

Sets a custom connection profile provider. Must be called before ``AdbcDatabaseInit()``.

**Parameters:**

- ``database``: Database object to configure
- ``provider``: Profile provider function, or ``NULL`` for default filesystem provider
- ``error``: Optional error output

**Returns:** ``ADBC_STATUS_OK`` on success, error code otherwise.

Setting Additional Search Paths
--------------------------------

.. code-block:: c

   AdbcStatusCode AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
       struct AdbcDatabase* database,
       const char* path_list,
       struct AdbcError* error);

Adds additional directories to search for profiles. Must be called before ``AdbcDatabaseInit()``.

**Parameters:**

- ``database``: Database object to configure
- ``path_list``: OS-specific path separator delimited list (``:``) on Unix, ``;`` on Windows), or ``NULL`` to clear
- ``error``: Optional error output

**Returns:** ``ADBC_STATUS_OK`` on success, error code otherwise.

**Example:**

.. code-block:: c

   // Unix/Linux/macOS
   AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
       &database, "/opt/app/profiles:/etc/app/profiles", &error);

   // Windows
   AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
       &database, "C:\\App\\Profiles;C:\\ProgramData\\App\\Profiles", &error);


See Also
========

- :doc:`how_manager` - Driver Manager overview
- :doc:`specification` - Driver specification and options
- :doc:`../cpp/driver_manager` - CPP Driver Manager Reference

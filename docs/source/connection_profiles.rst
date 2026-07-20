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

===================
Connection Profiles
===================

An ADBC connection profile combines a driver name and database options into a reusable, named configuration stored in a TOML file.
This keeps credentials and settings out of your application code.

Here are example profiles for a few databases. Each sets ``driver`` to a driver
name (see :doc:`Drivers <driver/index>`) and puts that driver's options under ``[Options]``.
Secrets are injected from environment variables with ``{{ env_var(NAME) }}``
rather than being written into the file:

.. tab-set::

   .. tab-item:: PostgreSQL

      .. code-block:: toml

         profile_version = 1
         driver = "postgresql"

         [Options]
         uri = "postgresql://postgres:{{ env_var(PGPASSWORD) }}@localhost:5432/demo"

   .. tab-item:: BigQuery

      .. code-block:: toml

         profile_version = 1
         driver = "bigquery"

         [Options]
         "adbc.bigquery.sql.project_id" = "my-gcp-project"
         "adbc.bigquery.sql.dataset_id" = "bigquery-public-data"

   .. tab-item:: Redshift

      .. code-block:: toml

         profile_version = 1
         driver = "redshift"

         [Options]
         uri = "postgresql://localhost:5439"
         "redshift.cluster_type" = "redshift-serverless"
         "redshift.workgroup_name" = "my-workgroup"
         "redshift.db_name" = "sample_data_dev"

   .. tab-item:: Snowflake

      .. code-block:: toml

         profile_version = 1
         driver = "snowflake"

         [Options]
         username = "MYUSER"
         "adbc.snowflake.sql.auth_type" = "auth_jwt"
         "adbc.snowflake.sql.client_option.jwt_private_key" = "/path/to/rsa_key.p8"
         "adbc.snowflake.sql.account" = "ACCOUNT-IDENT"
         "adbc.snowflake.sql.db" = "SNOWFLAKE_SAMPLE_DATA"
         "adbc.snowflake.sql.schema" = "TPCH_SF1"
         "adbc.snowflake.sql.warehouse" = "MY_WAREHOUSE"
         "adbc.snowflake.sql.role" = "MY_ROLE"

   .. tab-item:: StarRocks

      .. code-block:: toml

         profile_version = 1
         driver = "flightsql"

         [Options]
         uri = "grpc://localhost:9408"
         username = "root"
         password = "{{ env_var(STARROCKS_PASSWORD) }}"

Save the profile as a ``.toml`` file (for example ``myprofile.toml``) in the
user configuration directory, where the client library or driver manager
looks for it by name:

- **Linux**: ``~/.config/adbc/profiles/``
- **macOS**: ``~/Library/Application Support/ADBC/Profiles/``
- **Windows**: ``%LOCALAPPDATA%\ADBC\Profiles\``

Other locations are also searched; see
:ref:`Profile Search Locations <profile-search-locations>` for the full list
and precedence.

Your application then references the profile at connection time, and the client
library or driver manager loads it automatically. Point the ``uri`` option at
the profile name—the TOML file's name without the ``.toml`` extension—using
the ``profile://`` scheme:

.. tab-set::

   .. tab-item:: C/C++

      .. code-block:: cpp

         AdbcDatabase database = {};
         AdbcDatabaseNew(&database, &error);
         AdbcDatabaseSetOption(&database, "uri", "profile://myprofile", &error);
         AdbcDriverManagerDatabaseSetLoadFlags(&database, ADBC_LOAD_FLAG_DEFAULT, &error);
         AdbcDatabaseInit(&database, &error);

   .. tab-item:: Go

      .. code-block:: go

         var drv drivermgr.Driver

         db, err := drv.NewDatabase(map[string]string{
             "uri": "profile://myprofile",
         })

   .. tab-item:: Java

      .. code-block:: java

         Map<String, Object> params = new HashMap<>();
         JniDriver.PARAM_URI.set(params, "profile://myprofile");

         AdbcDatabase db =
             AdbcDriverManager.getInstance().connect(DRIVER_FACTORY, allocator, params);

   .. tab-item:: JavaScript

      .. code-block:: javascript

         import { AdbcDatabase } from '@apache-arrow/adbc-driver-manager';

         const db = new AdbcDatabase({
           databaseOptions: { uri: 'profile://myprofile' },
         });

   .. tab-item:: Python

      .. code-block:: python

         from adbc_driver_manager import dbapi

         with dbapi.connect(uri="profile://myprofile") as con:
             ...

   .. tab-item:: R

      .. code-block:: r

         library(adbcdrivermanager)

         db <- adbc_database_init(uri = "profile://myprofile")
         con <- adbc_connection_init(db)

   .. tab-item:: Ruby

      .. code-block:: ruby

         database = ADBC::Database.new
         database.set_option("uri", "profile://myprofile")
         database.set_load_flags(ADBC::LoadFlags::DEFAULT)
         database.init

   .. tab-item:: Rust

      .. code-block:: rust

         let uri = "profile://myprofile";
         let db = ManagedDatabase::from_uri(
             uri, None, AdbcVersion::default(), LOAD_FLAG_DEFAULT, None,
         )?;

You can also pass an absolute path to a profile file instead of a name.
For full details on the profile file format, search paths, option precedence, and how to implement a custom profile provider, see :doc:`format/connection_profiles`.

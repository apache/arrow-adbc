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

A connection profile combines a driver name and database options into a reusable, named configuration stored in a TOML file.
Instead of specifying credentials and settings in application code, you reference a profile by name and the :doc:`ADBC Driver Manager <format/how_manager>` loads it automatically at connection time.

.. code-block:: c

   AdbcDatabase database;
   AdbcDatabaseNew(&database, &error);
   AdbcDatabaseSetOption(&database, "uri", "profile://my_postgres_dev", &error);
   AdbcDatabaseInit(&database, &error);

Profiles are looked up by name from a set of standard filesystem locations, or you can provide an absolute path.
Sensitive values like passwords or tokens can be injected from environment variables using ``{{ env_var(VAR_NAME) }}`` substitution rather than hardcoding them in the file.

For full details on the profile file format, search paths, option precedence, and how to implement a custom profile provider, see :doc:`format/connection_profiles`.

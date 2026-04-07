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

==================
PostgreSQL Recipes
==================

Authenticate with a username and password
=========================================

.. recipe:: postgresql_authenticate.py

Enable autocommit mode
=======================

.. recipe:: postgresql_autocommit.py

.. _recipe-postgresql-create-append:

Create/append to a table from an Arrow dataset
==============================================

.. recipe:: postgresql_create_dataset_table.py

Create/append to a table from an Arrow table
============================================

.. recipe:: postgresql_create_append_table.py

Create/append to a temporary table
==================================

.. recipe:: postgresql_create_temp_table.py

Execute a statement with bind parameters
========================================

.. recipe:: postgresql_execute_bind.py

.. _recipe-postgresql-statement-nocopy:

Execute a statement without COPY
================================

.. recipe:: postgresql_execute_nocopy.py

Get the Arrow schema of a table
===============================

.. recipe:: postgresql_get_table_schema.py

Get the Arrow schema of a query
===============================

.. recipe:: postgresql_get_query_schema.py

List catalogs, schemas, and tables
==================================

.. recipe:: postgresql_list_catalogs.py

Connection pooling with SQLAlchemy
==================================

.. recipe:: postgresql_pool.py

Using Pandas and ADBC
=====================

.. recipe:: postgresql_pandas.py

Using Polars and ADBC
=====================

.. recipe:: postgresql_polars.py

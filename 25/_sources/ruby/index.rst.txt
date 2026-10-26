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

====
Ruby
====

**red-adbc** is the Ruby binding for ADBC, built on top of ADBC GLib.
It is distributed as the `red-adbc gem on RubyGems
<https://rubygems.org/gems/red-adbc>`__.

Here we'll briefly tour basic features of ADBC with Ruby using the SQLite driver.

Installation
============

``red-adbc`` depends on the native Arrow GLib and ADBC GLib libraries. The
`rubygems-requirements-system
<https://rubygems.org/gems/rubygems-requirements-system>`__ plugin installs
these system dependencies automatically using your system package manager, so
install it first.

With ``gem``:

.. code-block:: shell

   gem install rubygems-requirements-system
   gem install red-adbc

Or, with Bundler, add the plugin and the gem to your ``Gemfile``:

.. code-block:: ruby

   source "https://rubygems.org"

   plugin "rubygems-requirements-system"

   gem "red-adbc"

Installing Drivers
------------------

You also need a driver for the database you want to connect to. See
:ref:`driver-table-install` for instructions. For the example below, you could
install `dbc <https://docs.columnar.tech/dbc>`__ and then install the SQLite
driver with:

.. code-block:: shell

   dbc install sqlite

Creating a Connection
=====================

.. code-block:: ruby

   require "adbc"

   ADBC::Database.open(driver: "sqlite",
                       uri: ":memory") do |database|
     database.connect do |connection|
       # Use connection
     end
   end

In application code, the database and connection must be closed after usage or
memory may leak. Both ``Database.open`` and ``database.connect`` accept blocks
to accomplish this automatically.

Executing a Query
=================

We can execute a query and get the results as an Arrow table:

.. code-block:: ruby

   require "adbc"

   ADBC::Database.open(driver: "sqlite",
                       uri: ":memory:") do |database|
     database.connect do |connection|
       table, = connection.query("SELECT 1, 2.0, 'Hello, world!'")
       puts table
     end
   end

Output:

.. code-block:: text

      1  2.0  'Hello, world!'
      (int64)  (double)  (utf8)
   0  1  2.0  Hello, world!

Ingesting Bulk Data
===================

We can insert a table of Arrow data into a new database table:

.. code-block:: ruby

   require "adbc"

   ADBC::Database.open(driver: "sqlite",
                       uri: ":memory:") do |database|
     database.connect do |connection|
       # Create an Arrow table
       table = Arrow::Table.new(
         ints: Arrow::Int64Array.new([1, 2]),
         strs: Arrow::StringArray.new(["a", nil])
       )

       # Ingest into database
       connection.ingest("sample", table, mode: :create)

       # Query the data
       result, = connection.query("SELECT COUNT(DISTINCT ints) FROM sample")
       puts result
     end
   end

We can also append to an existing table:

.. code-block:: ruby

   require "adbc"

   ADBC::Database.open(driver: "sqlite",
                       uri: ":memory:") do |database|
     database.connect do |connection|
       # Create initial table
       table = Arrow::Table.new(
         ints: Arrow::Int64Array.new([1, 2]),
         strs: Arrow::StringArray.new(["a", nil])
       )
       connection.ingest("sample", table, mode: :create)

       # Append more data
       table2 = Arrow::Table.new(
         ints: Arrow::Int64Array.new([2, 3]),
         strs: Arrow::StringArray.new([nil, "c"])
       )
       connection.ingest("sample", table2, mode: :append)

       # Query the combined data
       result, = connection.query("SELECT COUNT(DISTINCT ints) FROM sample")
       puts result
     end
   end

The ``mode`` parameter can be:

- ``:create`` - Create a new table (default)
- ``:append`` - Append to an existing table
- ``:replace`` - Replace an existing table

Getting Database/Driver Metadata
=================================

We can get information about the driver and the database:

.. code-block:: ruby

   require "adbc"

   ADBC::Database.open(driver: "sqlite",
                       uri: ":memory:") do |database|
     database.connect do |connection|
       info = connection.info
       puts "Vendor: #{info[:vendor_name]}"
       puts "Driver: #{info[:driver_name]}"
     end
   end

We can also query for tables and columns in the database. This gives
a nested structure describing all the catalogs, schemas, tables, and
columns:

.. code-block:: ruby

   require "adbc"

   ADBC::Database.open(driver: "sqlite",
                       uri: ":memory:") do |database|
     database.connect do |connection|
       # Create a sample table first
       connection.query("CREATE TEMPORARY TABLE sample (ints INTEGER, strs TEXT)")

       # Get database objects
       objects = connection.get_objects(depth: :all)
       catalog = objects.raw_records[1]
       schema = catalog[1][0]
       tables = schema["db_schema_tables"]

       puts "Table: #{tables[0]["table_name"]}"
       columns = tables[0]["table_columns"]
       puts "Columns: #{columns.map { |c| c["column_name"] }.join(", ")}"
     end
   end

Next Steps
==========

- See the :doc:`drivers </driver/index>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/ruby>`_
- Explore the `Ruby source code <https://github.com/apache/arrow-adbc/tree/main/ruby>`_ for additional examples

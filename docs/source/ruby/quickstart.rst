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

==========
Quickstart
==========

Here we'll briefly tour basic features of ADBC with Ruby using the PostgreSQL driver.

Installation
============

.. code-block:: shell

   bundle add adbc

Prerequisites
-------------

The Ruby ADBC library requires the native Arrow GLib and ADBC GLib libraries. Install them using your package manager:

**macOS with Homebrew:**

.. code-block:: shell

   brew install apache-arrow-glib apache-arrow-adbc-glib

**Debian/Ubuntu:**

.. code-block:: shell

   sudo apt install libarrow-glib-dev libadbc-glib-dev

**RHEL-compatible distributions:**

.. code-block:: shell

   sudo dnf install arrow-glib-devel adbc-glib-devel

Basic Example
=============

This example demonstrates connecting to PostgreSQL, executing a query, and reading results.

.. code-block:: ruby

   require "adbc"

   database = ADBC::Database.new

   begin
     database.set_option("driver", "postgresql")
     database.set_option("uri", "postgresql://user:password@localhost:5432/database")
     database.set_load_flags(ADBC::LoadFlags::DEFAULT)
     database.init

     database.connect do |connection|
       table, = connection.query("SELECT * FROM my_table;")
       puts table
     end
   ensure
     database.release
   end

Working with Results
====================

ADBC returns results as Arrow tables, which you can process using the Arrow Ruby library:

.. code-block:: ruby

   require "adbc"

   database = ADBC::Database.new

   begin
     database.set_option("driver", "postgresql")
     database.set_option("uri", "postgresql://user:password@localhost:5432/database")
     database.init

     database.connect do |connection|
       table, = connection.query("SELECT * FROM my_table;")

       # Access columns
       puts "Columns: #{table.schema.fields.map(&:name)}"

       # Iterate over rows
       table.each_record_batch do |batch|
         batch.each do |row|
           puts row
         end
       end
     end
   ensure
     database.release
   end

Installing Drivers
==================

You'll need to install an ADBC driver for the database you want to connect to. The easiest way is using `dbc <https://docs.columnar.tech/dbc>`_:

.. code-block:: shell

   # Install dbc
   curl -fsSL https://dbc.sh | sh

   # Install a driver (e.g., PostgreSQL)
   dbc install postgresql

You can also build drivers from source or use other installation methods. See the :doc:`driver documentation </driver/index>` for more details.

Next Steps
==========

- Check out the :doc:`Ruby API documentation <index>` for more details
- See the :doc:`driver status </driver/status>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/ruby>`_
- Explore the `Ruby source code <https://github.com/apache/arrow-adbc/tree/main/ruby>`_ for additional examples

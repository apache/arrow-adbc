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

Red ADBC is the Ruby binding for ADBC, built on top of ADBC GLib.
It is distributed as the ``red-adbc`` gem on `RubyGems <https://rubygems.org/gems/red-adbc>`__.

Installation
============

Install the ``rubygems-requirements-system`` plugin first, then ``red-adbc``:

.. code-block:: console

   $ gem install rubygems-requirements-system
   $ gem install red-adbc

Or add it to your ``Gemfile``:

.. code-block:: ruby

   plugin "rubygems-requirements-system"

   gem "red-adbc"

Usage
=====

.. code-block:: ruby

   require "adbc"

   ADBC::Database.open(driver: "adbc_driver_sqlite",
                       uri: ":memory:") do |database|
     database.connect do |connection|
       puts(connection.query("SELECT 1"))
     end
   end

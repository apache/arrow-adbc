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
Static Linking
==============

To statically link and use *multiple* drivers, the following limitations hold:

#. You must build with these CMake options [#meson]_:

   - ``-DADBC_BUILD_STATIC=ON``
   - ``-DADBC_DEFINE_COMMON_ENTRYPOINTS=OFF``
   - ``-DADBC_WITH_VENDORED_FMT=OFF``
   - ``-DADBC_WITH_VENDORED_NANOARROW=OFF``

#. You must provide ``fmt`` and ``nanoarrow`` dependencies.  (This may be
   relaxed in the future.)
#. You must explicitly link all the required transitive dependencies.  This
   is:

   - ``libpq`` for PostgreSQL
   - ``sqlite3`` for SQLite
   - ``adbc_driver_common``, ``adbc_driver_framework``, ``fmt``,
     ``nanoarrow``, and the C++ standard library for either PostgreSQL or
     SQLite.
   - To link the C++ standard library, the easiest thing is to just use the
     C++ linker, even if the code itself is in C, e.g.

     .. code-block:: cmake

        set_target_properties(myapp PROPERTIES LINKER_LANGUAGE CXX)

   - Go-based drivers (BigQuery, Flight SQL, Snowflake) have no transitive
     dependencies.

#. You cannot link more than a single Go-based driver.  See `golang/go#20639
   <go20639_>`_, `StackOverflow #67243572 <so67243572_>`_, and `StackOverflow
   #34333107 <so34333107_>`_ for a discussion of the issues involved.  (This
   may be relaxed in the future by providing a way to build a single driver
   library with all driver implementations included.)

An example of this can be seen with ``cpp_static_test.sh`` and
``c/integration/static_test`` in the source tree.

.. [#meson] There is currently no documentation for Meson, but in principle
            it should work similarly.
.. _go20639: https://github.com/golang/go/issues/20639
.. _so67243572: https://stackoverflow.com/questions/67243572/go-cgo-produce-static-library-without-definitions-of-go-runtime-functions
.. _so34333107: https://stackoverflow.com/questions/34333107/is-there-a-way-to-include-multiple-c-archive-packages-in-a-single-binary

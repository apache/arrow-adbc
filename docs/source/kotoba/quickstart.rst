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

Install Kotoba 0.7.2, then compile a ``.kotoba`` entry point to wasm:

.. code-block:: shell

   kotoba compile kotoba/adbc/status.kotoba --target wasm -o status.wasm

The mock connection fixture compiles the library graph and exports ``main``,
which returns ``0`` when connect/close/query status codes match ``adbc.h``:

.. code-block:: shell

   kotoba compile kotoba/fixtures/mock_connection.kotoba \
     --source-path kotoba --unpinned --target wasm -o mock.wasm

From the repository root you can compile every module and run the fixtures:

.. code-block:: shell

   ./ci/scripts/kotoba_test.sh "$(pwd)"

Status codes are the integer values from ``adbc.h``. An error is packed as
``(status * 100000) + vendor_code``. SQLSTATE and message strings stay on
the host.

See the `Kotoba source tree <https://github.com/apache/arrow-adbc/tree/main/kotoba>`_
for the modules and fixtures.

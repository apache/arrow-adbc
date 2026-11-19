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

======
Kotoba
======

The ADBC Kotoba binding is an in-language guest API compiled to WebAssembly
with `Kotoba <https://kotoba-lang.org>`_ 0.7.2.

Kotoba programs cannot FFI ``libadbc``. v1 therefore ships only ADBC status
and error codes plus a mock connection fixture. There is no real database.
The host owns drivers and maps a URI to an integer kind before calling into
the wasm module.

.. toctree::
   :maxdepth: 2

   quickstart

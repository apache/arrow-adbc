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

================
Nightly Packages
================

Nightly builds of some binary packages are available.

.. warning:: These packages are not official releases. These packages
             are not intended for normal usage, only for project
             developers.

C/C++
=====

Conda users can install nightly builds from a Conda channel:
https://anaconda.org/arrow-adbc-nightlies

This should be used with conda-forge.  Example::

  mamba install -c conda-forge -c arrow-adbc-nightlies adbc-driver-manager

Both C/C++ and Python packages are available.

.. list-table:: Supported platforms for nightly Conda packages
   :header-rows: 1

   * - Operating System
     - AMD64 (x86_64)
     - AArch64

   * - Linux
     - Yes ✅
     - No ❌

   * - macOS
     - Yes ✅
     - No ❌

   * - Windows
     - No ✅
     - No ❌

Java
====

Not yet available.

Python
======

Packages can be installed from an alternative package index:
https://gemfury.com/arrow-adbc-nightlies

Example::

  pip install \
        --pre \
        --extra-index-url https://repo.fury.io/arrow-adbc-nightlies \
        adbc-driver-manager

.. list-table:: Supported platforms for nightly Python wheels
   :header-rows: 1

   * - Operating System
     - AMD64 (x86_64)
     - AArch64

   * - Linux
     - Yes ✅
     - Yes ✅

   * - macOS
     - Yes ✅
     - Yes ✅

   * - Windows
     - Yes ✅
     - No ❌

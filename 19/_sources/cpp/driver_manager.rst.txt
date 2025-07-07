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
Driver Manager
==============

The driver manager is a library that implements the ADBC API by
delegating to dynamically-loaded drivers.  This allows applications to
use multiple drivers simultaneously, and decouple themselves from the
specific driver.

Installation
============

Install the appropriate driver package. You can use conda-forge_, ``apt`` or ``dnf``.

conda-forge:

- ``mamba install adbc-driver-manager``

You can use ``apt`` on the following platforms:

- Debian GNU/Linux bookworm
- Ubuntu 22.04

Prepare the Apache Arrow APT repository:

.. code-block:: bash

   sudo apt update
   sudo apt install -y -V ca-certificates lsb-release wget
   sudo wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   sudo apt update

Install:

- ``sudo apt install libadbc-driver-manager-dev``

You can use ``dnf`` on the following platforms:

- AlmaLinux 8
- Oracle Linux 8
- Red Hat Enterprise Linux 8
- AlmaLinux 9
- Oracle Linux 9
- Red Hat Enterprise Linux 9

Prepare the Apache Arrow Yum repository:

.. code-block:: bash

   sudo dnf install -y epel-release || sudo dnf install -y oracle-epel-release-el$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1) || sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1).noarch.rpm
   sudo dnf install -y https://apache.jfrog.io/artifactory/arrow/almalinux/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm
   sudo dnf config-manager --set-enabled epel || :
   sudo dnf config-manager --set-enabled powertools || :
   sudo dnf config-manager --set-enabled crb || :
   sudo dnf config-manager --set-enabled ol$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)_codeready_builder || :
   sudo dnf config-manager --set-enabled codeready-builder-for-rhel-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)-rhui-rpms || :
   sudo subscription-manager repos --enable codeready-builder-for-rhel-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)-$(arch)-rpms || :

Install:

- ``sudo dnf install adbc-driver-manager-devel``

Then they can be used via CMake, e.g.:

.. code-block:: cmake

   find_package(AdbcDriverPostgreSQL)

   # ...

   target_link_libraries(myapp PRIVATE AdbcDriverPostgreSQL::adbc_driver_postgresql_shared)

.. _conda-forge: https://conda-forge.org/

Usage
=====

To create a database, use the :c:struct:`AdbcDatabase` API as usual, but
during initialization, provide two additional parameters in addition to the
driver-specific connection parameters: ``driver`` and (optionally)
``entrypoint``.  ``driver`` must be the name of a library to load, or the path
to a library to load. ``entrypoint``, if provided, should be the name of the
symbol that serves as the ADBC entrypoint (see :c:type:`AdbcDriverInitFunc`).

.. code-block:: c

   /* Ignoring error handling */
   struct AdbcDatabase database;
   memset(&database, 0, sizeof(database));
   AdbcDatabaseNew(&database, NULL);
   /* On Linux: loads libadbc_driver_sqlite.so
    * On MacOS: loads libadbc_driver_sqlite.dylib
    * On Windows: loads adbc_driver_sqlite.dll */
   AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", NULL);
   /* Set additional options for the specific driver, if needed */
   /* Initialize the database */
   AdbcDatabaseInit(&database, NULL);
   /* Create connections as usual */

API Reference
=============

The driver manager includes a few additional functions beyond the ADBC API.
See the API reference: :external+cpp_adbc:doc:`adbc_driver_manager.h`.

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

============
Installation
============

.. We can't link to things like conda-forge or crates.io because we
   technically do not vote on those packages as part of the release; they are
   created post-release. (Python wheels are voted on, though.) If we want to
   fix this, we would have to do something like provide prereleases on these
   platforms that we promote post-vote.

   The main Arrow project links to unofficial releases with a disclaimer, but
   as far as I can tell, this isn't actually allowed. Given our announce
   emails are getting rejected, let's not risk it.

   https://apache.org/legal/release-policy#publication
   > Your project's download page can only link to release artifacts that your PMC has approved.

.. note::

   See individual driver pages in the sidebar for specific installation instructions.

Source
======

Download the latest source release: |source_download| (|source_checksum|, |source_signature|)

When downloading, please check the SHA512 checksum, and verify the OpenPGP-compatible signature against the project's `KEYS`_ file.  Both the checksum and signature are provided above.  See the `instructions`_ page for more details.

Compilation instructions can be found in `CONTRIBUTING.md`_.

.. _CONTRIBUTING.md: https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md
.. _instructions: https://www.apache.org/info/verification.html
.. _KEYS: https://downloads.apache.org/arrow/KEYS

C/C++
=====

Install the appropriate driver package.  Packages for ``apt`` and ``dnf`` are provided as part of official releases.

You can use ``apt`` on the following platforms:

- Debian GNU/Linux 12 ("bookworm")
- Debian GNU/Linux 13 ("trixie")
- Ubuntu 22.04 LTS ("jammy")
- Ubuntu 24.04 LTS ("noble")

Prepare the Apache Arrow APT repository:

.. code-block:: bash

   sudo apt update
   sudo apt install -y -V ca-certificates lsb-release wget
   sudo wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   sudo apt update

Install:

- ``sudo apt install libadbc-driver-flightsql-dev``
- ``sudo apt install libadbc-driver-postgresql-dev``
- ``sudo apt install libadbc-driver-sqlite-dev``
- ``sudo apt install libadbc-driver-snowflake-dev``

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

- ``sudo dnf install adbc-driver-flightsql-devel``
- ``sudo dnf install adbc-driver-postgresql-devel``
- ``sudo dnf install adbc-driver-sqlite-devel``
- ``sudo dnf install adbc-driver-snowflake-devel``

Then they can be used via CMake, e.g.:

.. code-block:: cmake

   find_package(AdbcDriverPostgreSQL)

   # ...

   target_link_libraries(myapp PRIVATE AdbcDriverPostgreSQL::adbc_driver_postgresql_shared)

Java
====

Add a dependency on the driver package, for example:

- ``org.apache.arrow.adbc:adbc-driver-flight-sql``
- ``org.apache.arrow.adbc:adbc-driver-jdbc``

Python
======

Install the appropriate driver package.

.. note:: To use the DBAPI interface, either ``pyarrow`` or ``polars`` must be installed.

For example, from PyPI:

- ``pip install adbc-driver-flightsql``
- ``pip install adbc-driver-postgresql``
- ``pip install adbc-driver-snowflake``
- ``pip install adbc-driver-sqlite``

R
===

Install the appropriate driver package from CRAN:

.. code-block:: r

   install.packages("adbcsqlite")
   install.packages("adbcpostgresql")
   install.packages("duckdb")

Drivers not yet available on CRAN can be installed from R-multiverse:

.. code-block:: r

   install.packages("adbcflightsql", repos = "https://community.r-multiverse.org")
   install.packages("adbcsnowflake", repos = "https://community.r-multiverse.org")

Ruby
====

Install the appropriate driver package for C/C++. You can use it from
Ruby.

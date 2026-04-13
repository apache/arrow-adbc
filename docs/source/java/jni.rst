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
JNI Driver Manager
==================

The JNI driver manager wraps the :doc:`C/C++ driver manager
<../cpp/driver_manager>`, making it possible to use drivers written in C/C++,
Go, Rust, and more from Java. As the name implies, this requires JNI.

Installation
============

To include the JNI driver manager in your Maven project, add the following dependency:

.. code-block:: xml

   <dependency>
      <groupId>org.apache.arrow.adbc</groupId>
      <artifactId>adbc-driver-jni</artifactId>
      <version>${adbc.version}</version>
   </dependency>

Usage
=====

The JNI driver manager is implemented as a "driver"; parameters for opening an
:external+java_adbc:jtype:`AdbcDatabase
<org.apache.arrow.adbc.core.AdbcDatabase>` are passed to the C/C++ driver
manager, and the "real" driver is loaded. Once loaded, the Java ADBC APIs are
forwarded to the equivalent C APIs, so there is no difference from other
drivers in Java, and the resulting object can be used the same way as drivers
written in Java or another JVM language.

Drivers can be directly loaded via the
:external+java_adbc:jmember:`PARAM_DRIVER
<org.apache.arrow.adbc.driver.jni.JniDriver#PARAM_PROFILE>` option. This takes
a path to a driver, a :doc:`driver manifest <../format/driver_manifests>`, or
a name that will be passed to ``dlopen`` (or equivalent):

.. code-block:: java

   JniDriver driver = new JniDriver(allocator);
   Map<String, Object> parameters = new HashMap<>();
   // path to driver:
   JniDriver.PARAM_DRIVER.set(parameters, "/path/to/libadbc_driver_sqlite.so");

   // manifest:
   JniDriver.PARAM_DRIVER.set(parameters, "sqlite");

   // library name:
   JniDriver.PARAM_DRIVER.set(parameters, "adbc_driver_sqlite");

   AdbcDatabase db = driver.open(parameters);

Another option is to pass only a URI, in which case the URI scheme will be
used to find and load the driver or driver manifest:

.. code-block:: java

   JniDriver driver = new JniDriver(allocator);
   Map<String, Object> parameters = new HashMap<>();
   // Expects a profile named "postgresql" to exist (or libpostgresql.so, etc.)
   AdbcDriver.PARAM_URI.set(parameters, "postgresql://localhost:5432/dev");
   AdbcDatabase db = driver.open(parameters);

To use :doc:`connection profiles <../format/connection_profiles>`, pass the
:external+java_adbc:jmember:`PARAM_PROFILE
<org.apache.arrow.adbc.driver.jni.JniDriver#PARAM_PROFILE>` option:

.. code-block:: java

   JniDriver driver = new JniDriver(allocator);
   Map<String, Object> parameters = new HashMap<>();
   JniDriver.PARAM_PROFILE.set(parameters, "staging_aws");
   AdbcDatabase db = driver.open(parameters);

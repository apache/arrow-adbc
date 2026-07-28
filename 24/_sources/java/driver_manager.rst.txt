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

This document describes the Java :term:`driver manager`.

.. note:: This driver manager can only load drivers developed in Java (or
          other JVM languages). The :doc:`./jni` should be used to load
          drivers from other ecosystems (including drivers developed in C/C++,
          Go, and Rust). Currently, far more drivers are available though JNI
          than are available in pure-Java.

Installation
============

To include the ADBC Driver Manager in your Maven project, add the following dependency:

.. code-block:: xml

   <dependency>
      <groupId>org.apache.arrow.adbc</groupId>
      <artifactId>adbc-driver-manager</artifactId>
      <version>${adbc.version}</version>
   </dependency>

API Reference
=============

See the `API reference <./api/org/apache/arrow/adbc/drivermanager/package-summary.html>`_.

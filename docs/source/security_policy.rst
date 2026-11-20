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

===============
Security Policy
===============

This document outlines the security model for ADBC libraries and drivers and how to report vulnerabilities.

Security Model
==============

The ADBC project follows the `Apache Arrow Security Model <arrow_security_model_>`_ and builds on it.

In general, crashes, hangs, and resource exhaustion are considered bugs, not security vulnerabilities, unless they can be exploited to execute arbitrary code, escalate privileges, or disclose data the attacker should not have access to (such as the contents of process memory).

.. _arrow_security_model: https://arrow.apache.org/docs/format/Security.html

Libraries
---------

This section covers all language bindings and driver managers that live in this repository.

- Applications should only load trusted drivers with a driver manager.
  Issues resulting from loading a driver that behaves maliciously are not covered by this policy.
- Applications should only load trusted driver manifests or connection profiles with a driver manager.
  Issues resulting from loading untrusted manifests or profiles are not covered by this policy.

Drivers
-------

This section covers the ADBC drivers that live in and are released from this repository.
It does not cover ADBC drivers published outside the ADBC project.

- Drivers should only be used to connect to trusted databases and data sources.
- Drivers trust the responses they receive, so issues that require a malicious or compromised database or data source (for example, a server that sends malformed responses that crash the driver, corrupt memory, or cause the driver to produce invalid Arrow data) are not considered vulnerabilities in the driver.

Arrow Data
----------

The Arrow security model requires consumers of Arrow data to trust its producer.
Applications consume Arrow data from drivers and, since applications must trust drivers they choose to load, drivers are trusted producers.
Likewise, drivers trust Arrow data passed to them by the application.
Passing invalid data to a driver is API misuse, not a vulnerability.

Reporting a Bug
===============

To report a bug that is not a security vulnerability, see the :ref:`contributing guide <contributing>`.

Reporting a Security Vulnerability
==================================

For security vulnerabilities, please follow the responsible disclosure process below so we can investigate and fix the issue before it is exploited in the wild.

ADBC follows the Apache Software Foundation `security reporting process <asf_process_>`_, and reports of security vulnerabilities should be sent privately to `security@apache.org <mailto:security@apache.org>`_.

.. _asf_process: https://www.apache.org/security/#reporting-a-vulnerability

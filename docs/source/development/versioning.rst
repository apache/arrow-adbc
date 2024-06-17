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
Versioning
==========

ADBC has multiple subcomponents with independent version numbers all on the
same release cycle.

When a new release is made, the overall release is assigned a release number
(e.g. ADBC Release 12).  The various components, like the C# packages, Java
packages, C/C++/Go packages, and so on, are tagged individually with their own
version numbers, which will show up in package indices like PyPI or Maven
Central.  Common resources like documentation, verification script, and the
GitHub release use the release number.

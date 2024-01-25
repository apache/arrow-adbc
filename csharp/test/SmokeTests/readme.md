<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Smoke Tests
The smoke tests are designed to work with the NuGet packages that are generated from the `dotnet pack` command. These are run automatically as part of the `Build-All.ps1` script.

## Smoke Test Structure
Smoke tests are similar to the unit tests, and actually leverage the code from unit tests, but instead target using the NuGet packages (instead of the project references) to validate they function correctly.

Since these are smoke tests, they run a scaled down version of what the unit tests run. To do this, each `*.Tests` project has an equivalent `*.SmokeTests` project. For example, there is both `Apache.Arrow.Adbc.Tests` and `Apache.Arrow.Adbc.SmokeTests`. The smoke tests use linking to add items from the `*.Test` project and make them appear to be included, but they are actually located in the original project.

The smoke tests only run the ClientTests because those exercise multiple NuGet packages and the underlying driver.

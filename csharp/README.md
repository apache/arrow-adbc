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

# Apache Arrow ADBC

An implementation of Arrow ADBC targeting .NET Standard 2.0 and .NET 6 or later.

## Telemetry dependencies

The core and trace-listener packages retain a minimum dependency on
`System.Diagnostics.DiagnosticSource` 9.0.6. The published Testing helper uses
OpenTelemetry 1.15.3 or later and overrides its DiagnosticSource minimum to
10.0.0. This avoids OpenTelemetry.Api versions affected
by [CVE-2026-40894](https://github.com/advisories/GHSA-g94r-2vxg-569j) without
raising the core driver's shared-contract minimum.

Hosts that load managed drivers into the default assembly load context must
provide a single DiagnosticSource assembly satisfying all loaded components.
Consumers of `Apache.Arrow.Adbc.Testing` must update to a release containing
these dependency requirements; updating the core driver alone does not update
the Testing helper.

<!--

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# Traces Exporters

## FileExporter

Provides an OpenTelemetry (OTel) exporter to write telemetry traces to
rotating files in folder. File names are created with the following pattern:
`<trace-source>-<YYYY-MM-DD-HH-mm-ss-fff>-<process-id>.log`.
For example: `apache.arrow.adbc.drivers.databricks-2025-08-15-10-35-56-012345-99999.log`.
The default folder used is:

| Platform | Folder |
| --- | --- |
| Windows | `%LOCALAPPDATA%/Apache.Arrow.Adbc/Traces` |
| macOS   | `$HOME/Library/Application Support/Apache.Arrow.Adbc/Traces` |
| Linux   | `$HOME/.local/share/Apache.Arrow.Adbc/Traces` |

By default, up to 100 files of maximum size 1024 KB are written to
the trace folder.

## ExportersBuilder

Helps activate one of a dictionary of supported exporters.

The environment variable `OTEL_TRACES_EXPORTER` can be used to select one of the
available exporters. Or the database parameter `adbc.traces.exporter` can be used,
which has precedence over the environment variable.

The following exporters are supported:

| Exporter | Description |
| --- | --- |
| `otlp` | Exports traces to an OpenTelemetry Collector or directly to an Open Telemetry Line Protocol (OTLP) endpoint. |
| `file` | Exports traces to rotating files in a folder. |
| `console` | Exports traces to the console output. |
| `none` | Disables trace exporting. |

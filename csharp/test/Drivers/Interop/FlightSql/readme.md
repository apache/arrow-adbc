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

# Flight SQL
The Flight SQL tests leverage the interop nature of the C# ADBC library. These require the use of the [Flight SQL Go driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/flightsql). You will need to compile the Go driver for your platform and place the driver in the correct path in order for the tests to execute correctly.

To compile, navigate to the `go/adbc/pkg` directory of the cloned [arrow-adbc](https://github.com/apache/arrow-adbc) repository then run the `make` command.  If you encounter compilation errors, please ensure that Go, [GCC and C++](https://code.visualstudio.com/docs/cpp/config-mingw) tools are installed. And following [Contributing to ADBC](https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md#environment-setup).

## Setup
The environment variable `FLIGHTSQL_INTEROP_TEST_CONFIG_FILE` must be set to a configuration JSON file for the tests to execute. If it is not, the tests will show as passed with an output message that they are skipped. A template configuration file can be found in the Resources directory.

## Configuration
A growing number of data sources support Arrow Flight SQL. This library has tests that run against:

- [Denodo](https://community.denodo.com/docs/html/browse/9.1/en/vdp/developer/access_through_flight_sql/connection_using_flight_sql/connection_using_flight_sql)
- [Dremio](https://docs.dremio.com/current/sonar/developing-client-apps/arrow-flight-sql/)
- [DuckDB](https://github.com/voltrondata/SQLFlite)
- [SQLite](https://github.com/voltrondata/SQLFlite)
- [Spice.ai OSS](https://github.com/spiceai/spiceai)

It is recommended you test your data source with the Flight SQL Go driver to ensure compatibility, since each data source can implement the Flight protocol slightly differently.

A sample configuration file is provided in the Resources directory. The configuration file is a JSON file that contains the following fields:

- **uri**: The endpoint for the service
- **username**: User name to use for authentication
- **password**: Password to use for authentication
- **sslSkipVerify**: "adbc.flight.sql.client_option.authority",
- **headers**: Key/value pairs of additional headers to include with the request.
- **supportsWriteUpdate**: Indicates whether the data source supports creating new tables
- **supportsCatalogs**: Indicates whether the data source supports catalog names
- **type**: Specifies the type of data source used for running data from FlightSqlData. The supported types are:
    - Dremio
    - Denodo
    - DuckDB
    - SQLite
    - SpiceAI
- **tableTypes**: The table types to include in the GetObjects call
- **sqlFile**: A path to a SQL file to run queries to test CRUD operations
- **metadata**: Used for the GetObjects calls
  - **catalog**: The catalog name to use for the GetObjects call
  - **schema**: The schema name to use for the GetObjects call
  - **table**: The table name to use for the GetObjects call
  - **expectedColumnCount**: The number of columns that should be returned
- **authorization**: Used to set the `adbc.flight.sql.authorization_header` property
- **authority**: Used to set the `adbc.flight.sql.client_option.authority` property
- **query**: Select query run against the data source,
- **expectedResults**: Number of results expected from the query

The configuration file supports targeting multiple data sources
simultaneously. To use multiple data sources, you can configure them like:

```
    "testEnvironments**: [
        "Dremio_Remote",
        "DuckDb_Local",
        "SQLite_Local"
    ],
    "environments**: {
        "SQLite_Local**:
        {
           ...
        },
        "DuckDb_Local**:
        {
           ...
		},
        "Dremio_Remote**: {
           ...
        }
```

### Spice.ai OSS Configuration

Use the following command to run a local test instance of Spice.ai OSS. An example test configuration is available at [flightsql-spiceai.json](/csharp/configs/flightsql-spiceai.json).

```bash
docker compose up spiceai-test
```

Output:

```console
[+] Running 1/0
 ✔ Container adbc-spiceai  Created                                                    0.0s
Attaching to adbc-spiceai
adbc-spiceai  | 2025-06-12T22:44:38.684347Z  INFO spiced: Starting runtime v1.4.0+models
adbc-spiceai  | 2025-06-12T22:44:38.684737Z  INFO runtime::init::caching: Initialized results cache; max size: 128.00 MiB, item ttl: 1s
adbc-spiceai  | 2025-06-12T22:44:38.684772Z  INFO runtime::init::caching: Initialized search results cache;
adbc-spiceai  | 2025-06-12T22:44:39.043113Z  INFO runtime::init::dataset: Initializing dataset nation
adbc-spiceai  | 2025-06-12T22:44:39.044803Z  INFO runtime::metrics_server: Spice Runtime Metrics listening on 0.0.0.0:9090
adbc-spiceai  | 2025-06-12T22:44:39.044866Z  INFO runtime::opentelemetry: Spice Runtime OpenTelemetry listening on 127.0.0.1:50052
adbc-spiceai  | 2025-06-12T22:44:39.044926Z  INFO runtime::http: Spice Runtime HTTP listening on 0.0.0.0:8090
adbc-spiceai  | 2025-06-12T22:44:39.044351Z  INFO runtime::flight: Spice Runtime Flight listening on 0.0.0.0:50051
adbc-spiceai  | 2025-06-12T22:44:39.848102Z  INFO runtime::init::dataset: Dataset nation registered (s3://spiceai-demo-datasets/tpch/nation/), acceleration (arrow), results cache enabled.
adbc-spiceai  | 2025-06-12T22:44:39.849943Z  INFO runtime::accelerated_table::refresh_task: Loading data for dataset nation
adbc-spiceai  | 2025-06-12T22:44:40.537458Z  INFO runtime::accelerated_table::refresh_task: Loaded 25 rows (3.35 kiB) for dataset nation in 687ms.
adbc-spiceai  | 2025-06-12T22:44:40.568947Z  INFO runtime: All components are loaded. Spice runtime is ready!
```

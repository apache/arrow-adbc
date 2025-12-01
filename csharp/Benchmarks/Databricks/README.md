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

# Databricks CloudFetch E2E Benchmark

Real end-to-end benchmark for measuring memory usage and performance of the Databricks CloudFetch implementation against an actual Databricks cluster.

## Overview

This benchmark tests the complete CloudFetch flow with real queries against a Databricks warehouse:
- Full end-to-end CloudFetch flow (query execution, downloads, LZ4 decompression, batch consumption)
- Real data from Databricks tables
- Memory usage with actual network I/O
- Power BI consumption simulation with batch-proportional delays

## Benchmark

### CloudFetchRealE2EBenchmark

**Real end-to-end benchmark against actual Databricks cluster:**

**Parameters:**
- `ReadDelayMs`: Fixed at 5ms per 10K rows to simulate Power BI processing delays

**Method:**
- `ExecuteLargeQuery`: Executes the query specified in the config file, reads all batches with Power BI-like processing delays

**Prerequisites:**
- Set `DATABRICKS_TEST_CONFIG_FILE` environment variable pointing to your config JSON
- Config file must contain:
  - `uri`: Full Databricks warehouse URI (e.g., `https://hostname/sql/1.0/warehouses/xxx`)
  - `token`: Databricks access token
  - `query`: SQL query to execute (this will be run by the benchmark)

## Running the Benchmark

### Run the CloudFetch E2E benchmark on .NET 8.0:
```bash
cd csharp
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks-config.json
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetchRealE2E*"
```

### Run the CloudFetch E2E benchmark on .NET Framework 4.7.2 (Windows only, simulates Power BI):
```powershell
cd csharp
$env:DATABRICKS_TEST_CONFIG_FILE="C:\path\to\databricks-config.json"
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net472 -- --filter "*CloudFetchRealE2E*"
```

**Note**: .NET Framework 4.7.2 is only available on Windows. This target is useful for testing CloudFetch behavior in Power BI-like environments, as Power BI Desktop runs on .NET Framework 4.7.2.

### Real E2E Benchmark Configuration

Create a JSON config file with your Databricks cluster details:

```json
{
  "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/xxx",
  "token": "dapi...",
  "query": "select * from main.tpcds_sf1_delta.catalog_sales",
  "type": "databricks"
}
```

Then set the environment variable:
```bash
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks-config.json
```

**Note**: The `query` field specifies the SQL query that will be executed during the benchmark. Use a query that returns a large result set to properly test CloudFetch performance.

## Understanding the Results

### Key Metrics:

- **Peak Memory (MB)**: Maximum working set memory during execution
  - Displayed in the summary table via custom column
  - Printed to console output during each benchmark iteration
  - Shows the real memory footprint during CloudFetch operations
  - Stored in temp file for accurate reporting across BenchmarkDotNet processes

- **Total Rows**: Total number of rows processed during the benchmark
  - Displayed in the summary table via custom column
  - Shows the actual data volume processed

- **Total Batches**: Total number of Arrow RecordBatch objects processed
  - Displayed in the summary table via custom column
  - Indicates how data was chunked by CloudFetch
  - Useful for understanding batch size and network operation counts

- **Allocated**: Total managed memory allocated during the operation
  - Lower is better for memory efficiency

- **Gen0/Gen1/Gen2**: Number of garbage collections
  - Gen0: Frequent, low cost (short-lived objects)
  - Gen1/Gen2: Less frequent, higher cost (longer-lived objects)
  - LOH: Part of Gen2, objects >85KB

- **Mean/Median**: Execution time statistics
  - Shows the end-to-end time including query execution, CloudFetch downloads, LZ4 decompression, and batch consumption

### Example Output

**Console output during benchmark execution:**
```
Loaded config from: /path/to/databricks-config.json
Hostname: adb-6436897454825492.12.azuredatabricks.net
HTTP Path: /sql/1.0/warehouses/2f03dd43e35e2aa0
Query: select * from main.tpcds_sf1_delta.catalog_sales
Benchmark will test CloudFetch with 5ms per 10K rows read delay

// Warmup
CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 272.97 MB, Total rows: 1,441,548, Total batches: 145
Metrics written to: /tmp/cloudfetch_benchmark_metrics.json
WorkloadWarmup   1: 1 op, 11566591709.00 ns, 11.5666 s/op

// Actual iterations
CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 249.11 MB, Total rows: 1,441,548, Total batches: 145
Metrics written to: /tmp/cloudfetch_benchmark_metrics.json
WorkloadResult   1: 1 op, 8752445353.00 ns, 8.7524 s/op

CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 261.95 MB, Total rows: 1,441,548, Total batches: 145
Metrics written to: /tmp/cloudfetch_benchmark_metrics.json
WorkloadResult   2: 1 op, 9794630771.00 ns, 9.7946 s/op

CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 258.39 MB, Total rows: 1,441,548, Total batches: 145
Metrics written to: /tmp/cloudfetch_benchmark_metrics.json
WorkloadResult   3: 1 op, 9017280271.00 ns, 9.0173 s/op
```

**Summary table:**
```
BenchmarkDotNet v0.15.5, macOS Sequoia 15.7.1 (24G231) [Darwin 24.6.0]
Apple M1 Max, 1 CPU, 10 logical and 10 physical cores
.NET SDK 8.0.407
  [Host] : .NET 8.0.19 (8.0.19, 8.0.1925.36514), Arm64 RyuJIT armv8.0-a

| Method            | ReadDelayMs | Mean    | Min     | Max     | Median  | Peak Memory (MB) | Total Rows | Total Batches | Gen0       | Gen1       | Gen2       | Allocated |
|------------------ |------------ |--------:|--------:|--------:|--------:|-----------------:|-----------:|--------------:|-----------:|-----------:|-----------:|----------:|
| ExecuteLargeQuery | 5           | 9.19 s  | 8.75 s  | 9.79 s  | 9.02 s  | 256.48           | 1,441,548  | 145           | 28000.0000 | 28000.0000 | 28000.0000 |   1.78 GB |
```

**Key Metrics:**
- **E2E Time**: 8.75-9.79 seconds (includes query execution, CloudFetch downloads, LZ4 decompression, batch consumption)
- **Peak Memory**: 256.48 MB (tracked via Process.WorkingSet64, displayed via custom column)
- **Total Rows**: 1,441,548 rows processed
- **Total Batches**: 145 Arrow RecordBatch objects (average ~9,941 rows per batch)
- **Total Allocated**: 1.78 GB managed memory
- **GC Collections**: 28K Gen0/Gen1/Gen2 collections

**Note**: Metrics (Peak Memory, Total Rows, Total Batches) are stored in a temporary JSON file (`/tmp/cloudfetch_benchmark_metrics.json` on Unix, `%TEMP%\cloudfetch_benchmark_metrics.json` on Windows) during benchmark execution. Custom BenchmarkDotNet columns read from this file to display accurate values in the summary table.

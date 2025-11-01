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

### Run the CloudFetch E2E benchmark:
```bash
cd csharp
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks-config.json
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetchRealE2E*"
```

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
  - Printed to console output during each benchmark iteration
  - Shows the real memory footprint during CloudFetch operations

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
CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 272.97 MB
WorkloadWarmup   1: 1 op, 11566591709.00 ns, 11.5666 s/op

// Actual iterations
CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 249.11 MB
WorkloadResult   1: 1 op, 8752445353.00 ns, 8.7524 s/op

CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 261.95 MB
WorkloadResult   2: 1 op, 9794630771.00 ns, 9.7946 s/op

CloudFetch E2E [Delay=5ms/10K rows] - Peak memory: 258.39 MB
WorkloadResult   3: 1 op, 9017280271.00 ns, 9.0173 s/op
```

**Summary table:**
```
BenchmarkDotNet v0.15.4, macOS Sequoia 15.7.1 (24G231) [Darwin 24.6.0]
Apple M1 Max, 1 CPU, 10 logical and 10 physical cores
.NET SDK 8.0.407
  [Host] : .NET 8.0.19 (8.0.19, 8.0.1925.36514), Arm64 RyuJIT armv8.0-a

| Method            | ReadDelayMs | Mean    | Min     | Max     | Median  | Peak Memory (MB)          | Gen0       | Gen1       | Gen2       | Allocated |
|------------------ |------------ |--------:|--------:|--------:|--------:|--------------------------:|-----------:|-----------:|-----------:|----------:|
| ExecuteLargeQuery | 5           | 9.19 s  | 8.75 s  | 9.79 s  | 9.02 s  | See previous console output | 28000.0000 | 28000.0000 | 28000.0000 |   1.78 GB |
```

**Key Metrics:**
- **E2E Time**: 8.75-9.79 seconds (includes query execution, CloudFetch downloads, LZ4 decompression, batch consumption)
- **Peak Memory**: 249-262 MB (tracked via Process.WorkingSet64, printed in console)
- **Total Allocated**: 1.78 GB managed memory
- **GC Collections**: 28K Gen0/Gen1/Gen2 collections

**Note**: Peak memory values are printed to console during execution since BenchmarkDotNet runs each iteration in a separate process.

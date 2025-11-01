# Databricks CloudFetch Memory Benchmarks

Comprehensive benchmarks for measuring memory usage and performance of the Databricks CloudFetch implementation.

## Overview

Two types of benchmarks:

### 1. Synthetic Benchmarks (No DB Required)
Simulate the full CloudFetch pipeline with generated test data:
- Total memory allocations
- GC collections (Gen0, Gen1, Gen2)
- Large Object Heap (LOH) usage
- Impact of different optimization strategies
- Memory manager size tuning
- Read patterns (including Power BI simulation)

### 2. Real E2E Benchmarks (Requires Databricks)
Test against actual Databricks cluster with real queries:
- Full end-to-end CloudFetch flow
- Real data from TPCDS benchmark tables
- Memory usage with actual network I/O
- Parameter tuning validation (memory manager size, etc.)
- Power BI consumption simulation

## Benchmarks

### CloudFetchMemoryBenchmark

Tests the complete pipeline with realistic scenarios:

**Parameters:**
- `ReadDelayMs`: Simulates read delays (0ms = no delay, 10ms = simulate Power BI)
- `MemoryManagerMB`: Memory manager size limit (100MB, 200MB)

**Methods:**
1. **PreDecompressApproach** (Baseline): Pre-decompresses all data before queueing
   - Old approach: ~4 files × 20MB = 80MB in result queue
   - Higher memory usage but simpler implementation

2. **StreamingApproach** (Optimized): Streams LZ4 decompression on-demand
   - New approach: ~4 files × 8MB = 32MB in result queue
   - 66% memory reduction, decompresses as ArrowStreamReader reads

### CloudFetchMemoryManagerBenchmark

Measures the impact of memory manager size on throughput and concurrency:

**Parameters:**
- `FileCount`: Number of files to process (5, 10, 20)
- `MemoryManagerMB`: Memory manager size limit (50MB, 100MB, 200MB)

Shows how the memory limit affects download parallelism and queueing behavior.

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

## Running the Benchmarks

### Run all CloudFetch benchmarks:
```bash
cd csharp
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetch*"
```

### Run specific benchmark:
```bash
# Synthetic memory optimization comparison (no DB needed)
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetchMemoryBenchmark*"

# Synthetic memory manager tuning (no DB needed)
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetchMemoryManager*"

# Real E2E with Databricks (requires DATABRICKS_TEST_CONFIG_FILE)
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks-config.json
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetchRealE2E*" --job dry
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

### Run with specific parameters:
```bash
# Only test with 100MB memory manager and no read delay
dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- \
  --filter "*CloudFetchMemoryBenchmark*" \
  --job short \
  --runtimes net8.0
```

## Understanding the Results

### Key Metrics:

- **Peak Memory (MB)**: Maximum working set memory during execution
  - Printed to console output during benchmark execution
  - Shows the real memory impact of each approach
  - Pre-decompress: ~465 MB peak
  - Streaming: ~176 MB peak (62% reduction!)

- **Allocated**: Total managed memory allocated during the operation
  - Lower is better
  - Compare Pre-decompress vs Streaming to see memory savings

- **Gen0/Gen1/Gen2**: Number of garbage collections
  - Gen0: Frequent, low cost (short-lived objects)
  - Gen1/Gen2: Less frequent, higher cost (longer-lived objects)
  - LOH: Part of Gen2, objects >85KB

- **Mean/Median**: Average execution time
  - Streaming may be slightly slower but uses much less memory

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
| ExecuteLargeQuery | 5           | 9.19 s  | 8.75 s  | 9.79 s  | 9.02 s  | See previous console output | 28000.0000 | 28000.0000 | 23000.0000 |   1.77 GB |
```

**Key Metrics:**
- **E2E Time**: 8.75-9.79 seconds (includes query execution, CloudFetch downloads, LZ4 decompression, batch consumption)
- **Peak Memory**: 249-262 MB (tracked via Process.WorkingSet64, printed in console)
- **Total Allocated**: 1.77 GB managed memory
- **GC Collections**: 28K Gen0/Gen1, 23K Gen2 collections

**Note**: Peak memory values are printed to console during execution since BenchmarkDotNet runs each iteration in a separate process.

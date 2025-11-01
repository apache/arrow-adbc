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

### Expected Results:

Based on the optimization work:
- **Peak Memory**:
  - Pre-decompress: ~465 MB
  - Streaming: ~176 MB
  - **Reduction: 289 MB (62%)**
- **Total Allocations**: Streaming uses ~639x less cumulative allocations (640 MB vs 1.16 MB)
- **GC Collections**: Streaming has fewer Gen0/Gen1/Gen2 collections
- **Memory Manager**: 100MB limit provides good balance vs 200MB

### Example Output:

```
Test data: 10 files, 200MB uncompressed → 61MB compressed
PreDecompressApproach - Peak memory: 465.70 MB
StreamingApproach - Peak memory: 176.16 MB

| Method                          | ReadDelayMs | MemoryManagerMB | Mean      | Allocated | Peak Memory (MB) |
|-------------------------------- |------------ |---------------- |----------:|----------:|------------------:|
| 'Pre-decompress (OLD approach)' | 0           | 100             | 176.21 ms | 640.04 MB | ~465 MB          |
| 'Streaming LZ4 (NEW approach)'  | 0           | 100             |  95.53 ms |   1.16 MB | ~176 MB          |
```

**Key Takeaway**: Streaming LZ4 reduces peak memory by 289 MB (62%) and total allocations by 639x!

### Power BI Simulation:

The `ReadDelayMs` parameter simulates slower consumption patterns:
- `0ms`: Fast reading (typical programmatic access)
- `10ms`: Simulates Power BI slower processing every 1MB
- Shows how streaming benefits are magnified with slower consumers

## Implementation Details

### Test Data:
- 10 files, each ~20MB decompressed (typical CloudFetch file size)
- Compresses to ~8MB (realistic compression ratio)
- Mix of patterns (70% repeated, 30% random) mimics Arrow columnar data

### Pipeline Simulation:
1. **Download phase**: Files downloaded respecting memory manager limit
2. **Queue phase**: Up to 4 files buffered in result queue
3. **Read phase**: ArrowStreamReader consumes in 80KB chunks

### Memory Manager:
- Tracks compressed file sizes (in-flight downloads)
- Blocks new downloads when limit reached
- Default production value: 100MB

## Related Documentation

- `lz4-memory-optimization-approaches.md`: Detailed analysis of optimization approaches
- PR #3657: Implementation of streaming LZ4 decompression

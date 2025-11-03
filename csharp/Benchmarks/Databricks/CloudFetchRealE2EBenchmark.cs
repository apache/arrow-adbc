/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace Apache.Arrow.Adbc.Benchmarks.Databricks
{
    /// <summary>
    /// Custom column to display peak memory usage in the benchmark results table.
    /// </summary>
    public class PeakMemoryColumn : IColumn
    {
        public string Id => nameof(PeakMemoryColumn);
        public string ColumnName => "Peak Memory (MB)";
        public string Legend => "Peak working set memory during benchmark execution";
        public UnitType UnitType => UnitType.Size;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 0;
        public bool IsNumeric => true;
        public bool IsAvailable(Summary summary) => true;
        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            // Try CloudFetchRealE2EBenchmark (includes parameters in key)
            if (benchmarkCase.Descriptor.Type == typeof(CloudFetchRealE2EBenchmark))
            {
                // Extract ReadDelayMs parameter
                var readDelayParam = benchmarkCase.Parameters["ReadDelayMs"];
                string key = $"ExecuteLargeQuery_{readDelayParam}";
                if (CloudFetchRealE2EBenchmark.PeakMemoryResults.TryGetValue(key, out var peakMemoryMB))
                {
                    return $"{peakMemoryMB:F2}";
                }
            }

            return "See previous console output";
        }

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            return GetValue(summary, benchmarkCase);
        }

        public override string ToString() => ColumnName;
    }

    /// <summary>
    /// Configuration model for Databricks test configuration JSON file.
    /// </summary>
    internal class DatabricksTestConfig
    {
        public string? uri { get; set; }
        public string? token { get; set; }
        public string? query { get; set; }
        public string? type { get; set; }
        public string? catalog { get; set; }
        public string? schema { get; set; }
    }

    /// <summary>
    /// Real E2E performance benchmark for Databricks CloudFetch with actual cluster.
    ///
    /// Prerequisites:
    /// - Set DATABRICKS_TEST_CONFIG_FILE environment variable
    /// - Config file should contain cluster connection details
    ///
    /// Run with: dotnet run -c Release --project Benchmarks/Benchmarks.csproj --framework net8.0 -- --filter "*CloudFetchRealE2E*" --job dry
    ///
    /// Measures:
    /// - Peak memory usage
    /// - Total allocations
    /// - GC collections
    /// - Query execution time
    /// - Row processing throughput
    ///
    /// Parameters:
    /// - ReadDelayMs: Fixed at 5 milliseconds per 10K rows to simulate Power BI consumption
    /// </summary>
    [MemoryDiagnoser]
    [GcServer(true)]
    [SimpleJob(warmupCount: 1, iterationCount: 3)]
    [MinColumn, MaxColumn, MeanColumn, MedianColumn]
    public class CloudFetchRealE2EBenchmark
    {
        // Static dictionary to store peak memory results for the custom column
        public static readonly Dictionary<string, double> PeakMemoryResults = new Dictionary<string, double>();

        private AdbcConnection? _connection;
        private Process _currentProcess = null!;
        private long _peakMemoryBytes;
        private DatabricksTestConfig _testConfig = null!;
        private string _hostname = null!;
        private string _httpPath = null!;

        [Params(5)] // Read delay in milliseconds per 10K rows (5 = simulate Power BI)
        public int ReadDelayMs { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            // Check if Databricks config is available
            string? configFile = Environment.GetEnvironmentVariable("DATABRICKS_TEST_CONFIG_FILE");
            if (string.IsNullOrEmpty(configFile))
            {
                throw new InvalidOperationException(
                    "DATABRICKS_TEST_CONFIG_FILE environment variable must be set. " +
                    "Set it to the path of your Databricks test configuration JSON file.");
            }

            // Read and parse config file
            string configJson = File.ReadAllText(configFile);
            _testConfig = JsonSerializer.Deserialize<DatabricksTestConfig>(configJson)
                ?? throw new InvalidOperationException("Failed to parse config file");

            if (string.IsNullOrEmpty(_testConfig.uri) || string.IsNullOrEmpty(_testConfig.token))
            {
                throw new InvalidOperationException("Config file must contain 'uri' and 'token' fields");
            }

            if (string.IsNullOrEmpty(_testConfig.query))
            {
                throw new InvalidOperationException("Config file must contain 'query' field");
            }

            // Parse URI to extract hostname and http_path
            // Format: https://hostname/sql/1.0/warehouses/xxx
            var uri = new Uri(_testConfig.uri);
            _hostname = uri.Host;
            _httpPath = uri.PathAndQuery;

            _currentProcess = Process.GetCurrentProcess();
            Console.WriteLine($"Loaded config from: {configFile}");
            Console.WriteLine($"Hostname: {_hostname}");
            Console.WriteLine($"HTTP Path: {_httpPath}");
            Console.WriteLine($"Query: {_testConfig.query}");
            Console.WriteLine($"Benchmark will test CloudFetch with {ReadDelayMs}ms per 10K rows read delay");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            // Create connection for this iteration using config values
            var parameters = new Dictionary<string, string>
            {
                [AdbcOptions.Uri] = _testConfig.uri!,
                [SparkParameters.Token] = _testConfig.token!,
                [DatabricksParameters.UseCloudFetch] = "true",
                [DatabricksParameters.EnableDirectResults] = "true",
                [DatabricksParameters.CanDecompressLz4] = "true",
                [DatabricksParameters.MaxBytesPerFile] = "10485760", // 10MB per file
            };

            var driver = new DatabricksDriver();
            var database = driver.Open(parameters);
            _connection = database.Connect(parameters);

            // Reset peak memory tracking
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: false);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: false);
            _currentProcess.Refresh();
            _peakMemoryBytes = _currentProcess.WorkingSet64;
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            _connection?.Dispose();
            _connection = null;

            // Print and store peak memory for this iteration
            double peakMemoryMB = _peakMemoryBytes / 1024.0 / 1024.0;
            Console.WriteLine($"CloudFetch E2E [Delay={ReadDelayMs}ms/10K rows] - Peak memory: {peakMemoryMB:F2} MB");

            // Store in static dictionary for the custom column (key includes parameter)
            string key = $"ExecuteLargeQuery_{ReadDelayMs}";
            PeakMemoryResults[key] = peakMemoryMB;
        }

        /// <summary>
        /// Execute a large query against Databricks and consume all result batches.
        /// Simulates client behavior like Power BI reading data.
        /// Uses the query from the config file.
        /// </summary>
        [Benchmark]
        public async Task<long> ExecuteLargeQuery()
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Connection not initialized");
            }

            // Execute query from config file
            var statement = _connection.CreateStatement();
            statement.SqlQuery = _testConfig.query;

            var result = await statement.ExecuteQueryAsync();
            if (result.Stream == null)
            {
                throw new InvalidOperationException("Result stream is null");
            }

            // Read all batches and track peak memory
            long totalRows = 0;
            long totalBatches = 0;
            RecordBatch? batch;

            while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
            {
                totalRows += batch.Length;
                totalBatches++;

                // Track peak memory periodically
                if (totalBatches % 10 == 0)
                {
                    TrackPeakMemory();
                }

                // Simulate Power BI processing delay if configured
                // Delay is proportional to batch size: ReadDelayMs per 10K rows
                if (ReadDelayMs > 0)
                {
                    int delayForBatch = (int)((batch.Length / 10000.0) * ReadDelayMs);
                    if (delayForBatch > 0)
                    {
                        Thread.Sleep(delayForBatch);
                    }
                }

                batch.Dispose();
            }

            // Final peak memory check
            TrackPeakMemory();

            statement.Dispose();
            return totalRows;
        }

        private void TrackPeakMemory()
        {
            _currentProcess.Refresh();
            long currentMemory = _currentProcess.WorkingSet64;
            if (currentMemory > _peakMemoryBytes)
            {
                _peakMemoryBytes = currentMemory;
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }
    }
}

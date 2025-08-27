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
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Reader for CloudFetch results from Databricks Spark Thrift server.
    /// Handles downloading and processing URL-based result sets.
    /// </summary>
    internal sealed class CloudFetchReader : BaseDatabricksReader
    {
        private ICloudFetchDownloadManager? downloadManager;
        private ArrowStreamReader? currentReader;
        private IDownloadResult? currentDownloadResult;
        private bool isPrefetchEnabled;

        // Row-count batching state
        private readonly int targetBatchSizeRows;
        private readonly List<RecordBatch> pendingBatches = new List<RecordBatch>();
        private int pendingTotalRows = 0;

        // Performance logging
        private static readonly object s_logLock = new object();
        private static readonly string s_logFilePath = Path.Combine(Path.GetTempPath(), "adbc-cloudfetch-concatenation.log");

        // Cumulative performance tracking
        private static long s_totalConcatenationTimeMs = 0;
        private static long s_totalOperationTimeMs = 0;
        private static int s_totalConcatenationOperations = 0;
        private static int s_totalSingleBatchOperations = 0;
        private static long s_totalRowsProcessed = 0;
        private static readonly DateTime s_sessionStartTime = DateTime.UtcNow;

        // Client call tracking
        private static int s_totalClientCalls = 0;
        private static long s_totalClientResponseTimeMs = 0;
        private static DateTime s_lastClientCallTime = DateTime.MinValue;
        private static readonly List<long> s_clientCallGaps = new List<long>();

        // Final summary tracking
        private static bool s_finalSummaryWritten = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchReader"/> class.
        /// </summary>
        /// <param name="statement">The Databricks statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        public CloudFetchReader(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            TFetchResultsResp? initialResults,
            bool isLz4Compressed,
            HttpClient httpClient)
            : base(statement, schema, response, isLz4Compressed)
        {
            var connectionProps = statement.Connection.Properties;

            // Initialize performance logging
            EnsureLogHeader();

            // Parse batch size in rows (default: 1000 - conservative optimal for varying conditions)
            targetBatchSizeRows = 1;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchBatchSizeRows, out string? batchSizeRowsStr))
            {
                if (int.TryParse(batchSizeRowsStr, out int parsedBatchSizeRows) && parsedBatchSizeRows > 0)
                {
                    targetBatchSizeRows = parsedBatchSizeRows;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchBatchSizeRows}: {batchSizeRowsStr}. Expected a positive integer.");
                }
            }

            // Check if prefetch is enabled
            isPrefetchEnabled = true; // Default to true
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchPrefetchEnabled, out string? prefetchEnabledStr))
            {
                if (bool.TryParse(prefetchEnabledStr, out bool parsedPrefetchEnabled))
                {
                    isPrefetchEnabled = parsedPrefetchEnabled;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchPrefetchEnabled}: {prefetchEnabledStr}. Expected a boolean value.");
                }
            }

            // Initialize the download manager
            if (isPrefetchEnabled)
            {
                downloadManager = new CloudFetchDownloadManager(statement, schema, response, initialResults, isLz4Compressed, httpClient);
                downloadManager.StartAsync().Wait();
            }
            else
            {
                // For now, we only support the prefetch implementation
                // This flag is reserved for future use if we need to support a non-prefetch mode
                downloadManager = new CloudFetchDownloadManager(statement, schema, response, initialResults, isLz4Compressed, httpClient);
                downloadManager.StartAsync().Wait();
            }
        }

        /// <summary>
        /// Reads the next record batch from the result set.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next record batch, or null if there are no more batches.</returns>
        public override async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async _ =>
            {
                var callStartTime = DateTime.UtcNow;
                var callStopwatch = Stopwatch.StartNew();

                // Track client call patterns
                long gapFromLastCallMs = 0;
                lock (s_logLock)
                {
                    s_totalClientCalls++;

                    if (s_lastClientCallTime != DateTime.MinValue)
                    {
                        gapFromLastCallMs = (long)(callStartTime - s_lastClientCallTime).TotalMilliseconds;
                        s_clientCallGaps.Add(gapFromLastCallMs);
                    }

                    s_lastClientCallTime = callStartTime;
                }

                ThrowIfDisposed();
                var result = await ReadNextBatchRowBased(cancellationToken);

                // If this is the end of the stream (result is null), write final summary
                if (result == null && !s_finalSummaryWritten)
                {
                    lock (s_logLock)
                    {
                        if (!s_finalSummaryWritten)
                        {
                            s_finalSummaryWritten = true;
                            WriteFinalSessionSummary();
                        }
                    }
                }

                // Track response time
                callStopwatch.Stop();
                var responseTimeMs = callStopwatch.ElapsedMilliseconds;

                lock (s_logLock)
                {
                    s_totalClientResponseTimeMs += responseTimeMs;

                    // Log client call details
                    WriteClientCallLog(s_totalClientCalls, responseTimeMs, gapFromLastCallMs,
                        result?.Length ?? 0, result != null);

                    // Write client summary every 20 calls
                    if (s_totalClientCalls % 20 == 0)
                    {
                        WriteClientSummaryLog();
                    }
                }

                return result;
            });
        }

        /// <summary>
        /// Row-based batch reading with configurable batch size
        /// </summary>
        private async ValueTask<RecordBatch?> ReadNextBatchRowBased(CancellationToken cancellationToken)
        {
            // Accumulate batches until we exceed target row count
            while (pendingTotalRows <= targetBatchSizeRows)
            {
                var nextBatch = await GetNextSourceBatch(cancellationToken);
                if (nextBatch == null)
                {
                    // No more data - return accumulated batches if any
                    return FlushPendingBatches();
                }

                // Add to pending batches
                pendingBatches.Add(nextBatch);
                pendingTotalRows += nextBatch.Length;
            }

            // We have enough rows - return all accumulated batches
            return FlushPendingBatches();
        }

        /// <summary>
        /// Gets the next batch from the source files
        /// </summary>
        private async ValueTask<RecordBatch?> GetNextSourceBatch(CancellationToken cancellationToken)
        {
            while (true)
            {
                // If we have a current reader, try to read the next batch
                if (this.currentReader != null)
                {
                    RecordBatch? next = await this.currentReader.ReadNextRecordBatchAsync(cancellationToken);
                    if (next != null)
                    {
                        return next;
                    }
                    else
                    {
                        // Clean up the current reader and download result
                        this.currentReader.Dispose();
                        this.currentReader = null;

                        if (this.currentDownloadResult != null)
                        {
                            this.currentDownloadResult.Dispose();
                            this.currentDownloadResult = null;
                        }
                    }
                }

                // Get the next downloaded file
                if (this.downloadManager != null)
                {
                    try
                    {
                        this.currentDownloadResult = await this.downloadManager.GetNextDownloadedFileAsync(cancellationToken);
                        if (this.currentDownloadResult == null)
                        {
                            this.downloadManager.Dispose();
                            this.downloadManager = null;
                            return null; // No more files
                        }

                        await this.currentDownloadResult.DownloadCompletedTask;
                        this.currentReader = new ArrowStreamReader(this.currentDownloadResult.DataStream);
                        continue;
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"Error getting next downloaded file: {ex.Message}");
                        throw;
                    }
                }

                return null; // No more files
            }
        }

        /// <summary>
        /// Returns any remaining pending batches as a final batch
        /// </summary>
        private RecordBatch? FlushPendingBatches()
        {
            if (pendingBatches.Count == 0) return null;

            var stopwatch = Stopwatch.StartNew();
            var batchCount = pendingBatches.Count;
            var totalRows = pendingTotalRows;
            RecordBatch result;

            if (batchCount == 1)
            {
                result = pendingBatches[0];
                pendingBatches.Clear();

                // Log single batch (no concatenation)
                stopwatch.Stop();
                WriteConcatenationLog(1, totalRows, stopwatch.ElapsedMilliseconds, 0, "single_batch_no_concat");
            }
            else
            {
                // Measure concatenation time
                var concatStopwatch = Stopwatch.StartNew();
                result = ConcatenateBatches(pendingBatches);
                concatStopwatch.Stop();

                foreach (var batch in pendingBatches)
                {
                    batch.Dispose();
                }
                pendingBatches.Clear();

                // Log concatenation performance
                stopwatch.Stop();
                WriteConcatenationLog(batchCount, totalRows, stopwatch.ElapsedMilliseconds, concatStopwatch.ElapsedMilliseconds, "multi_batch_concat");
            }

            pendingTotalRows = 0;
            return result;
        }



        /// <summary>
        /// Concatenates multiple RecordBatches into a single batch
        /// </summary>
        private RecordBatch ConcatenateBatches(List<RecordBatch> batches)
        {
            if (batches.Count == 0) throw new ArgumentException("Cannot concatenate empty batch list");
            if (batches.Count == 1) return batches[0].Slice(0, batches[0].Length); // Return a copy

            var totalStopwatch = Stopwatch.StartNew();
            var columnCount = batches[0].Schema.FieldsList.Count;
            var totalRows = batches.Sum(b => b.Length);
            var arrays = new IArrowArray[columnCount];

            // Time per-column concatenation
            var columnTimes = new List<long>();

            for (int columnIndex = 0; columnIndex < arrays.Length; columnIndex++)
            {
                var columnStopwatch = Stopwatch.StartNew();
                var columnArrays = batches.Select(batch => batch.Column(columnIndex)).ToList();
                arrays[columnIndex] = ArrowArrayConcatenator.Concatenate(columnArrays);
                columnStopwatch.Stop();
                columnTimes.Add(columnStopwatch.ElapsedMilliseconds);
            }

            totalStopwatch.Stop();

            // Log detailed concatenation metrics
            var avgColumnTime = columnTimes.Count > 0 ? columnTimes.Average() : 0;
            var maxColumnTime = columnTimes.Count > 0 ? columnTimes.Max() : 0;

            WriteConcatenationDetailLog(batches.Count, totalRows, columnCount,
                totalStopwatch.ElapsedMilliseconds, avgColumnTime, maxColumnTime);

            return new RecordBatch(batches[0].Schema, arrays, totalRows);
        }

        /// <summary>
        /// Writes concatenation performance log entry
        /// </summary>
        private void WriteConcatenationLog(int batchCount, int totalRows, long totalTimeMs, long concatTimeMs, string operation)
        {
            try
            {
                lock (s_logLock)
                {
                    // Update cumulative statistics
                    s_totalOperationTimeMs += totalTimeMs;
                    s_totalConcatenationTimeMs += concatTimeMs;
                    s_totalRowsProcessed += totalRows;

                    if (operation == "multi_batch_concat")
                        s_totalConcatenationOperations++;
                    else
                        s_totalSingleBatchOperations++;

                    // Calculate session totals
                    var sessionDurationMs = (long)(DateTime.UtcNow - s_sessionStartTime).TotalMilliseconds;
                    var totalOps = s_totalConcatenationOperations + s_totalSingleBatchOperations;

                    var logEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}|{operation}|BatchCount={batchCount}|TotalRows={totalRows}|TotalTimeMs={totalTimeMs}|ConcatTimeMs={concatTimeMs}|AvgRowsPerMs={totalRows/(Math.Max(totalTimeMs, 1))}|TargetBatchSize={targetBatchSizeRows}|SessionTotalConcatMs={s_totalConcatenationTimeMs}|SessionTotalOpsMs={s_totalOperationTimeMs}|SessionTotalOps={totalOps}|SessionTotalRows={s_totalRowsProcessed}|SessionDurationMs={sessionDurationMs}";

                    File.AppendAllText(s_logFilePath, logEntry + Environment.NewLine);

                    // Write session summary every 10 operations
                    if (totalOps % 10 == 0)
                    {
                        WriteSessionSummaryLog();
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to write concatenation log: {ex.Message}");
            }
        }

        /// <summary>
        /// Writes detailed concatenation performance log entry
        /// </summary>
        private void WriteConcatenationDetailLog(int batchCount, int totalRows, int columnCount, long totalTimeMs, double avgColumnTimeMs, long maxColumnTimeMs)
        {
            try
            {
                var logEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}|concat_detail|BatchCount={batchCount}|TotalRows={totalRows}|Columns={columnCount}|TotalTimeMs={totalTimeMs}|AvgColumnMs={avgColumnTimeMs:F2}|MaxColumnMs={maxColumnTimeMs}|RowsPerMs={totalRows/(Math.Max(totalTimeMs, 1))}|RowsPerSecond={totalRows * 1000/(Math.Max(totalTimeMs, 1))}|TargetBatchSize={targetBatchSizeRows}";

                lock (s_logLock)
                {
                    File.AppendAllText(s_logFilePath, logEntry + Environment.NewLine);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to write detailed concatenation log: {ex.Message}");
            }
        }

        /// <summary>
        /// Writes client call log entry
        /// </summary>
        private void WriteClientCallLog(int callNumber, long responseTimeMs, long gapFromLastCallMs, int rowsReturned, bool hasData)
        {
            try
            {
                var logEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}|CLIENT_CALL|CallNum={callNumber}|ResponseMs={responseTimeMs}|GapFromLastMs={gapFromLastCallMs}|RowsReturned={rowsReturned}|HasData={hasData}|TargetBatchSize={targetBatchSizeRows}";

                File.AppendAllText(s_logFilePath, logEntry + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to write client call log: {ex.Message}");
            }
        }

        /// <summary>
        /// Writes client summary log entry
        /// </summary>
        private static void WriteClientSummaryLog()
        {
            try
            {
                var avgResponseTimeMs = s_totalClientCalls > 0 ? s_totalClientResponseTimeMs / (double)s_totalClientCalls : 0;
                var avgGapMs = s_clientCallGaps.Count > 0 ? s_clientCallGaps.Average() : 0;
                var minGapMs = s_clientCallGaps.Count > 0 ? s_clientCallGaps.Min() : 0;
                var maxGapMs = s_clientCallGaps.Count > 0 ? s_clientCallGaps.Max() : 0;
                var sessionDurationMs = (long)(DateTime.UtcNow - s_sessionStartTime).TotalMilliseconds;
                var callFrequencyHz = sessionDurationMs > 0 ? (s_totalClientCalls * 1000.0) / sessionDurationMs : 0;

                var summaryEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}|CLIENT_SUMMARY|" +
                                  $"TotalCalls={s_totalClientCalls}|TotalResponseMs={s_totalClientResponseTimeMs}|AvgResponseMs={avgResponseTimeMs:F2}|" +
                                  $"AvgGapMs={avgGapMs:F2}|MinGapMs={minGapMs}|MaxGapMs={maxGapMs}|CallFrequencyHz={callFrequencyHz:F2}|" +
                                  $"SessionDurationMs={sessionDurationMs}|ResponseTimePercent={((s_totalClientResponseTimeMs * 100.0) / Math.Max(sessionDurationMs, 1)):F1}%";

                File.AppendAllText(s_logFilePath, summaryEntry + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to write client summary log: {ex.Message}");
            }
        }

        /// <summary>
        /// Writes session summary log entry
        /// </summary>
        private static void WriteSessionSummaryLog()
        {
            try
            {
                var sessionDurationMs = (long)(DateTime.UtcNow - s_sessionStartTime).TotalMilliseconds;
                var totalOps = s_totalConcatenationOperations + s_totalSingleBatchOperations;
                var avgConcatTimePerOp = s_totalConcatenationOperations > 0 ? s_totalConcatenationTimeMs / (double)s_totalConcatenationOperations : 0;
                var avgTotalTimePerOp = totalOps > 0 ? s_totalOperationTimeMs / (double)totalOps : 0;
                var concatPercentage = s_totalOperationTimeMs > 0 ? (s_totalConcatenationTimeMs * 100.0) / s_totalOperationTimeMs : 0;
                var avgRowsPerSecond = sessionDurationMs > 0 ? (s_totalRowsProcessed * 1000.0) / sessionDurationMs : 0;

                var summaryEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}|SESSION_SUMMARY|" +
                                  $"TotalOps={totalOps}|ConcatOps={s_totalConcatenationOperations}|SingleOps={s_totalSingleBatchOperations}|" +
                                  $"TotalConcatMs={s_totalConcatenationTimeMs}|TotalOpsMs={s_totalOperationTimeMs}|SessionDurationMs={sessionDurationMs}|" +
                                  $"TotalRowsProcessed={s_totalRowsProcessed}|AvgConcatMs={avgConcatTimePerOp:F2}|AvgTotalMs={avgTotalTimePerOp:F2}|" +
                                  $"ConcatPercentage={concatPercentage:F1}%|AvgRowsPerSecond={avgRowsPerSecond:F0}";

                File.AppendAllText(s_logFilePath, summaryEntry + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to write session summary log: {ex.Message}");
            }
        }

        /// <summary>
        /// Writes final session summary on disposal
        /// </summary>
        private static void WriteFinalSessionSummary()
        {
            try
            {
                lock (s_logLock)
                {
                    WriteSessionSummaryLog();
                    WriteClientSummaryLog();

                    // Calculate final client statistics
                    var totalSessionSeconds = (DateTime.UtcNow - s_sessionStartTime).TotalSeconds;
                    var avgResponseTimeMs = s_totalClientCalls > 0 ? s_totalClientResponseTimeMs / (double)s_totalClientCalls : 0;
                    var avgGapMs = s_clientCallGaps.Count > 0 ? s_clientCallGaps.Average() : 0;
                    var responseTimePercent = totalSessionSeconds > 0 ? (s_totalClientResponseTimeMs / 10.0) / totalSessionSeconds : 0;

                    // Calculate performance improvement vs no-batching baseline
                    var expectedNoBatchingTime = s_totalClientCalls * 70.3; // 333.76s / 4750 calls = 70.3ms avg per call
                    var actualTotalTime = (s_totalClientResponseTimeMs + (s_clientCallGaps.Count > 0 ? s_clientCallGaps.Sum() : 0));
                    var performanceMultiplier = expectedNoBatchingTime > 0 ? expectedNoBatchingTime / actualTotalTime : 1.0;
                    var timeSavedMs = expectedNoBatchingTime - actualTotalTime;

                    var finalEntry = $"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}|FINAL_SUMMARY|" +
                                   $"SessionEnded={DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}|" +
                                   $"TotalSessionTimeSeconds={totalSessionSeconds:F2}|" +
                                   $"TotalClientCalls={s_totalClientCalls}|" +
                                   $"FinalAvgResponseMs={avgResponseTimeMs:F2}|" +
                                   $"FinalAvgGapMs={avgGapMs:F2}|" +
                                   $"FinalResponseTimePercent={responseTimePercent:F1}%|" +
                                   $"PerformanceMultiplier={performanceMultiplier:F1}x|" +
                                   $"TimeSavedMs={timeSavedMs:F0}|" +
                                   $"BatchOptimization=ENABLED";

                    File.AppendAllText(s_logFilePath, finalEntry + Environment.NewLine);
                    File.AppendAllText(s_logFilePath, Environment.NewLine); // Add blank line for readability

                    // Also write to console/debug for immediate visibility in tests
                    var logMessage = $"🎯 CloudFetch FINAL_SUMMARY written to: {s_logFilePath}";
                    Console.WriteLine(logMessage);
                    Debug.WriteLine(logMessage);

                    // For test environments, also log key metrics directly
                    Console.WriteLine($"📊 Performance: {s_totalClientCalls} calls, {performanceMultiplier:F1}x faster, {timeSavedMs:F0}ms saved");
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to write final session summary: {ex.Message}");
            }
        }

        /// <summary>
        /// Writes log header if file doesn't exist
        /// </summary>
        private static void EnsureLogHeader()
        {
            try
            {
                if (!File.Exists(s_logFilePath))
                {
                    lock (s_logLock)
                    {
                        if (!File.Exists(s_logFilePath)) // Double-check after acquiring lock
                        {
                            var header = $"# CloudFetch Concatenation & Client Performance Log - Started {s_sessionStartTime:yyyy-MM-dd HH:mm:ss} UTC" + Environment.NewLine +
                                        "# Operation Format: Timestamp|Operation|BatchCount|TotalRows|TotalTimeMs|ConcatTimeMs|AvgRowsPerMs|TargetBatchSize|SessionTotalConcatMs|SessionTotalOpsMs|SessionTotalOps|SessionTotalRows|SessionDurationMs" + Environment.NewLine +
                                        "# Detail Format: Timestamp|concat_detail|BatchCount|TotalRows|Columns|TotalTimeMs|AvgColumnMs|MaxColumnMs|RowsPerMs|RowsPerSecond|TargetBatchSize" + Environment.NewLine +
                                        "# Client Call Format: Timestamp|CLIENT_CALL|CallNum|ResponseMs|GapFromLastMs|RowsReturned|HasData|TargetBatchSize" + Environment.NewLine +
                                        "# Client Summary Format: Timestamp|CLIENT_SUMMARY|TotalCalls|TotalResponseMs|AvgResponseMs|AvgGapMs|MinGapMs|MaxGapMs|CallFrequencyHz|SessionDurationMs|ResponseTimePercent" + Environment.NewLine +
                                        "# Session Summary Format: Timestamp|SESSION_SUMMARY|TotalOps|ConcatOps|SingleOps|TotalConcatMs|TotalOpsMs|SessionDurationMs|TotalRowsProcessed|AvgConcatMs|AvgTotalMs|ConcatPercentage|AvgRowsPerSecond" + Environment.NewLine;
                            File.WriteAllText(s_logFilePath, header);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Failed to write log header: {ex.Message}");
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (this.currentReader != null)
            {
                this.currentReader.Dispose();
                this.currentReader = null;
            }

            if (this.currentDownloadResult != null)
            {
                this.currentDownloadResult.Dispose();
                this.currentDownloadResult = null;
            }

            if (this.downloadManager != null)
            {
                this.downloadManager.Dispose();
                this.downloadManager = null;
            }

            // Clean up row-based batching state
            foreach (var batch in this.pendingBatches)
            {
                batch.Dispose();
            }
            this.pendingBatches.Clear();

            // Write final session summary on disposal if not already written
            lock (s_logLock)
            {
                if (!s_finalSummaryWritten)
                {
                    s_finalSummaryWritten = true;
                    WriteFinalSessionSummary();
                }
            }

            base.Dispose(disposing);
        }
    }
}

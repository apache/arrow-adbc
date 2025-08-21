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
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

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

        // Tracing counters
        private int _readCallCount = 0;
        private int _totalBatchesReturned = 0;
        private long _totalRowsReturned = 0;
        private long _totalBytesRead = 0;
        private long _totalFilesDownloaded = 0;  // Track total files downloaded across all calls
        private long _totalFileDataProcessed = 0;  // Track total file data processed (downloaded file sizes)

        // Multi-file batch aggregation
        private readonly long _batchSizeLimitBytes;
        private readonly List<RecordBatch> _batchBuffer = new List<RecordBatch>();
        private long _currentBatchBufferSize = 0;

        #region Debug Helpers

        private static void WriteCloudFetchDebug(string message)
        {
            try
            {

                var debugFile = System.IO.Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "adbc-cloudfetch-debug.log"
                );
                var timestamped = $"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] {message}";
                System.IO.File.AppendAllText(debugFile, timestamped + Environment.NewLine);

            }
            catch
            {
                // Ignore file write errors
            }
        }

        #endregion

        #region Multi-File Batch Aggregation Helpers

        // Removed estimation methods - we now use actual file sizes for batching decisions

        /// <summary>
        /// Concatenates multiple RecordBatch objects into a single batch
        /// Uses Apache Arrow's built-in concatenation functionality
        /// </summary>
        private RecordBatch? ConcatenateBatches(List<RecordBatch> batches)
        {
            if (batches.Count == 0) return null;
            if (batches.Count == 1) return batches[0];

            try
            {
                WriteCloudFetchDebug($"CloudFetchReader: Concatenating {batches.Count} batches using Arrow's built-in functionality");

                // Calculate total rows for validation
                long totalRows = batches.Sum(b => (long)b.Length);
                WriteCloudFetchDebug($"CloudFetchReader: Total rows to concatenate: {totalRows:N0}");

                // Get the schema from the first batch (all batches should have the same schema)
                var schema = batches[0].Schema;

                // Create lists to hold concatenated arrays for each column
                var concatenatedArrays = new IArrowArray[schema.FieldsList.Count];

                for (int columnIndex = 0; columnIndex < schema.FieldsList.Count; columnIndex++)
                {
                    // Collect all arrays for this column from all batches
                    var columnArrays = batches.Select(batch => batch.Column(columnIndex)).ToArray();

                    // Use ArrowArrayConcatenator to concatenate arrays for this column
                    concatenatedArrays[columnIndex] = ArrowArrayConcatenator.Concatenate(columnArrays);
                }

                // Create the final concatenated RecordBatch
                var concatenatedBatch = new RecordBatch(schema, concatenatedArrays, (int)totalRows);

                WriteCloudFetchDebug($"CloudFetchReader: Successfully concatenated {batches.Count} batches into one batch with {concatenatedBatch.Length} rows");

                return concatenatedBatch;
            }
            catch (Exception ex)
            {
                WriteCloudFetchDebug($"CloudFetchReader: Failed to concatenate {batches.Count} batches: {ex.Message}");
                WriteCloudFetchDebug($"CloudFetchReader: Falling back to returning first batch to avoid data loss");

                // Fallback: return first batch if concatenation fails
                // This preserves at least some data rather than losing everything
                return batches[0];
            }
        }

        /// <summary>
        /// Clears the batch buffer and resets size tracking
        /// </summary>
        private void ClearBatchBuffer()
        {
            foreach (var batch in _batchBuffer)
            {
                batch?.Dispose();
            }
            _batchBuffer.Clear();
            _currentBatchBufferSize = 0;
        }

        /// <summary>
        /// Estimates the memory size of a RecordBatch in bytes (decompressed size).
        /// </summary>
        private long EstimateRecordBatchSize(RecordBatch batch)
        {
            long totalSize = 0;

            // Estimate based on column count, row count, and average bytes per value
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                var array = batch.Column(i);
                totalSize += EstimateArraySize(array);
            }

            return totalSize;
        }

        /// <summary>
        /// Estimates the memory size of an IArrowArray in bytes.
        /// </summary>
        private long EstimateArraySize(IArrowArray array)
        {
            // Base overhead per array
            long size = 64; // Base object overhead

            // Add data buffer sizes based on array type and length
            switch (array.Data.DataType.TypeId)
            {
                case ArrowTypeId.Int8:
                    size += array.Length * 1;
                    break;
                case ArrowTypeId.Int16:
                    size += array.Length * 2;
                    break;
                case ArrowTypeId.Int32:
                case ArrowTypeId.Float:
                    size += array.Length * 4;
                    break;
                case ArrowTypeId.Int64:
                case ArrowTypeId.Double:
                case ArrowTypeId.Timestamp:
                    size += array.Length * 8;
                    break;
                case ArrowTypeId.String:
                case ArrowTypeId.Binary:
                    // For string/binary, estimate average string length
                    // This is rough - could be more accurate by examining actual data
                    size += array.Length * 32; // Assume average 32 bytes per string
                    break;
                case ArrowTypeId.Boolean:
                    size += (array.Length + 7) / 8; // Packed bits
                    break;
                default:
                    // Default estimate for unknown types
                    size += array.Length * 8;
                    break;
            }

            // Add validity bitmap if present (1 bit per element)
            if (array.Data.NullCount > 0)
            {
                size += (array.Length + 7) / 8;
            }

            return size;
        }

        #endregion

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
            WriteCloudFetchDebug($"CloudFetchReader constructor called - CloudFetch is being used! (ReadCallCount initialized to: {_readCallCount})");
            WriteCloudFetchDebug($"🗃️ CloudFetchReader: File download counters initialized - TotalFilesDownloaded: {_totalFilesDownloaded}, TotalFileDataProcessed: {_totalFileDataProcessed}");

            // Initialize batch size limit (default 300MB, configurable)
            var connectionProps = statement.Connection.Properties;
            const long DefaultBatchSizeLimitBytes = 20L * 1024 * 1024; // 300MB default for decompressed RecordBatch data
            _batchSizeLimitBytes = DefaultBatchSizeLimitBytes;

            if (connectionProps.TryGetValue("cloudfetch.batch.size.limit.mb", out string? batchSizeLimitStr))
            {
                if (long.TryParse(batchSizeLimitStr, out long batchSizeLimitMB) && batchSizeLimitMB > 0)
                {
                    _batchSizeLimitBytes = batchSizeLimitMB * 1024 * 1024;
                    WriteCloudFetchDebug($"CloudFetchReader: Custom batch size limit configured: {batchSizeLimitMB}MB ({_batchSizeLimitBytes:N0} bytes)");
                }
            }

            WriteCloudFetchDebug($"CloudFetchReader: Multi-file batch aggregation enabled with size limit: {_batchSizeLimitBytes / (1024 * 1024)}MB");

            // CRITICAL FIX: Test using shared ActivitySource instead of TracingReader
            WriteCloudFetchDebug("CloudFetchReader: Testing SHARED ActivitySource that TracerProvider listens for...");
            using var testActivity = DatabricksConnection.s_sharedActivitySource.StartActivity("CloudFetchReader-TracerTest");
            WriteCloudFetchDebug($"CloudFetchReader: SHARED ActivitySource test activity created: {(testActivity != null ? "YES" : "NO")}");
            if (testActivity != null)
            {
                WriteCloudFetchDebug($"CloudFetchReader: SHARED ActivitySource Test Activity ID='{testActivity.Id}', Source='{testActivity.Source?.Name}'");
                testActivity.SetTag("test.message", "CloudFetchReader constructor test via SHARED ActivitySource");
                testActivity.SetTag("test.component", "CloudFetchReader");
                testActivity.SetTag("test.timestamp", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss"));
                testActivity.AddEvent(new ActivityEvent("CloudFetchReader constructor test event"));
                testActivity.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
                WriteCloudFetchDebug($"CloudFetchReader: SHARED ActivitySource test activity status set to: {testActivity.Status}");
            }
            WriteCloudFetchDebug("CloudFetchReader: SHARED ActivitySource test activity completed - should appear in OpenTelemetry trace file!");

            // Check if prefetch is enabled
            //var connectionProps = statement.Connection.Properties;
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
            var callNumber = _readCallCount + 1; // Preview the call number before increment
            WriteCloudFetchDebug($"CloudFetchReader.ReadNextRecordBatchAsync called - CALL #{callNumber}");

            // FIXED: Use shared ActivitySource that TracerProvider is listening for
            using var manualActivity = DatabricksConnection.s_sharedActivitySource.StartActivity("ReadNextRecordBatchAsync");
            WriteCloudFetchDebug($"CloudFetchReader: SHARED Activity {(manualActivity != null ? "CREATED" : "NULL")}");
            if (manualActivity != null)
            {
                WriteCloudFetchDebug($"CloudFetchReader SHARED Activity Details: ID='{manualActivity.Id}', Name='{manualActivity.DisplayName}', Source='{manualActivity.Source?.Name}', Status='{manualActivity.Status}'");
            }

            try
            {
                // Increment read call counter
                var currentCallNumber = System.Threading.Interlocked.Increment(ref _readCallCount);
                var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                WriteCloudFetchDebug($"🚀 [{timestamp}] CloudFetchReader: EXTERNAL CALL #{currentCallNumber} STARTED (this may internally consume multiple queue items for batch aggregation)");

                // Set initial activity tags (only if activity exists)
                manualActivity?.SetTag("cloudfetch.reader.call_number", currentCallNumber);
                manualActivity?.SetTag("cloudfetch.reader.total_calls_so_far", currentCallNumber);
                manualActivity?.SetTag("cloudfetch.reader.total_batches_returned_so_far", _totalBatchesReturned);
                manualActivity?.SetTag("cloudfetch.reader.total_rows_returned_so_far", _totalRowsReturned);
                manualActivity?.SetTag("cloudfetch.reader.total_bytes_read_so_far", _totalBytesRead);
                manualActivity?.SetTag("cloudfetch.reader.total_files_downloaded_so_far", _totalFilesDownloaded);
                manualActivity?.SetTag("cloudfetch.reader.total_file_data_processed_so_far", _totalFileDataProcessed);

                ThrowIfDisposed();

                int internalLoopIteration = 0;
                int filesConsumedThisCall = 0;
                int batchesProcessedThisCall = 0;
                WriteCloudFetchDebug($"🔄 CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - Starting internal loop to collect files until {_batchSizeLimitBytes / 1024 / 1024}MB limit");

                while (true)
                {
                    internalLoopIteration++;
                    var loopTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    WriteCloudFetchDebug($"🔄 [{loopTimestamp}] CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - INTERNAL LOOP ITERATION #{internalLoopIteration} - current_reader={(this.currentReader != null ? "EXISTS" : "NULL")}, buffer_count={_batchBuffer.Count}");
                    // If we have a current reader, try to read the next batch
                    if (this.currentReader != null)
                    {
                        WriteCloudFetchDebug($"CloudFetchReader: INTERNAL LOOP #{internalLoopIteration} - Reading from EXISTING stream (this.currentReader != null)");
                        manualActivity?.AddEvent("cloudfetch.reader.reading_from_current_stream");
                        RecordBatch? next = await this.currentReader.ReadNextRecordBatchAsync(cancellationToken);
                        if (next != null)
                        {
                            // Calculate batch metrics
                            var batchRowCount = next.Length;
                            var actualDecompressedFileSize = this.currentDownloadResult?.Size ?? 0;

                            // CRITICAL FIX: Use ACTUAL decompressed file size for comparison, but estimate individual RecordBatch size
                            // Note: We still need to estimate individual RecordBatch size since we're aggregating RecordBatch objects, not files
                            var estimatedRecordBatchSize = EstimateRecordBatchSize(next);
                            batchesProcessedThisCall++;
                            WriteCloudFetchDebug($"📊 CloudFetchReader: Got batch #{batchesProcessedThisCall} with {batchRowCount:N0} rows");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: TOTAL FILE size (ACTUAL decompressed): {actualDecompressedFileSize:N0} bytes ({actualDecompressedFileSize / 1024.0 / 1024.0:F1}MB)");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: SINGLE RecordBatch size (estimated): {estimatedRecordBatchSize:N0} bytes ({estimatedRecordBatchSize / 1024.0 / 1024.0:F1}MB)");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: File contains approximately {(actualDecompressedFileSize > 0 ? (double)actualDecompressedFileSize / estimatedRecordBatchSize : 0):F1} RecordBatch objects");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: ✅ ACTUAL vs ESTIMATED - File: {actualDecompressedFileSize:N0} bytes, This RecordBatch: {estimatedRecordBatchSize:N0} bytes");

                            // Add to batch buffer for aggregation until we reach configured size limit
                            var bufferCountBefore = _batchBuffer.Count;
                            var bufferSizeBefore = _currentBatchBufferSize;
                            var bufferTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                            WriteCloudFetchDebug($"🔄 [{bufferTimestamp}] CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - BATCH #{batchesProcessedThisCall} - About to ADD RecordBatch to batch buffer - buffer_count_before={bufferCountBefore}, buffer_size_before={bufferSizeBefore:N0} bytes");

                            _batchBuffer.Add(next);
                            _currentBatchBufferSize += estimatedRecordBatchSize; // FIXED: Use estimated RecordBatch size for individual batch aggregation

                            var bufferCountAfter = _batchBuffer.Count;
                            var bufferSizeAfter = _currentBatchBufferSize;
                            WriteCloudFetchDebug($"✅ CloudFetchReader: RecordBatch ADDED to batch buffer - buffer_before={bufferCountBefore}, buffer_after={bufferCountAfter}, size_before={bufferSizeBefore:N0}, size_after={bufferSizeAfter:N0} bytes");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: *** PROOF: Result queue item was CONSUMED and stored in batch buffer for aggregation ***");
                            WriteCloudFetchDebug($"CloudFetchReader: Buffer now has {_batchBuffer.Count} batches, {_currentBatchBufferSize:N0} bytes (limit: {_batchSizeLimitBytes:N0} bytes)");

                            // Check if we've reached the size limit
                            WriteCloudFetchDebug($"🎯 CloudFetchReader: Size limit check - current: {_currentBatchBufferSize:N0} bytes vs limit: {_batchSizeLimitBytes:N0} bytes, exceeded: {(_currentBatchBufferSize >= _batchSizeLimitBytes)}");
                            if (_currentBatchBufferSize >= _batchSizeLimitBytes)
                            {
                                var concatTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                                WriteCloudFetchDebug($"🔗 [{concatTimestamp}] CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - Size limit reached! Starting CONCATENATION of {_batchBuffer.Count} batches (this may cause result queue backup)");

                                // Create aggregated batch from all buffered batches
                                var aggregatedBatch = ConcatenateBatches(_batchBuffer);

                                if (aggregatedBatch != null)
                                {
                                    var aggregatedRowCount = aggregatedBatch.Length;
                                    var completionTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");

                                    WriteCloudFetchDebug($"✅ [{completionTimestamp}] CloudFetchReader: EXTERNAL CALL #{currentCallNumber} COMPLETED ✅ | Aggregated {_batchBuffer.Count} files into {aggregatedRowCount:N0} rows from {_currentBatchBufferSize:N0} bytes");
                                    WriteCloudFetchDebug($"📊 CloudFetchReader: EXTERNAL CALL #{currentCallNumber} FINAL STATISTICS: files_consumed_from_queue={filesConsumedThisCall}, batches_processed={batchesProcessedThisCall}, internal_loop_iterations={internalLoopIteration}");
                                    WriteCloudFetchDebug($"🗃️ CloudFetchReader: EXTERNAL CALL #{currentCallNumber} GLOBAL IMPACT: total_files_downloaded_so_far={_totalFilesDownloaded:N0}, total_file_data_processed_so_far={_totalFileDataProcessed / 1024.0 / 1024.0:F1}MB");

                                    // Update counters for the aggregated batch
                                    System.Threading.Interlocked.Increment(ref _totalBatchesReturned);
                                    System.Threading.Interlocked.Add(ref _totalRowsReturned, aggregatedRowCount);
                                    System.Threading.Interlocked.Add(ref _totalBytesRead, _currentBatchBufferSize); // This is now total file sizes

                                    // Add aggregated batch activity tags
                                    manualActivity?.SetTag("cloudfetch.reader.result", "aggregated_batch_returned");
                                    manualActivity?.SetTag("cloudfetch.reader.aggregated_batch_row_count", aggregatedRowCount);
                                    manualActivity?.SetTag("cloudfetch.reader.aggregated_from_files", _batchBuffer.Count);
                                    manualActivity?.SetTag("cloudfetch.reader.aggregated_file_size_bytes", _currentBatchBufferSize);
                                    manualActivity?.SetTag("cloudfetch.reader.batch_column_count", aggregatedBatch.ColumnCount);
                                    manualActivity?.SetTag("cloudfetch.reader.new_total_batches", _totalBatchesReturned);
                                    manualActivity?.SetTag("cloudfetch.reader.new_total_rows", _totalRowsReturned);
                                    manualActivity?.SetTag("cloudfetch.reader.new_total_bytes", _totalBytesRead);

                                    manualActivity?.AddEvent($"cloudfetch.reader.aggregated_batch_returned: {aggregatedRowCount:N0} rows from {_batchBuffer.Count} files ({_currentBatchBufferSize:N0} bytes)");

                                    // Clear buffer for next aggregation
                                    ClearBatchBuffer();

                                    WriteCloudFetchDebug($"CloudFetchReader: Batch aggregation COMPLETED - returning {aggregatedRowCount:N0} rows to consumer, result queue should now start draining faster");

                                    // Set activity status
                                    if (manualActivity != null)
                                    {
                                        manualActivity.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
                                    }

                                    return aggregatedBatch;
                                }
                            }

                            // Size limit not reached yet, continue collecting more batches
                            WriteCloudFetchDebug($"📈 CloudFetchReader: Size limit NOT reached ({_currentBatchBufferSize:N0} < {_batchSizeLimitBytes:N0} bytes), continuing to collect more batches...");
                            WriteCloudFetchDebug($"🔄 CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - Will continue internal loop to get NEXT file from queue");
                            continue;
                        }
                        else
                        {
                            WriteCloudFetchDebug($"💀 CloudFetchReader: INTERNAL LOOP #{internalLoopIteration} - Current stream EXHAUSTED (returned null) - disposing current reader");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: *** File #{filesConsumedThisCall} contained {batchesProcessedThisCall} RecordBatch objects - will need to get NEXT file from result queue ***");
                            WriteCloudFetchDebug($"🔄 CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - File #{filesConsumedThisCall} processing complete, buffer_size={_currentBatchBufferSize:N0}, limit={_batchSizeLimitBytes:N0}");
                            manualActivity?.AddEvent("cloudfetch.reader.current_stream_exhausted");

                            // Clean up the current reader and download result
                            WriteCloudFetchDebug($"🧹 CloudFetchReader: INTERNAL LOOP #{internalLoopIteration} - Disposing current reader and download result");
                            this.currentReader.Dispose();
                            this.currentReader = null;

                            if (this.currentDownloadResult != null)
                            {
                                this.currentDownloadResult.Dispose();
                                this.currentDownloadResult = null;
                            }

                            WriteCloudFetchDebug($"✅ CloudFetchReader: INTERNAL LOOP #{internalLoopIteration} - Cleanup complete, currentReader=NULL, currentDownloadResult=NULL");
                            WriteCloudFetchDebug($"🤔 CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - Should we continue or return? buffer_size={_currentBatchBufferSize:N0} vs limit={_batchSizeLimitBytes:N0}");

                            // CRITICAL: Check if we have accumulated data in buffer AND have reached size limit
                            if (_batchBuffer.Count > 0 && _currentBatchBufferSize >= _batchSizeLimitBytes)
                            {
                                WriteCloudFetchDebug($"🔗 CloudFetchReader: Stream exhausted AND size limit reached - we have {_batchBuffer.Count} batches in buffer ({_currentBatchBufferSize:N0} bytes >= {_batchSizeLimitBytes:N0}) - returning aggregated batch");

                                // Create aggregated batch from all buffered batches
                                var aggregatedBatch = ConcatenateBatches(_batchBuffer);

                                if (aggregatedBatch != null)
                                {
                                    var aggregatedRowCount = aggregatedBatch.Length;

                                    WriteCloudFetchDebug($"CloudFetchReader: CALL #{currentCallNumber} COMPLETED ✅ | Aggregated {_batchBuffer.Count} remaining files into {aggregatedRowCount:N0} rows from {_currentBatchBufferSize:N0} bytes");

                                    // Update counters for the aggregated batch
                                    System.Threading.Interlocked.Increment(ref _totalBatchesReturned);
                                    System.Threading.Interlocked.Add(ref _totalRowsReturned, aggregatedRowCount);
                                    System.Threading.Interlocked.Add(ref _totalBytesRead, _currentBatchBufferSize);

                                    // Add aggregated batch activity tags
                                    manualActivity?.SetTag("cloudfetch.reader.result", "final_aggregated_batch_returned");
                                    manualActivity?.SetTag("cloudfetch.reader.aggregated_batch_row_count", aggregatedRowCount);
                                    manualActivity?.SetTag("cloudfetch.reader.aggregated_from_files", _batchBuffer.Count);
                                    manualActivity?.SetTag("cloudfetch.reader.aggregated_file_size_bytes", _currentBatchBufferSize);
                                    manualActivity?.SetTag("cloudfetch.reader.batch_column_count", aggregatedBatch.ColumnCount);
                                    manualActivity?.SetTag("cloudfetch.reader.new_total_batches", _totalBatchesReturned);
                                    manualActivity?.SetTag("cloudfetch.reader.new_total_rows", _totalRowsReturned);
                                    manualActivity?.SetTag("cloudfetch.reader.new_total_bytes", _totalBytesRead);

                                    manualActivity?.AddEvent($"cloudfetch.reader.final_aggregated_batch_returned: {aggregatedRowCount:N0} rows from {_batchBuffer.Count} files ({_currentBatchBufferSize:N0} bytes)");

                                    // Clear buffer for next aggregation
                                    ClearBatchBuffer();

                                    // Set activity status
                                    if (manualActivity != null)
                                    {
                                        manualActivity.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
                                    }

                                    return aggregatedBatch;
                                }
                                else
                                {
                                    // If concatenation failed, clear buffer and continue
                                    WriteCloudFetchDebug("CloudFetchReader: Failed to concatenate remaining batches, clearing buffer");
                                    ClearBatchBuffer();
                                }
                            }
                            else if (_batchBuffer.Count > 0)
                            {
                                WriteCloudFetchDebug($"🔄 CloudFetchReader: Stream exhausted but size limit NOT reached ({_currentBatchBufferSize:N0} < {_batchSizeLimitBytes:N0}) - continuing to get NEXT file from queue");
                                WriteCloudFetchDebug($"📊 CloudFetchReader: Current progress: {_batchBuffer.Count} batches, {_currentBatchBufferSize:N0} bytes - need {_batchSizeLimitBytes - _currentBatchBufferSize:N0} more bytes");
                                // Continue the loop to get the next file
                            }
                            else
                            {
                                WriteCloudFetchDebug($"🤷 CloudFetchReader: Stream exhausted with empty buffer - continuing to get NEXT file from queue");
                                // Continue the loop to get the next file
                            }
                        }
                    }

                    // If we don't have a current reader, get the next downloaded file
                    if (this.downloadManager != null)
                    {
                        WriteCloudFetchDebug($"🔄 CloudFetchReader: INTERNAL LOOP #{internalLoopIteration} - NO current reader, need to get NEXT file from result queue");
                        // Start the download manager if it's not already started
                        if (!this.isPrefetchEnabled)
                        {
                            manualActivity?.SetTag("cloudfetch.reader.error", "prefetch_disabled");
                            throw new InvalidOperationException("Prefetch must be enabled.");
                        }

                        try
                        {
                            WriteCloudFetchDebug("CloudFetchReader: Requesting next downloaded file from manager");
                            manualActivity?.AddEvent("cloudfetch.reader.requesting_next_downloaded_file");

                            // Get the next downloaded file - THIS IS WHERE QUEUE ITEMS ARE CONSUMED FOR BATCH BUILDING
                            var internalTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                            WriteCloudFetchDebug($"🔄 [{internalTimestamp}] CloudFetchReader: EXTERNAL CALL #{currentCallNumber} - INTERNAL LOOP requesting next file from result queue for BATCH AGGREGATION");
                            this.currentDownloadResult = await this.downloadManager.GetNextDownloadedFileAsync(cancellationToken);

                            if (this.currentDownloadResult == null)
                            {
                                WriteCloudFetchDebug($"📊 CloudFetchReader: *** PROOF: Item was CONSUMED from result queue for batch building immediately (null = EndOfResultsGuard) ***");
                                WriteCloudFetchDebug($"CloudFetchReader: CALL #{currentCallNumber} - No more files available (EndOfResultsGuard.Instance was encountered)");

                                // Check if we have any buffered batches to return as final aggregated batch
                                if (_batchBuffer.Count > 0)
                                {
                                    WriteCloudFetchDebug($"CloudFetchReader: ⚪ FINAL BATCH AGGREGATION - Returning final aggregated batch from {_batchBuffer.Count} buffered files ({_currentBatchBufferSize:N0} bytes) - PREVENTS DATA LOSS!");
                                    WriteCloudFetchDebug($"CloudFetchReader: ⚪ This handles the case where we have < {_batchSizeLimitBytes / 1024 / 1024}MB remaining data at the end of results");

                                    // Return the final aggregated batch
                                    var finalBatch = ConcatenateBatches(_batchBuffer);
                                    if (finalBatch != null)
                                    {
                                        var finalRowCount = finalBatch.Length;

                                        WriteCloudFetchDebug($"CloudFetchReader: CALL #{currentCallNumber} COMPLETED ✅ | Final aggregated batch: {finalRowCount:N0} rows from {_batchBuffer.Count} files ({_currentBatchBufferSize:N0} bytes)");

                                        // Update counters
                                        System.Threading.Interlocked.Increment(ref _totalBatchesReturned);
                                        System.Threading.Interlocked.Add(ref _totalRowsReturned, finalRowCount);
                                        System.Threading.Interlocked.Add(ref _totalBytesRead, _currentBatchBufferSize); // This is now total file sizes

                                        // Add final batch activity tags
                                        manualActivity?.SetTag("cloudfetch.reader.result", "final_aggregated_batch_returned");
                                        manualActivity?.SetTag("cloudfetch.reader.final_batch_row_count", finalRowCount);
                                        manualActivity?.SetTag("cloudfetch.reader.final_aggregated_from_files", _batchBuffer.Count);
                                        manualActivity?.SetTag("cloudfetch.reader.final_aggregated_file_size_bytes", _currentBatchBufferSize);
                                        manualActivity?.SetTag("cloudfetch.reader.batch_column_count", finalBatch.ColumnCount);
                                        manualActivity?.SetTag("cloudfetch.reader.new_total_batches", _totalBatchesReturned);
                                        manualActivity?.SetTag("cloudfetch.reader.new_total_rows", _totalRowsReturned);
                                        manualActivity?.SetTag("cloudfetch.reader.new_total_bytes", _totalBytesRead);

                                        manualActivity?.AddEvent($"cloudfetch.reader.final_batch_returned: {finalRowCount:N0} rows from {_batchBuffer.Count} final files ({_currentBatchBufferSize:N0} bytes)");

                                        // Clear buffer
                                        ClearBatchBuffer();

                                        // Set activity status
                                        if (manualActivity != null)
                                        {
                                            manualActivity.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
                                        }

                                        return finalBatch;
                                    }
                                }

                                WriteCloudFetchDebug($"CloudFetchReader: END OF RESULTS 🏁");
                                WriteCloudFetchDebug($"🏆 CloudFetchReader: FINAL SUMMARY - Total ReadNextRecordBatchAsync calls: {currentCallNumber}, Total batches returned: {_totalBatchesReturned}, Total rows: {_totalRowsReturned:N0}");
                                WriteCloudFetchDebug($"🗃️ CloudFetchReader: FINAL FILE STATISTICS - Total files downloaded: {_totalFilesDownloaded:N0}, Total file data processed: {_totalFileDataProcessed:N0} bytes ({_totalFileDataProcessed / 1024.0 / 1024.0:F1}MB)");
                                WriteCloudFetchDebug($"📊 CloudFetchReader: FILE PROCESSING EFFICIENCY - Average downloaded file size: {(_totalFilesDownloaded > 0 ? _totalFileDataProcessed / _totalFilesDownloaded / 1024.0 / 1024.0 : 0):F1}MB, Total RecordBatch data aggregated: {_totalBytesRead:N0} bytes ({_totalBytesRead / 1024.0 / 1024.0:F1}MB)");
                                manualActivity?.SetTag("cloudfetch.reader.result", "no_more_files");
                                manualActivity?.AddEvent("cloudfetch.reader.end_of_results");
                                manualActivity?.SetTag("cloudfetch.reader.final_call_count", currentCallNumber);
                                manualActivity?.SetTag("cloudfetch.reader.final_total_batches", _totalBatchesReturned);
                                manualActivity?.SetTag("cloudfetch.reader.final_total_rows", _totalRowsReturned);
                                manualActivity?.SetTag("cloudfetch.reader.final_total_bytes", _totalBytesRead);
                                manualActivity?.SetTag("cloudfetch.reader.final_total_files_downloaded", _totalFilesDownloaded);
                                manualActivity?.SetTag("cloudfetch.reader.final_total_file_data_processed", _totalFileDataProcessed);

                                // Set final activity status
                                if (manualActivity != null)
                                {
                                    WriteCloudFetchDebug($"CloudFetchReader: Setting final activity status to OK");
                                    manualActivity.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
                                    WriteCloudFetchDebug($"CloudFetchReader: Final activity status: {manualActivity.Status}");
                                }

                                WriteCloudFetchDebug($"CloudFetchReader: Attempting to trigger OpenTelemetry export by sleeping 100ms...");
                                await System.Threading.Tasks.Task.Delay(100, cancellationToken);

                                this.downloadManager.Dispose();
                                this.downloadManager = null;
                                // No more files
                                return null;
                            }

                            filesConsumedThisCall++;
                            var totalFilesDownloaded = System.Threading.Interlocked.Increment(ref _totalFilesDownloaded);
                            var totalFileDataProcessed = System.Threading.Interlocked.Add(ref _totalFileDataProcessed, this.currentDownloadResult.Size);

                            WriteCloudFetchDebug($"🎯 CloudFetchReader: FILE #{filesConsumedThisCall} RECEIVED for BATCH BUILDING - size={this.currentDownloadResult.Size:N0} bytes ({this.currentDownloadResult.Size / 1024.0 / 1024.0:F1}MB downloaded)");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: *** PROOF: Item #{filesConsumedThisCall} was CONSUMED from result queue for batch building immediately (received non-null item) ***");
                            WriteCloudFetchDebug($"📊 CloudFetchReader: EXTERNAL CALL #{currentCallNumber} statistics so far: files_consumed={filesConsumedThisCall}, batches_processed={batchesProcessedThisCall}, buffer_count={_batchBuffer.Count}");
                            WriteCloudFetchDebug($"🗃️ CloudFetchReader: GLOBAL FILE STATISTICS - Total files downloaded: {totalFilesDownloaded:N0}, Total file data processed: {totalFileDataProcessed:N0} bytes ({totalFileDataProcessed / 1024.0 / 1024.0:F1}MB)");
                            manualActivity?.AddEvent("cloudfetch.reader.new_download_received");
                            manualActivity?.SetTag("cloudfetch.reader.download_size", this.currentDownloadResult.Size);

                            await this.currentDownloadResult.DownloadCompletedTask;

                            // Create a new reader for the downloaded file
                            try
                            {
                                manualActivity?.AddEvent("cloudfetch.reader.creating_arrow_stream_reader");
                                this.currentReader = new ArrowStreamReader(this.currentDownloadResult.DataStream);
                                manualActivity?.AddEvent("cloudfetch.reader.arrow_stream_reader_created");
                                continue;
                            }
                            catch (Exception ex)
                            {
                                manualActivity?.SetTag("cloudfetch.reader.error", "arrow_reader_creation_failed");
                                manualActivity?.SetTag("cloudfetch.reader.error_message", ex.Message);
                                manualActivity?.AddEvent($"cloudfetch.reader.error: Failed to create Arrow reader - {ex.Message}");

                                Debug.WriteLine($"Error creating Arrow reader: {ex.Message}");
                                this.currentDownloadResult.Dispose();
                                this.currentDownloadResult = null;
                                throw;
                            }
                        }
                        catch (Exception ex) when (!(ex is InvalidOperationException && ex.Message.Contains("Arrow reader")))
                        {
                            manualActivity?.SetTag("cloudfetch.reader.error", "download_failed");
                            manualActivity?.SetTag("cloudfetch.reader.error_message", ex.Message);
                            manualActivity?.AddEvent($"cloudfetch.reader.error: Failed to get downloaded file - {ex.Message}");

                            Debug.WriteLine($"Error getting next downloaded file: {ex.Message}");
                            throw;
                        }
                    }

                    // If we get here, there are no more files
                    manualActivity?.SetTag("cloudfetch.reader.result", "no_download_manager");
                    manualActivity?.AddEvent("cloudfetch.reader.no_more_results");
                    return null;
                }
            }
            catch (Exception ex)
            {
                WriteCloudFetchDebug($"CloudFetchReader: Exception in call #{callNumber}: {ex.Message}");
                manualActivity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                manualActivity?.SetTag("cloudfetch.reader.exception", ex.Message);
                throw;
            }
            finally
            {
                if (manualActivity != null)
                {
                    WriteCloudFetchDebug($"CloudFetchReader: Activity completed with status: {manualActivity.Status}");
                    if (manualActivity.Status == System.Diagnostics.ActivityStatusCode.Unset)
                    {
                        manualActivity.SetStatus(System.Diagnostics.ActivityStatusCode.Ok);
                    }
                }
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

            // Clean up batch buffer
            if (disposing)
            {
                ClearBatchBuffer();
            }

            base.Dispose(disposing);
        }
    }
}

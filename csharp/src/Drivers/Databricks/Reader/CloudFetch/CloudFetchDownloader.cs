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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Databricks;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Downloads files from URLs.
    /// </summary>
    internal sealed class CloudFetchDownloader : ICloudFetchDownloader
    {
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly BlockingCollection<IDownloadResult> _resultQueue;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly HttpClient _httpClient;
        private readonly ICloudFetchResultFetcher _resultFetcher;
        private readonly int _maxParallelDownloads;
        private readonly bool _isLz4Compressed;
        private readonly int _maxRetries;
        private readonly int _retryDelayMs;
        private readonly int _maxUrlRefreshAttempts;
        private readonly int _urlExpirationBufferSeconds;
        private readonly SemaphoreSlim _downloadSemaphore;
        private Task? _downloadTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private bool _isCompleted;
        private Exception? _error;
        private readonly object _errorLock = new object();

        // Instance tracking for debugging
        private static int s_instanceCounter = 0;
        private readonly int _instanceId;

        #region Debug Helpers

        private void WriteCloudFetchDebug(string message)
        {
            try
            {

                var debugFile = System.IO.Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "adbc-cloudfetch-debug.log"
                );
                var timestamped = $"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] [INSTANCE-{_instanceId}] {message}";
                System.IO.File.AppendAllText(debugFile, timestamped + Environment.NewLine);

            }
            catch
            {
                // Ignore file write errors
            }
        }

        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloader"/> class.
        /// </summary>
        /// <param name="downloadQueue">The queue of downloads to process.</param>
        /// <param name="resultQueue">The queue to add completed downloads to.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="httpClient">The HTTP client to use for downloads.</param>
        /// <param name="resultFetcher">The result fetcher that manages URLs.</param>
        /// <param name="maxParallelDownloads">The maximum number of parallel downloads.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        /// <param name="maxRetries">The maximum number of retry attempts.</param>
        /// <param name="retryDelayMs">The delay between retry attempts in milliseconds.</param>
        /// <param name="maxUrlRefreshAttempts">The maximum number of URL refresh attempts.</param>
        /// <param name="urlExpirationBufferSeconds">Buffer time in seconds before URL expiration to trigger refresh.</param>
        public CloudFetchDownloader(
            BlockingCollection<IDownloadResult> downloadQueue,
            BlockingCollection<IDownloadResult> resultQueue,
            ICloudFetchMemoryBufferManager memoryManager,
            HttpClient httpClient,
            ICloudFetchResultFetcher resultFetcher,
            int maxParallelDownloads,
            bool isLz4Compressed,
            int maxRetries = 3,
            int retryDelayMs = 500,
            int maxUrlRefreshAttempts = 3,
            int urlExpirationBufferSeconds = 60)
        {
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _resultQueue = resultQueue ?? throw new ArgumentNullException(nameof(resultQueue));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _resultFetcher = resultFetcher ?? throw new ArgumentNullException(nameof(resultFetcher));
            _maxParallelDownloads = maxParallelDownloads > 0 ? maxParallelDownloads : throw new ArgumentOutOfRangeException(nameof(maxParallelDownloads));
            _isLz4Compressed = isLz4Compressed;
            _maxRetries = maxRetries > 0 ? maxRetries : throw new ArgumentOutOfRangeException(nameof(maxRetries));
            _retryDelayMs = retryDelayMs > 0 ? retryDelayMs : throw new ArgumentOutOfRangeException(nameof(retryDelayMs));
            _maxUrlRefreshAttempts = maxUrlRefreshAttempts > 0 ? maxUrlRefreshAttempts : throw new ArgumentOutOfRangeException(nameof(maxUrlRefreshAttempts));
            _urlExpirationBufferSeconds = urlExpirationBufferSeconds > 0 ? urlExpirationBufferSeconds : throw new ArgumentOutOfRangeException(nameof(urlExpirationBufferSeconds));
            _downloadSemaphore = new SemaphoreSlim(_maxParallelDownloads, _maxParallelDownloads);
            _isCompleted = false;

            // Initialize instance tracking
            _instanceId = System.Threading.Interlocked.Increment(ref s_instanceCounter);

            WriteCloudFetchDebug($"CloudFetchDownloader constructor: Instance #{_instanceId} initialized with {_maxParallelDownloads} parallel downloads");
            WriteCloudFetchDebug($"CloudFetchDownloader constructor: Instance #{_instanceId} - Semaphore created with initial={_maxParallelDownloads}, max={_maxParallelDownloads}, current_available={_downloadSemaphore.CurrentCount}");
        }

        /// <inheritdoc />
        public bool IsCompleted => _isCompleted;

        /// <inheritdoc />
        public bool HasError => _error != null;

        /// <inheritdoc />
        public Exception? Error => _error;

        /// <inheritdoc />
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_downloadTask != null)
            {
                throw new InvalidOperationException("Downloader is already running.");
            }

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _downloadTask = DownloadFilesAsync(_cancellationTokenSource.Token);

            // Wait for the download task to start
            await Task.Yield();
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            if (_downloadTask == null)
            {
                return;
            }

            _cancellationTokenSource?.Cancel();

            try
            {
                await _downloadTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error stopping downloader: {ex.Message}");
            }
            finally
            {
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
                _downloadTask = null;
            }
        }

        /// <inheritdoc />
        public async Task<IDownloadResult?> GetNextDownloadedFileAsync(CancellationToken cancellationToken)
        {
            var queueCountBefore = _resultQueue.Count;
            var takeTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            WriteCloudFetchDebug($"🔄 [{takeTimestamp}] [INSTANCE-{_instanceId}] CloudFetchDownloader.GetNextDownloadedFileAsync called - queue_count_before_take={queueCountBefore}, queue_capacity={_resultQueue.BoundedCapacity}");

            // CRITICAL DEBUGGING: Check producer status when consumer is waiting
            WriteCloudFetchDebug($"🕵️ [INSTANCE-{_instanceId}] PRODUCER DEBUG - download_queue_count={_downloadQueue.Count}, download_queue_completed={_downloadQueue.IsCompleted}");
            WriteCloudFetchDebug($"🕵️ [INSTANCE-{_instanceId}] PRODUCER DEBUG - result_queue_count={_resultQueue.Count}, result_queue_completed={_resultQueue.IsCompleted}");
            WriteCloudFetchDebug($"🕵️ [INSTANCE-{_instanceId}] PRODUCER DEBUG - downloader_completed={_isCompleted}, has_error={HasError}");
            if (HasError && _error != null)
            {
                WriteCloudFetchDebug($"🕵️ [INSTANCE-{_instanceId}] PRODUCER DEBUG - error_message={_error.Message}");
            }

            // LIFECYCLE ANALYSIS: Diagnose completion state
            if (_isCompleted && _resultQueue.Count == 0)
            {
                if (_resultQueue.IsCompleted)
                {
                    WriteCloudFetchDebug($"✅ [INSTANCE-{_instanceId}] LIFECYCLE: Downloader completed AND result queue completed - future calls should return null immediately");
                }
                else
                {
                    WriteCloudFetchDebug($"🚨 [INSTANCE-{_instanceId}] LIFECYCLE BUG: Downloader completed BUT result queue NOT completed - this causes hangs!");
                }
            }

            try
            {
                // Check if there's an error before trying to take from the queue
                if (HasError)
                {
                    throw new AdbcException("Error in download process", _error ?? new Exception("Unknown error"));
                }

                var takeStartTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                WriteCloudFetchDebug($"📤 [{takeStartTimestamp}] [INSTANCE-{_instanceId}] CloudFetchDownloader: About to TAKE item from result queue - queue_count={_resultQueue.Count}, queue_completed={_resultQueue.IsCompleted}");

                if (_resultQueue.Count == 0)
                {
                    WriteCloudFetchDebug($"⏳ [{takeStartTimestamp}] [INSTANCE-{_instanceId}] *** CONSUMER BLOCKING: Queue is empty - Take() will wait until producer adds items ***");
                    WriteCloudFetchDebug($"💭 [INSTANCE-{_instanceId}] CONSUMER BLOCKING ANALYSIS:");
                    WriteCloudFetchDebug($"   📥 Download Queue: count={_downloadQueue.Count}, completed={_downloadQueue.IsCompleted}");
                    WriteCloudFetchDebug($"   📤 Result Queue: count={_resultQueue.Count}, completed={_resultQueue.IsCompleted}");
                    WriteCloudFetchDebug($"   📊 Downloader: completed={_isCompleted}, has_error={HasError}");
                    WriteCloudFetchDebug($"   🔧 Semaphore: available={_downloadSemaphore.CurrentCount}, max={_maxParallelDownloads}");

                    if (_downloadQueue.IsCompleted && _resultQueue.Count == 0 && !_resultQueue.IsCompleted)
                    {
                        WriteCloudFetchDebug($"🚨 [INSTANCE-{_instanceId}] POTENTIAL DEADLOCK: Download queue completed but result queue not completed and empty!");
                        WriteCloudFetchDebug($"🚨 [INSTANCE-{_instanceId}] This suggests EndOfResultsGuard was never added to result queue!");
                    }
                }

                // Try to take the next result from the queue - THIS IS WHERE ITEMS ARE REMOVED FROM QUEUE
                IDownloadResult result = await Task.Run(() => _resultQueue.Take(cancellationToken), cancellationToken);

                var queueCountAfter = _resultQueue.Count;
                var takeCompleteTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                WriteCloudFetchDebug($"✅ [{takeCompleteTimestamp}] [INSTANCE-{_instanceId}] CloudFetchDownloader: ITEM SUCCESSFULLY TAKEN from result queue - queue_before={queueCountBefore}, queue_after={queueCountAfter}, items_removed={(queueCountBefore - queueCountAfter)}, size={result.Size}");

                if (queueCountBefore == 0)
                {
                    WriteCloudFetchDebug($"🎯 [{takeCompleteTimestamp}] [INSTANCE-{_instanceId}] *** CONSUMER UNBLOCKED: Producer added item to empty queue - consumer can now proceed ***");
                }

                // Check if this is the end of results guard
                if (result == EndOfResultsGuard.Instance)
                {
                    _isCompleted = true;
                    WriteCloudFetchDebug("CloudFetchDownloader GetNext: ⚪ EndOfResultsGuard.Instance encountered - signaling end of results to CloudFetchReader");
                    WriteCloudFetchDebug("CloudFetchDownloader GetNext: ⚪ CloudFetchReader will now return any accumulated batch buffer as final aggregated batch");

                    // CRITICAL FIX: Mark result queue as completed so subsequent calls return immediately
                    WriteCloudFetchDebug("CloudFetchDownloader GetNext: 🔒 Marking result queue as COMPLETED - future calls will return null immediately");
                    _resultQueue.CompleteAdding();

                    return null;
                }

                WriteCloudFetchDebug($"🎯 CloudFetchDownloader: Returning item to CloudFetchReader for BATCH BUILDING - size={result.Size}, queue_remaining={_resultQueue.Count}");
                WriteCloudFetchDebug($"📊 CloudFetchDownloader: *** PROOF: Item was REMOVED from result queue IMMEDIATELY when consumed for batch aggregation ***");
                return result;
            }
            catch (OperationCanceledException)
            {
                WriteCloudFetchDebug("CloudFetchDownloader GetNext: Operation cancelled");
                return null;
            }
            catch (InvalidOperationException) when (_resultQueue.IsCompleted)
            {
                _isCompleted = true;
                WriteCloudFetchDebug("CloudFetchDownloader GetNext: ✅ Queue completed and empty - no more results available");
                return null;
            }
            catch (AdbcException)
            {
                WriteCloudFetchDebug("CloudFetchDownloader GetNext: ADBC error occurred");
                throw;
            }
            catch (Exception ex)
            {
                WriteCloudFetchDebug($"CloudFetchDownloader GetNext: Exception - {ex.Message}");
                SetError(ex);
                throw;
            }
        }

        private async Task DownloadFilesAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();

            var startTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            WriteCloudFetchDebug($"🚀 [{startTimestamp}] CloudFetchDownloader.DownloadFilesAsync STARTED");
            WriteCloudFetchDebug($"📊 [{startTimestamp}] CloudFetchDownloader: PIPELINE ENTRY - download_queue={_downloadQueue.Count}, result_queue={_resultQueue.Count}, semaphore_available={_downloadSemaphore.CurrentCount}");
            WriteCloudFetchDebug($"📊 [{startTimestamp}] CloudFetchDownloader: Instance #{_instanceId} - PIPELINE STATUS CHECK - max_parallel={_maxParallelDownloads}, memory_max={_memoryManager.MaxMemory / 1024 / 1024}MB, memory_used={_memoryManager.UsedMemory / 1024 / 1024}MB");
            WriteCloudFetchDebug($"🎯 [{startTimestamp}] CloudFetchDownloader: *** PRODUCER PIPELINE STARTING - Will add items to result queue for consumer to take ***");

            try
            {
                WriteCloudFetchDebug($"CloudFetchDownloader: About to call DownloadFilesInternalAsync - download_queue={_downloadQueue.Count}");
                await DownloadFilesInternalAsync(cancellationToken);
                WriteCloudFetchDebug($"CloudFetchDownloader: DownloadFilesInternalAsync completed - download_queue={_downloadQueue.Count}, result_queue={_resultQueue.Count}");

                WriteCloudFetchDebug("CloudFetchDownloader Download: Files download completed successfully");
                WriteCloudFetchDebug($"CloudFetchDownloader: Batch completed - semaphore_available={_downloadSemaphore.CurrentCount}, max_parallel={_maxParallelDownloads}, download_queue={_downloadQueue.Count}, result_queue={_resultQueue.Count}");
            }
            catch (Exception ex)
            {
                WriteCloudFetchDebug($"CloudFetchDownloader Download: Exception - {ex.Message}");
                throw;
            }
        }

        private async Task DownloadFilesInternalAsync(CancellationToken cancellationToken)
        {

            int totalFiles = 0;
            int successfulDownloads = 0;
            int failedDownloads = 0;
            long totalBytes = 0;
            var overallStopwatch = Stopwatch.StartNew();

            try
            {
                // Keep track of active download tasks
                var downloadTasks = new ConcurrentDictionary<Task, IDownloadResult>();
                var downloadTaskCompletionSource = new TaskCompletionSource<bool>();

                // Process items from the download queue until it's completed
                WriteCloudFetchDebug($"CloudFetchDownloader Internal: About to start consuming download queue - queue_count={_downloadQueue.Count}, queue_completed={_downloadQueue.IsCompleted}");
                WriteCloudFetchDebug($"🔍 CloudFetchDownloader Internal: PRODUCER LOOP STARTING - This will iterate until download queue is completed");

                foreach (var downloadResult in _downloadQueue.GetConsumingEnumerable(cancellationToken))
                {
                    totalFiles++;
                    WriteCloudFetchDebug($"CloudFetchDownloader Internal: Processing file #{totalFiles} - download_queue={_downloadQueue.Count}, result_queue={_resultQueue.Count}");

                    // Check if there's an error before processing more downloads
                    if (HasError)
                    {
                        // Add the failed download result to the queue to signal the error
                        // This will be caught by GetNextDownloadedFileAsync
                        break;
                    }

                    // Check if this is the end of results guard
                    if (downloadResult == EndOfResultsGuard.Instance)
                    {
                        // Wait for all active downloads to complete
                        if (downloadTasks.Count > 0)
                        {
                            try
                            {
                                await Task.WhenAll(downloadTasks.Keys).ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                WriteCloudFetchDebug($"CloudFetchDownloader: Warning - Error waiting for downloads to complete: {ex.Message}");
                                // Don't set error here, as individual download tasks will handle their own errors
                            }
                        }

                        // Only add the guard if there's no error
                        if (!HasError)
                        {
                            // Add the guard to the result queue to signal the end of results
                            WriteCloudFetchDebug("CloudFetchDownloader Internal: ⚪ All downloads completed successfully - adding EndOfResultsGuard.Instance to result queue");
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: ⚪ Result queue has {_resultQueue.Count} items before adding EndOfResultsGuard");
                            _resultQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                            WriteCloudFetchDebug("CloudFetchDownloader Internal: ⚪ EndOfResultsGuard.Instance added - CloudFetchReader will handle final batch aggregation");
                            _isCompleted = true;
                        }
                        break;
                    }

                    // Check if the URL is expired or about to expire
                    if (downloadResult.IsExpiredOrExpiringSoon(_urlExpirationBufferSeconds))
                    {
                        // Get a refreshed URL before starting the download
                        var refreshedLink = await _resultFetcher.GetUrlAsync(downloadResult.Link.StartRowOffset, cancellationToken);
                        if (refreshedLink != null)
                        {
                            // Update the download result with the refreshed link
                            downloadResult.UpdateWithRefreshedLink(refreshedLink);
                            System.Diagnostics.Trace.TraceInformation($"Updated URL for file at offset {refreshedLink.StartRowOffset} before download");
                        }
                    }

                    // Acquire a download slot
                    int taskId = totalFiles; // Use file number as unique task ID
                    WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] About to wait for semaphore - available={_downloadSemaphore.CurrentCount}, max={_maxParallelDownloads}, active_tasks={downloadTasks.Count}");

                    // CRITICAL: Add debugging around the WaitAsync call
                    var semaphoreBefore = _downloadSemaphore.CurrentCount;
                    WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Semaphore BEFORE WaitAsync: available={semaphoreBefore}");

                    await _downloadSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                    var semaphoreAfter = _downloadSemaphore.CurrentCount;
                    WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Got semaphore slot - BEFORE: available={semaphoreBefore}, AFTER: available={semaphoreAfter}, active_tasks={downloadTasks.Count}");

                    // Sanity check: After WaitAsync, available count should be 1 less than before
                    if (semaphoreAfter != semaphoreBefore - 1)
                    {
                        WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] ⚠️ SEMAPHORE INCONSISTENCY: Expected available={semaphoreBefore - 1}, got available={semaphoreAfter}");
                    }

                    WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Starting download task - active_tasks={downloadTasks.Count}");

                    // Start the download task
                    WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] About to start DownloadFileAsync");
                    Task downloadTask = DownloadFileAsync(downloadResult, cancellationToken);

                    // CRITICAL: Add task to dictionary BEFORE ContinueWith to avoid race condition
                    downloadTasks[downloadTask] = downloadResult;
                    WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Added task to dictionary BEFORE continuation - active_tasks={downloadTasks.Count}");

                    // Now add the continuation
                    downloadTask = downloadTask.ContinueWith(t =>
                        {
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Download task completed - Status={t.Status}, IsFaulted={t.IsFaulted}, IsCanceled={t.IsCanceled}");

                            // CRITICAL: Add debugging around the Release call
                            var semaphoreBeforeRelease = _downloadSemaphore.CurrentCount;
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Semaphore BEFORE Release: available={semaphoreBeforeRelease}, max={_maxParallelDownloads}");

                            var releasedCount = _downloadSemaphore.Release();

                            var semaphoreAfterRelease = _downloadSemaphore.CurrentCount;
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] RELEASED semaphore slot - BEFORE: available={semaphoreBeforeRelease}, AFTER: available={semaphoreAfterRelease}, released_count={releasedCount}, max_count={_maxParallelDownloads}");

                            // CRITICAL: If released_count is 0, that means we're over-releasing!
                            if (releasedCount == 0)
                            {
                                WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] ⚠️ WARNING: Release() returned 0 - semaphore was already at maximum! BEFORE={semaphoreBeforeRelease}, AFTER={semaphoreAfterRelease}");
                            }

                            // Sanity check: After Release, available count should be 1 more than before (unless we were already at max)
                            if (releasedCount > 0 && semaphoreAfterRelease != semaphoreBeforeRelease + 1)
                            {
                                WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] ⚠️ RELEASE INCONSISTENCY: Expected available={semaphoreBeforeRelease + 1}, got available={semaphoreAfterRelease}");
                            }

                            // Remove the ORIGINAL task from the dictionary (not the continuation task)
                            var originalTask = t; // t is the original DownloadFileAsync task
                            bool removed = downloadTasks.TryRemove(originalTask, out _);
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Removed ORIGINAL task from dictionary - removed={removed}, active_tasks={downloadTasks.Count}");

                            // Handle any exceptions
                            if (t.IsFaulted)
                            {
                                Exception ex = t.Exception?.InnerException ?? new Exception("Unknown error");
                                WriteCloudFetchDebug($"CloudFetchDownloader: [TASK-{taskId}] Error - Download failed for file {SanitizeUrl(downloadResult.Link.FileLink)}: {ex.Message}");

                                // Set the download as failed
                                downloadResult.SetFailed(ex);
                                Interlocked.Increment(ref failedDownloads);

                                // Set the error state to stop the download process
                                SetError(ex);

                                // Signal that we should stop processing downloads
                                downloadTaskCompletionSource.TrySetException(ex);
                            }
                            else if (t.IsCompleted && !t.IsFaulted && !t.IsCanceled)
                            {
                                Interlocked.Increment(ref successfulDownloads);
                                Interlocked.Add(ref totalBytes, downloadResult.Size);
                                WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Download successful - total_success={successfulDownloads}, total_bytes={totalBytes / 1024.0 / 1024.0:F1}MB");
                            }
                            else if (t.IsCanceled)
                            {
                                WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Download was canceled");
                            }
                        }, TaskContinuationOptions.ExecuteSynchronously);

                                    // CRITICAL: Add to result queue in ORDER to maintain sequence (before download completes)
                // But use TryAdd with timeout to prevent deadlock when queue is full
                var queueCountBeforeAdd = _resultQueue.Count;
                WriteCloudFetchDebug($"📥 CloudFetchDownloader Internal: [TASK-{taskId}] About to ADD item to result queue - queue_count_before={queueCountBeforeAdd}, queue_capacity={_resultQueue.BoundedCapacity}, item_size={downloadResult.Size}");

                if (!_resultQueue.TryAdd(downloadResult, 100, cancellationToken))
                    {
                        // If we can't add within 100ms, implement backpressure by pausing
                        WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] ⚠️ RESULT QUEUE FULL! (capacity={_resultQueue.BoundedCapacity}) - This is blocking TCP connection #{taskId}");

                        // Wait a bit longer and try again
                        await Task.Delay(500, cancellationToken);
                        if (!_resultQueue.TryAdd(downloadResult, 1000, cancellationToken))
                        {
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] ⚠️ RESULT QUEUE STILL FULL after 1500ms - TCP connection #{taskId} will BLOCK here until consumer processes results");
                            // Force add with blocking - but now we know why it's blocking
                            _resultQueue.Add(downloadResult, cancellationToken);
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Result queue blockage resolved - TCP connection #{taskId} can now continue");
                        }
                        else
                        {
                            WriteCloudFetchDebug($"CloudFetchDownloader Internal: [TASK-{taskId}] Result queue unblocked after 1500ms - TCP connection #{taskId} proceeding");
                        }
                                    }
                else
                {
                    var queueCountAfterAdd = _resultQueue.Count;
                    var addTimestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    WriteCloudFetchDebug($"✅ [{addTimestamp}] CloudFetchDownloader Internal: [TASK-{taskId}] ITEM SUCCESSFULLY ADDED to result queue - queue_before={queueCountBeforeAdd}, queue_after={queueCountAfterAdd}, items_added={(queueCountAfterAdd - queueCountBeforeAdd)}");
                    WriteCloudFetchDebug($"📊 [{addTimestamp}] CloudFetchDownloader Internal: [TASK-{taskId}] *** PRODUCER: Item available for consumption *** - result_queue_count={_resultQueue.Count}");
                }

                    // If there's an error, stop processing more downloads
                    if (HasError)
                    {
                        break;
                    }
                }

                // CRITICAL: Log why the producer loop ended
                WriteCloudFetchDebug($"🔍 CloudFetchDownloader Internal: PRODUCER LOOP ENDED - total_files_processed={totalFiles}");
                WriteCloudFetchDebug($"🔍 CloudFetchDownloader Internal: FINAL STATE - download_queue_count={_downloadQueue.Count}, download_queue_completed={_downloadQueue.IsCompleted}");
                WriteCloudFetchDebug($"🔍 CloudFetchDownloader Internal: FINAL STATE - result_queue_count={_resultQueue.Count}, result_queue_completed={_resultQueue.IsCompleted}");
                WriteCloudFetchDebug($"🔍 CloudFetchDownloader Internal: FINAL STATE - has_error={HasError}, is_completed={_isCompleted}");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected when cancellation is requested
                WriteCloudFetchDebug("CloudFetchDownloader: Download process was cancelled");
            }
            catch (Exception ex)
            {
                WriteCloudFetchDebug($"CloudFetchDownloader: Error in download loop: {ex.Message}");
                SetError(ex);
            }
            finally
            {
                overallStopwatch.Stop();

                WriteCloudFetchDebug($"CloudFetchDownloader: Download process completed - TotalFiles={totalFiles}, SuccessfulDownloads={successfulDownloads}, FailedDownloads={failedDownloads}, TotalSizeMB={totalBytes / 1024.0 / 1024.0:F2}, TotalTimeSec={overallStopwatch.ElapsedMilliseconds / 1000.0:F2}");

                // If there's an error, add the error to the result queue and complete it
                if (HasError)
                {
                    CompleteWithError();
                    WriteCloudFetchDebug("CloudFetchDownloader Internal: 🔒 Marking result queue as COMPLETED due to error");
                    _resultQueue.CompleteAdding();
                }
                else if (!_resultQueue.IsCompleted)
                {
                    // Ensure result queue is completed in normal scenarios (should already be done by EndOfResultsGuard processing)
                    WriteCloudFetchDebug("CloudFetchDownloader Internal: 🔒 Ensuring result queue is COMPLETED (backup completion)");
                    _resultQueue.CompleteAdding();
                }
            }
        }

        private async Task DownloadFileAsync(IDownloadResult downloadResult, CancellationToken cancellationToken)
        {
            string url = downloadResult.Link.FileLink;
            string sanitizedUrl = SanitizeUrl(downloadResult.Link.FileLink);
            byte[]? fileData = null;

            // Use the size directly from the download result
            long size = downloadResult.Size;

            WriteCloudFetchDebug($"CloudFetchDownloader: ENTERED DownloadFileAsync for {sanitizedUrl}, expected size: {size / 1024.0:F2} KB");

            // Create a stopwatch to track download time
            var stopwatch = Stopwatch.StartNew();

            // Log download start
            System.Diagnostics.Trace.TraceInformation($"Starting download of file {sanitizedUrl}, expected size: {size / 1024.0:F2} KB");

            // Acquire memory before downloading
            WriteCloudFetchDebug($"CloudFetchDownloader: About to acquire memory for {sanitizedUrl}, size: {size / 1024.0:F2} KB");

            // SUSPECT #1: Memory acquisition might be hanging
            WriteCloudFetchDebug($"CloudFetchDownloader: MEMORY STATE BEFORE ACQUIRE for {sanitizedUrl} - Requesting: {size / 1024.0 / 1024.0:F1}MB, Current: {_memoryManager.UsedMemory / 1024.0 / 1024.0:F1}MB, Max: {_memoryManager.MaxMemory / 1024.0 / 1024.0:F1}MB, Available: {(_memoryManager.MaxMemory - _memoryManager.UsedMemory) / 1024.0 / 1024.0:F1}MB");

            var memoryTask = _memoryManager.AcquireMemoryAsync(size, cancellationToken);
            var memoryDelay = Task.Delay(5000); // 5 second timeout for debugging
            var completedTask = await Task.WhenAny(memoryTask, memoryDelay);

            if (completedTask == memoryDelay)
            {
                WriteCloudFetchDebug($"CloudFetchDownloader: MEMORY ACQUISITION HANGING for {sanitizedUrl} - Requesting: {size / 1024.0 / 1024.0:F1}MB, Used: {_memoryManager.UsedMemory / 1024.0 / 1024.0:F1}MB, Max: {_memoryManager.MaxMemory / 1024.0 / 1024.0:F1}MB");
                WriteCloudFetchDebug($"CloudFetchDownloader: ⚠️ MEMORY PRESSURE! Need {size / 1024.0 / 1024.0:F1}MB but only {(_memoryManager.MaxMemory - _memoryManager.UsedMemory) / 1024.0 / 1024.0:F1}MB available - this is likely the problem!");

                // Still wait for it to complete, but now we know it's slow
                await memoryTask.ConfigureAwait(false);
                WriteCloudFetchDebug($"CloudFetchDownloader: Memory finally acquired for {sanitizedUrl} after timeout - New Used: {_memoryManager.UsedMemory / 1024.0 / 1024.0:F1}MB");
            }
            else
            {
                await memoryTask.ConfigureAwait(false);
                WriteCloudFetchDebug($"CloudFetchDownloader: Memory acquired quickly for {sanitizedUrl} - New Used: {_memoryManager.UsedMemory / 1024.0 / 1024.0:F1}MB");
            }

            // Retry logic for downloading files
            for (int retry = 0; retry < _maxRetries; retry++)
            {
                try
                {
                    WriteCloudFetchDebug($"CloudFetchDownloader: Starting HTTP request for {sanitizedUrl} (attempt {retry + 1}/{_maxRetries})");

                    // SUSPECT #2: HTTP request might be hanging
                    var httpTask = _httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                    var httpDelay = Task.Delay(10000); // 10 second timeout for debugging
                    var httpCompletedTask = await Task.WhenAny(httpTask, httpDelay);

                    HttpResponseMessage response;
                    if (httpCompletedTask == httpDelay)
                    {
                        WriteCloudFetchDebug($"CloudFetchDownloader: HTTP REQUEST HANGING for {sanitizedUrl} - this is likely the problem!");
                        response = await httpTask.ConfigureAwait(false);
                        WriteCloudFetchDebug($"CloudFetchDownloader: HTTP request finally completed for {sanitizedUrl} after timeout");
                    }
                    else
                    {
                        response = await httpTask.ConfigureAwait(false);
                        WriteCloudFetchDebug($"CloudFetchDownloader: HTTP response received quickly for {sanitizedUrl} - Status: {response.StatusCode}");
                    }

                    using (response)
                    {
                        // Check if the response indicates an expired URL (typically 403 or 401)
                        if (response.StatusCode == System.Net.HttpStatusCode.Forbidden ||
                            response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                        {
                            // If we've already tried refreshing too many times, fail
                            if (downloadResult.RefreshAttempts >= _maxUrlRefreshAttempts)
                            {
                                throw new InvalidOperationException($"Failed to download file after {downloadResult.RefreshAttempts} URL refresh attempts.");
                            }

                            // Try to refresh the URL
                            var refreshedLink = await _resultFetcher.GetUrlAsync(downloadResult.Link.StartRowOffset, cancellationToken);
                            if (refreshedLink != null)
                            {
                                // Update the download result with the refreshed link
                                downloadResult.UpdateWithRefreshedLink(refreshedLink);
                                url = refreshedLink.FileLink;
                                sanitizedUrl = SanitizeUrl(url);

                                System.Diagnostics.Trace.TraceInformation($"URL for file at offset {refreshedLink.StartRowOffset} was refreshed after expired URL response");

                                // Continue to the next retry attempt with the refreshed URL
                                continue;
                            }
                            else
                            {
                                // If refresh failed, throw an exception
                                throw new InvalidOperationException("Failed to refresh expired URL.");
                            }
                        }

                        response.EnsureSuccessStatusCode();

                        // Log the download size if available from response headers
                        long? contentLength = response.Content.Headers.ContentLength;
                        if (contentLength.HasValue && contentLength.Value > 0)
                        {
                            System.Diagnostics.Trace.TraceInformation($"Actual file size for {sanitizedUrl}: {contentLength.Value / 1024.0 / 1024.0:F2} MB");
                        }

                        // SUSPECT #3: Data reading might be hanging
                        WriteCloudFetchDebug($"CloudFetchDownloader: Starting to read response data for {sanitizedUrl}");
                        var readTask = response.Content.ReadAsByteArrayAsync();
                        var readDelay = Task.Delay(15000); // 15 second timeout for debugging
                        var readCompletedTask = await Task.WhenAny(readTask, readDelay);

                        if (readCompletedTask == readDelay)
                        {
                            WriteCloudFetchDebug($"CloudFetchDownloader: DATA READING HANGING for {sanitizedUrl} - this is likely the problem!");
                            fileData = await readTask.ConfigureAwait(false);
                            WriteCloudFetchDebug($"CloudFetchDownloader: Data finally read for {sanitizedUrl} after timeout - size: {fileData.Length / 1024.0:F2} KB");
                        }
                        else
                        {
                            fileData = await readTask.ConfigureAwait(false);
                            WriteCloudFetchDebug($"CloudFetchDownloader: Data read quickly for {sanitizedUrl} - size: {fileData.Length / 1024.0:F2} KB");
                        }

                        break; // Success, exit retry loop
                    }
                }
                catch (Exception ex) when (retry < _maxRetries - 1 && !cancellationToken.IsCancellationRequested)
                {
                    // Log the error and retry
                    WriteCloudFetchDebug($"CloudFetchDownloader: Error downloading file {SanitizeUrl(url)} (attempt {retry + 1}/{_maxRetries}): {ex.Message}");

                    await Task.Delay(_retryDelayMs * (retry + 1), cancellationToken).ConfigureAwait(false);
                }
            }

            if (fileData == null)
            {
                stopwatch.Stop();
                System.Diagnostics.Trace.TraceError($"Failed to download file {sanitizedUrl} after {_maxRetries} attempts. Elapsed time: {stopwatch.ElapsedMilliseconds} ms");

                // Release the memory we acquired
                _memoryManager.ReleaseMemory(size);

                // DO NOT RELEASE SEMAPHORE HERE - it will be released in ContinueWith
                throw new InvalidOperationException($"Failed to download file from {url} after {_maxRetries} attempts.");
            }

            // Process the downloaded file data
            MemoryStream dataStream;
            long actualSize = fileData.Length;

            // If the data is LZ4 compressed, decompress it
            if (_isLz4Compressed)
            {
                try
                {
                    WriteCloudFetchDebug($"CloudFetchDownloader: Starting LZ4 decompression for {sanitizedUrl}");
                    var decompressStopwatch = Stopwatch.StartNew();
                    dataStream = new MemoryStream();

                    // SUSPECT #4: LZ4 decompression might be hanging
                    using (var inputStream = new MemoryStream(fileData))
                    using (var decompressor = LZ4Stream.Decode(inputStream))
                    {
                        var decompressTask = decompressor.CopyToAsync(dataStream, 81920, cancellationToken);
                        var decompressDelay = Task.Delay(10000); // 10 second timeout for debugging
                        var decompressCompletedTask = await Task.WhenAny(decompressTask, decompressDelay);

                        if (decompressCompletedTask == decompressDelay)
                        {
                            WriteCloudFetchDebug($"CloudFetchDownloader: LZ4 DECOMPRESSION HANGING for {sanitizedUrl} - this is likely the problem!");
                            await decompressTask.ConfigureAwait(false);
                            WriteCloudFetchDebug($"CloudFetchDownloader: LZ4 decompression finally completed for {sanitizedUrl} after timeout");
                        }
                        else
                        {
                            await decompressTask.ConfigureAwait(false);
                            WriteCloudFetchDebug($"CloudFetchDownloader: LZ4 decompression completed quickly for {sanitizedUrl}");
                        }
                    }
                    dataStream.Position = 0;
                    decompressStopwatch.Stop();

                    var compressedSize = actualSize;
                    var decompressedSize = dataStream.Length;

                    System.Diagnostics.Trace.TraceInformation($"Decompressed file {sanitizedUrl} in {decompressStopwatch.ElapsedMilliseconds} ms. Compressed size: {compressedSize / 1024.0:F2} KB, Decompressed size: {decompressedSize / 1024.0:F2} KB");
                    WriteCloudFetchDebug($"📊 CloudFetchDownloader: ACTUAL DECOMPRESSION STATS - Compressed: {compressedSize:N0} bytes ({compressedSize / 1024.0 / 1024.0:F1}MB) → Decompressed: {decompressedSize:N0} bytes ({decompressedSize / 1024.0 / 1024.0:F1}MB)");
                    WriteCloudFetchDebug($"📊 CloudFetchDownloader: COMPRESSION RATIO - {(double)decompressedSize / compressedSize:F1}x expansion ({100.0 * (decompressedSize - compressedSize) / compressedSize:F1}% larger after decompression)");

                    actualSize = dataStream.Length;
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    System.Diagnostics.Trace.TraceError($"Error decompressing data for file {sanitizedUrl}: {ex.Message}. Elapsed time: {stopwatch.ElapsedMilliseconds} ms");

                    // Release the memory we acquired
                    _memoryManager.ReleaseMemory(size);

                    // DO NOT RELEASE SEMAPHORE HERE - it will be released in ContinueWith
                    throw new InvalidOperationException($"Error decompressing data: {ex.Message}", ex);
                }
            }
            else
            {
                dataStream = new MemoryStream(fileData);
            }

            // Stop the stopwatch and log download completion
            stopwatch.Stop();
            WriteCloudFetchDebug($"CloudFetchDownloader: COMPLETED DownloadFileAsync for {sanitizedUrl}. Size: {actualSize / 1024.0:F2} KB, Latency: {stopwatch.ElapsedMilliseconds} ms, Throughput: {(actualSize / 1024.0 / 1024.0) / (stopwatch.ElapsedMilliseconds / 1000.0):F2} MB/s");
            System.Diagnostics.Trace.TraceInformation($"Completed download of file {sanitizedUrl}. Size: {actualSize / 1024.0:F2} KB, Latency: {stopwatch.ElapsedMilliseconds} ms, Throughput: {(actualSize / 1024.0 / 1024.0) / (stopwatch.ElapsedMilliseconds / 1000.0):F2} MB/s");

            // Set the download as completed with the ACTUAL decompressed size (critical for accurate batch aggregation)
            downloadResult.SetCompleted(dataStream, actualSize);
            WriteCloudFetchDebug($"CloudFetchDownloader: SetCompleted called for {sanitizedUrl} - DownloadFileAsync FINISHED");
            WriteCloudFetchDebug($"CloudFetchDownloader: MEMORY STATE AFTER DOWNLOAD for {sanitizedUrl} - Used: {_memoryManager.UsedMemory / 1024.0 / 1024.0:F1}MB, Max: {_memoryManager.MaxMemory / 1024.0 / 1024.0:F1}MB (Note: Memory will be released when file is consumed)");
        }

        private void SetError(Exception ex)
        {
            lock (_errorLock)
            {
                if (_error == null)
                {
                    WriteCloudFetchDebug($"CloudFetchDownloader: Setting error state: {ex.Message}");
                    _error = ex;
                }
            }
        }

        private void CompleteWithError()
        {
            try
            {
                // Mark the result queue as completed to prevent further additions
                _resultQueue.CompleteAdding();

                // Mark the download as completed with error
                _isCompleted = true;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError($"Error completing with error: {ex.Message}");
            }
        }

        // Helper method to sanitize URLs for logging (to avoid exposing sensitive information)
        private string SanitizeUrl(string url)
        {
            try
            {
                var uri = new Uri(url);
                return $"{uri.Scheme}://{uri.Host}/{Path.GetFileName(uri.LocalPath)}";
            }
            catch
            {
                // If URL parsing fails, return a generic identifier
                return "cloud-storage-url";
            }
        }
    }
}

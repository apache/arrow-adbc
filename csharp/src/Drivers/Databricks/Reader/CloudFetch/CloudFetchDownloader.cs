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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tracing;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Downloads files from URLs.
    /// </summary>
    internal sealed class CloudFetchDownloader : ICloudFetchDownloader, IActivityTracer
    {
        // Straggler mitigation timing constants
        private static readonly TimeSpan StragglerMonitoringInterval = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan MetricsCleanupDelay = TimeSpan.FromSeconds(5);  // Must be > monitoring interval
        private static readonly TimeSpan CtsDisposalDelay = TimeSpan.FromSeconds(6);  // Must be > metrics cleanup delay

        private readonly ITracingStatement _statement;
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

        // Straggler mitigation fields
        private readonly bool _isStragglerMitigationEnabled;
        private readonly StragglerDownloadDetector? _stragglerDetector;
        private readonly ConcurrentDictionary<long, FileDownloadMetrics>? _activeDownloadMetrics;
        private readonly ConcurrentDictionary<long, CancellationTokenSource>? _perFileDownloadCancellationTokens;
        private readonly ConcurrentDictionary<long, bool>? _alreadyCountedStragglers;  // Prevents duplicate counting of same file
        private readonly ConcurrentDictionary<long, Task>? _metricCleanupTasks;  // Tracks cleanup tasks for proper shutdown
        private Task? _stragglerMonitoringTask;
        private CancellationTokenSource? _stragglerMonitoringCts;
        private volatile bool _hasTriggeredSequentialDownloadFallback;
        private SemaphoreSlim _sequentialSemaphore = new SemaphoreSlim(1, 1);
        private volatile bool _isSequentialMode;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloader"/> class.
        /// </summary>
        /// <param name="statement">The tracing statement for Activity context.</param>
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
        /// <param name="stragglerConfig">Optional configuration for straggler mitigation (null = disabled).</param>
        public CloudFetchDownloader(
            ITracingStatement statement,
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
            int urlExpirationBufferSeconds = 60,
            CloudFetchStragglerMitigationConfig? stragglerConfig = null)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
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

            // Initialize straggler mitigation from config object
            var config = stragglerConfig ?? CloudFetchStragglerMitigationConfig.Disabled;
            _isStragglerMitigationEnabled = config.Enabled;

            if (config.Enabled)
            {
                _stragglerDetector = new StragglerDownloadDetector(
                    config.Multiplier,
                    config.Quantile,
                    config.Padding,
                    config.SynchronousFallbackEnabled ? config.MaxStragglersBeforeFallback : int.MaxValue);

                _activeDownloadMetrics = new ConcurrentDictionary<long, FileDownloadMetrics>();
                _perFileDownloadCancellationTokens = new ConcurrentDictionary<long, CancellationTokenSource>();
                _alreadyCountedStragglers = new ConcurrentDictionary<long, bool>();
                _metricCleanupTasks = new ConcurrentDictionary<long, Task>();
                _hasTriggeredSequentialDownloadFallback = false;
            }
        }

        /// <inheritdoc />
        public bool IsCompleted => _isCompleted;

        /// <inheritdoc />
        public bool HasError => _error != null;

        /// <inheritdoc />
        public Exception? Error => _error;

        /// <summary>
        /// Internal property to check if straggler mitigation is enabled (for testing).
        /// </summary>
        internal bool IsStragglerMitigationEnabled => _isStragglerMitigationEnabled;

        /// <summary>
        /// Internal property to get total stragglers detected (for testing).
        /// </summary>
        internal long GetTotalStragglersDetected() => _stragglerDetector?.GetTotalStragglersDetectedInQuery() ?? 0;

        /// <summary>
        /// Internal property to get count of active downloads being tracked (for testing).
        /// </summary>
        internal int GetActiveDownloadCount() => _activeDownloadMetrics?.Count ?? 0;

        /// <summary>
        /// Internal property to check if tracking dictionaries are initialized (for testing).
        /// </summary>
        internal bool AreTrackingDictionariesInitialized() => _activeDownloadMetrics != null && _perFileDownloadCancellationTokens != null;


        /// <inheritdoc />
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_downloadTask != null)
            {
                throw new InvalidOperationException("Downloader is already running.");
            }

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _downloadTask = DownloadFilesAsync(_cancellationTokenSource.Token);

            // Start straggler monitoring if enabled
            if (_isStragglerMitigationEnabled && _stragglerDetector != null)
            {
                _stragglerMonitoringCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _stragglerMonitoringTask = MonitorForStragglerDownloadsAsync(_stragglerMonitoringCts.Token);
            }

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

            // Stop straggler monitoring if running
            if (_stragglerMonitoringTask != null)
            {
                _stragglerMonitoringCts?.Cancel();
                try
                {
                    await _stragglerMonitoringTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancellation is requested
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Error stopping straggler monitoring: {ex.Message}");
                }
                finally
                {
                    _stragglerMonitoringCts?.Dispose();
                    _stragglerMonitoringCts = null;
                    _stragglerMonitoringTask = null;
                }
            }

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

                // Await all metric cleanup tasks before disposing resources
                if (_metricCleanupTasks != null && _metricCleanupTasks.Count > 0)
                {
                    try
                    {
                        await Task.WhenAll(_metricCleanupTasks.Values).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Ignore cleanup task exceptions during shutdown
                    }
                    _metricCleanupTasks.Clear();
                }

                // Cleanup per-file cancellation tokens
                if (_perFileDownloadCancellationTokens != null)
                {
                    foreach (var cts in _perFileDownloadCancellationTokens.Values)
                    {
                        cts?.Dispose();
                    }
                    _perFileDownloadCancellationTokens.Clear();
                }

                // Dispose sequential semaphore
                _sequentialSemaphore?.Dispose();
            }
        }

        /// <inheritdoc />
        public async Task<IDownloadResult?> GetNextDownloadedFileAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Check if there's an error before trying to take from the queue
                if (HasError)
                {
                    throw new AdbcException("Error in download process", _error ?? new Exception("Unknown error"));
                }

                // Try to take the next result from the queue
                IDownloadResult result = await Task.Run(() => _resultQueue.Take(cancellationToken), cancellationToken);

                // Check if this is the end of results guard
                if (result == EndOfResultsGuard.Instance)
                {
                    _isCompleted = true;
                    return null;
                }

                return result;
            }
            catch (OperationCanceledException)
            {
                // Cancellation was requested
                return null;
            }
            catch (InvalidOperationException) when (_resultQueue.IsCompleted)
            {
                // Queue is completed and empty
                _isCompleted = true;
                return null;
            }
            catch (AdbcException)
            {
                // Re-throw AdbcExceptions (these are our own errors)
                throw;
            }
            catch (Exception ex)
            {
                // If there's an error, set the error state and propagate it
                SetError(ex);
                throw;
            }
        }

        private async Task DownloadFilesAsync(CancellationToken cancellationToken)
        {
            await this.TraceActivityAsync(async activity =>
            {
                await Task.Yield();

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
                    foreach (var downloadResult in _downloadQueue.GetConsumingEnumerable(cancellationToken))
                    {
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
                                    activity?.AddException(ex, [new("error.context", "cloudfetch.wait_for_downloads")]);
                                    // Don't set error here, as individual download tasks will handle their own errors
                                }
                            }

                            // Only add the guard if there's no error
                            if (!HasError)
                            {
                                // Add the guard to the result queue to signal the end of results
                                _resultQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                                _isCompleted = true;
                            }
                            break;
                        }

                        // This is a real file, count it
                        totalFiles++;

                        // Check if the URL is expired or about to expire
                        if (downloadResult.IsExpiredOrExpiringSoon(_urlExpirationBufferSeconds))
                        {
                            // Get a refreshed URL before starting the download
                            var refreshedLink = await _resultFetcher.GetUrlAsync(downloadResult.Link.StartRowOffset, cancellationToken);
                            if (refreshedLink != null)
                            {
                                // Update the download result with the refreshed link
                                downloadResult.UpdateWithRefreshedLink(refreshedLink);
                                activity?.AddEvent("cloudfetch.url_refreshed_before_download", [
                                    new("offset", refreshedLink.StartRowOffset)
                                ]);
                            }
                        }

                        // Acquire a download slot
                        await _downloadSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                        bool shouldAcquireSequential = _isSequentialMode;
                        bool acquiredSequential = false;
                        if (shouldAcquireSequential)
                        {
                            await _sequentialSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                            acquiredSequential = true;
                        }

                        Task downloadTask;
                        try
                        {
                            // Start the download task
                            downloadTask = DownloadFileAsync(downloadResult, cancellationToken)
                                .ContinueWith(t =>
                                {
                                    // Release in reverse order
                                    if (acquiredSequential)
                                    {
                                        _sequentialSemaphore.Release();
                                    }
                                    _downloadSemaphore.Release();

                                // Remove the task from the dictionary
                                downloadTasks.TryRemove(t, out _);

                                // Handle any exceptions
                                if (t.IsFaulted)
                                {
                                    Exception ex = t.Exception?.InnerException ?? new Exception("Unknown error");
                                    string sanitizedUrl = SanitizeUrl(downloadResult.Link.FileLink);
                                    activity?.AddException(ex, [
                                        new("error.context", "cloudfetch.download_failed"),
                                        new("offset", downloadResult.Link.StartRowOffset),
                                        new("sanitized_url", sanitizedUrl)
                                    ]);

                                    // Set the download as failed
                                    downloadResult.SetFailed(ex);
                                    failedDownloads++;

                                    // Set the error state to stop the download process
                                    SetError(ex, activity);

                                    // Signal that we should stop processing downloads
                                    downloadTaskCompletionSource.TrySetException(ex);
                                }
                                else if (!t.IsFaulted && !t.IsCanceled)
                                {
                                    successfulDownloads++;
                                    totalBytes += downloadResult.Size;
                                }
                            }, cancellationToken);

                            // Add the task to the dictionary
                            downloadTasks[downloadTask] = downloadResult;
                        }
                        catch
                        {
                            // If task creation fails, release semaphores to prevent leak
                            if (acquiredSequential)
                            {
                                _sequentialSemaphore.Release();
                            }
                            _downloadSemaphore.Release();
                            throw;
                        }

                        // Add the result to the result queue add the result here to assure the download sequence.
                        _resultQueue.Add(downloadResult, cancellationToken);

                        // If there's an error, stop processing more downloads
                        if (HasError)
                        {
                            break;
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Expected when cancellation is requested
                    activity?.AddEvent("cloudfetch.download_cancelled");
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex, [new("error.context", "cloudfetch.download_loop")]);
                    SetError(ex, activity);
                }
                finally
                {
                    overallStopwatch.Stop();

                    activity?.AddEvent("cloudfetch.download_summary", [
                        new("total_files", totalFiles),
                        new("successful_downloads", successfulDownloads),
                        new("failed_downloads", failedDownloads),
                        new("total_bytes", totalBytes),
                        new("total_mb", totalBytes / 1024.0 / 1024.0),
                        new("total_time_ms", overallStopwatch.ElapsedMilliseconds),
                        new("total_time_sec", overallStopwatch.ElapsedMilliseconds / 1000.0)
                    ]);

                    // If there's an error, add the error to the result queue
                    if (HasError)
                    {
                        CompleteWithError(activity);
                    }
                }
            });
        }

        private async Task DownloadFileAsync(IDownloadResult downloadResult, CancellationToken cancellationToken)
        {
            await this.TraceActivityAsync(async activity =>
            {
                string url = downloadResult.Link.FileLink;
                string sanitizedUrl = SanitizeUrl(downloadResult.Link.FileLink);
                byte[]? fileData = null;

                // Use the size directly from the download result
                long size = downloadResult.Size;

                // Add tags to the Activity for filtering/searching
                activity?.SetTag("cloudfetch.offset", downloadResult.Link.StartRowOffset);
                activity?.SetTag("cloudfetch.sanitized_url", sanitizedUrl);
                activity?.SetTag("cloudfetch.expected_size_bytes", size);

                // Create a stopwatch to track download time
                var stopwatch = Stopwatch.StartNew();

                // Log download start
                activity?.AddEvent("cloudfetch.download_start", [
                new("offset", downloadResult.Link.StartRowOffset),
                    new("sanitized_url", sanitizedUrl),
                    new("expected_size_bytes", size),
                    new("expected_size_kb", size / 1024.0)
            ]);

                // Acquire memory before downloading
                await _memoryManager.AcquireMemoryAsync(size, cancellationToken).ConfigureAwait(false);

                // Declare variables for cleanup in finally block
                FileDownloadMetrics? downloadMetrics = null;
                CancellationTokenSource? perFileCancellationTokenSource = null;
                long fileOffset = downloadResult.Link.StartRowOffset;

            try
            {
                // Initialize straggler tracking if enabled (inside try block for proper cleanup)
                if (_isStragglerMitigationEnabled && _activeDownloadMetrics != null && _perFileDownloadCancellationTokens != null)
                {
                    downloadMetrics = new FileDownloadMetrics(fileOffset, size);
                    _activeDownloadMetrics[fileOffset] = downloadMetrics;

                    perFileCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    _perFileDownloadCancellationTokens[fileOffset] = perFileCancellationTokenSource;
                }


                // Retry logic for downloading files
                for (int retry = 0; retry < _maxRetries; retry++)
                {
                    try
                    {
                        // Use per-file cancellation token if available, otherwise use global token
                        CancellationToken effectiveToken = perFileCancellationTokenSource?.Token ?? cancellationToken;

                        // Download the file directly
                        using HttpResponseMessage response = await _httpClient.GetAsync(
                            url,
                            HttpCompletionOption.ResponseHeadersRead,
                            effectiveToken).ConfigureAwait(false);

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

                                activity?.AddEvent("cloudfetch.url_refreshed_after_auth_error", [
                                    new("offset", refreshedLink.StartRowOffset),
                                    new("sanitized_url", sanitizedUrl)
                                ]);

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
                            activity?.AddEvent("cloudfetch.content_length", [
                                new("offset", downloadResult.Link.StartRowOffset),
                                new("sanitized_url", sanitizedUrl),
                                new("content_length_bytes", contentLength.Value),
                                new("content_length_mb", contentLength.Value / 1024.0 / 1024.0)
                            ]);
                        }

                        // Read the file data
                        fileData = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
                        break; // Success, exit retry loop
                    }
                    catch (Exception ex) when (retry < _maxRetries - 1 && !cancellationToken.IsCancellationRequested)
                    {
                        // Log the error and retry
                        activity?.AddException(ex, [
                            new("error.context", "cloudfetch.download_retry"),
                            new("offset", downloadResult.Link.StartRowOffset),
                            new("sanitized_url", SanitizeUrl(url)),
                            new("attempt", retry + 1),
                            new("max_retries", _maxRetries)
                        ]);

                        await Task.Delay(_retryDelayMs * (retry + 1), cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (
                        perFileCancellationTokenSource?.IsCancellationRequested == true
                        && !cancellationToken.IsCancellationRequested
                        && retry < _maxRetries - 1  // Edge case protection: don't cancel last retry
                        && fileData == null)  // Race condition check: only retry if download didn't complete
                    {
                        // Straggler cancelled - this counts as one retry
                        activity?.AddEvent("cloudfetch.straggler_cancelled", [
                            new("offset", downloadResult.Link.StartRowOffset),
                            new("sanitized_url", sanitizedUrl),
                            new("file_size_mb", size / 1024.0 / 1024.0),
                            new("elapsed_seconds", stopwatch.ElapsedMilliseconds / 1000.0),
                            new("attempt", retry + 1),
                            new("max_retries", _maxRetries)
                        ]);

                        downloadMetrics?.MarkCancelledAsStragler();

                        // Create fresh cancellation token for retry atomically
                        var newCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        if (_perFileDownloadCancellationTokens != null)
                        {
                            var oldCts = _perFileDownloadCancellationTokens.AddOrUpdate(
                                downloadResult.Link.StartRowOffset,
                                newCts,
                                (key, existing) =>
                                {
                                    existing?.Dispose();  // Dispose old one atomically
                                    return newCts;
                                });

                            // If this was an add (not update), oldCts == newCts, so don't dispose
                            if (oldCts != newCts)
                            {
                                perFileCancellationTokenSource?.Dispose();
                            }

                            perFileCancellationTokenSource = newCts;
                        }
                        else
                        {
                            perFileCancellationTokenSource?.Dispose();
                            perFileCancellationTokenSource = newCts;
                        }

                        // Check if URL needs refresh (expired or expiring soon)
                        if (downloadResult.IsExpiredOrExpiringSoon(_urlExpirationBufferSeconds))
                        {
                            var refreshedLink = await _resultFetcher.GetUrlAsync(downloadResult.Link.StartRowOffset, cancellationToken);
                            if (refreshedLink != null)
                            {
                                downloadResult.UpdateWithRefreshedLink(refreshedLink);
                                url = refreshedLink.FileLink;
                                sanitizedUrl = SanitizeUrl(url);

                                activity?.AddEvent("cloudfetch.url_refreshed_for_straggler_retry", [
                                    new("offset", refreshedLink.StartRowOffset),
                                    new("sanitized_url", sanitizedUrl)
                                ]);
                            }
                            else
                            {
                                // URL refresh failed, log warning and continue with existing URL
                                activity?.AddEvent("cloudfetch.url_refresh_failed_for_straggler_retry", [
                                    new("offset", downloadResult.Link.StartRowOffset),
                                    new("sanitized_url", sanitizedUrl),
                                    new("warning", "Failed to refresh expired URL, continuing with existing URL")
                                ]);
                            }
                        }

                        // Apply retry delay
                        await Task.Delay(_retryDelayMs * (retry + 1), cancellationToken).ConfigureAwait(false);
                    }
                }

                if (fileData == null)
                {
                    stopwatch.Stop();
                    activity?.AddEvent("cloudfetch.download_failed_all_retries", [
                        new("offset", downloadResult.Link.StartRowOffset),
                        new("sanitized_url", sanitizedUrl),
                        new("max_retries", _maxRetries),
                        new("elapsed_time_ms", stopwatch.ElapsedMilliseconds)
                    ]);

                    // Release the memory we acquired
                    _memoryManager.ReleaseMemory(size);
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
                        var decompressStopwatch = Stopwatch.StartNew();
                        dataStream = new MemoryStream();
                        using (var inputStream = new MemoryStream(fileData))
                        using (var decompressor = LZ4Stream.Decode(inputStream))
                        {
                            await decompressor.CopyToAsync(dataStream, 81920, cancellationToken).ConfigureAwait(false);
                        }
                        dataStream.Position = 0;
                        decompressStopwatch.Stop();

                        activity?.AddEvent("cloudfetch.decompression_complete", [
                            new("offset", downloadResult.Link.StartRowOffset),
                            new("sanitized_url", sanitizedUrl),
                            new("decompression_time_ms", decompressStopwatch.ElapsedMilliseconds),
                            new("compressed_size_bytes", actualSize),
                            new("compressed_size_kb", actualSize / 1024.0),
                            new("decompressed_size_bytes", dataStream.Length),
                            new("decompressed_size_kb", dataStream.Length / 1024.0),
                            new("compression_ratio", (double)dataStream.Length / actualSize)
                        ]);

                        actualSize = dataStream.Length;
                    }
                    catch (Exception ex)
                    {
                        stopwatch.Stop();
                        activity?.AddException(ex, [
                            new("error.context", "cloudfetch.decompression"),
                            new("offset", downloadResult.Link.StartRowOffset),
                            new("sanitized_url", sanitizedUrl),
                            new("elapsed_time_ms", stopwatch.ElapsedMilliseconds)
                        ]);

                        // Release the memory we acquired
                        _memoryManager.ReleaseMemory(size);
                        throw new InvalidOperationException($"Error decompressing data: {ex.Message}", ex);
                    }
                }
                else
                {
                    dataStream = new MemoryStream(fileData);
                }

                // Stop the stopwatch and log download completion
                stopwatch.Stop();
                double throughputMBps = (actualSize / 1024.0 / 1024.0) / (stopwatch.ElapsedMilliseconds / 1000.0);
                activity?.AddEvent("cloudfetch.download_complete", [
                    new("offset", downloadResult.Link.StartRowOffset),
                    new("sanitized_url", sanitizedUrl),
                    new("actual_size_bytes", actualSize),
                    new("actual_size_kb", actualSize / 1024.0),
                    new("latency_ms", stopwatch.ElapsedMilliseconds),
                    new("throughput_mbps", throughputMBps)
                ]);

                // Set the download as completed with the original size
                downloadResult.SetCompleted(dataStream, size);

                // Mark download as completed
                if (downloadMetrics != null)
                {
                    downloadMetrics.MarkDownloadCompleted();
                }
            }
            finally
            {
                // Delay CTS disposal to avoid race with monitoring thread
                // Monitoring thread may still be checking this CTS, so schedule disposal after monitoring can complete
                if (_perFileDownloadCancellationTokens != null)
                {
                    if (_perFileDownloadCancellationTokens.TryRemove(fileOffset, out var cts))
                    {
                        // Schedule disposal after delay to allow monitoring thread to finish
                        _ = Task.Run(async () =>
                        {
                            await Task.Delay(CtsDisposalDelay);
                            cts?.Dispose();
                        });
                    }
                }

                // Track cleanup task instead of fire-and-forget to ensure proper shutdown
                if (_activeDownloadMetrics != null && _metricCleanupTasks != null)
                {
                    var cleanupTask = Task.Run(async () =>
                    {
                        try
                        {
                            // Use cancellationToken to respect shutdown - removes immediately if cancelled
                            await Task.Delay(MetricsCleanupDelay, cancellationToken);
                            _activeDownloadMetrics?.TryRemove(fileOffset, out _);
                        }
                        catch (OperationCanceledException)
                        {
                            // Shutdown requested - remove immediately
                            _activeDownloadMetrics?.TryRemove(fileOffset, out _);
                        }
                        catch
                        {
                            // Ignore other exceptions in cleanup task
                        }
                        finally
                        {
                            // Always remove from tracking dictionary
                            _metricCleanupTasks?.TryRemove(fileOffset, out _);
                        }
                    });
                    _metricCleanupTasks[fileOffset] = cleanupTask;
                }
            }
            }, activityName: "DownloadFile");
        }

        private void SetError(Exception ex, Activity? activity = null)
        {
            lock (_errorLock)
            {
                if (_error == null)
                {
                    activity?.AddException(ex, [new("error.context", "cloudfetch.error_state_set")]);
                    _error = ex;
                }
            }
        }

        private void CompleteWithError(Activity? activity = null)
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
                activity?.AddException(ex, [new("error.context", "cloudfetch.complete_with_error_failed")]);
            }
        }

        private async Task MonitorForStragglerDownloadsAsync(CancellationToken cancellationToken)
        {
            await this.TraceActivityAsync(async activity =>
            {
                activity?.SetTag("straggler.monitoring_interval_seconds", 2);
                activity?.SetTag("straggler.enabled", _isStragglerMitigationEnabled);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(StragglerMonitoringInterval, cancellationToken).ConfigureAwait(false);

                        if (_activeDownloadMetrics == null || _stragglerDetector == null || _perFileDownloadCancellationTokens == null)
                        {
                            continue;
                        }

                        // Check for fallback condition
                        if (_stragglerDetector.ShouldFallbackToSequentialDownloads && !_hasTriggeredSequentialDownloadFallback)
                        {
                            _isSequentialMode = true;
                            _hasTriggeredSequentialDownloadFallback = true;
                            activity?.AddEvent("cloudfetch.sequential_fallback_triggered", [
                                new("total_stragglers_in_query", _stragglerDetector.GetTotalStragglersDetectedInQuery()),
                                new("new_parallelism", 1)
                            ]);
                        }

                        // Get snapshot of active downloads
                        var metricsSnapshot = _activeDownloadMetrics.Values.ToList();

                        // Identify stragglers (pass tracking dict to prevent duplicate counting)
                        var stragglerOffsets = _stragglerDetector.IdentifyStragglerDownloads(
                            metricsSnapshot,
                            DateTime.UtcNow,
                            _alreadyCountedStragglers);
                        var stragglerList = stragglerOffsets.ToList();

                        if (stragglerList.Count > 0)
                        {
                            activity?.AddEvent("cloudfetch.straggler_check", [
                                new("active_downloads", metricsSnapshot.Count(m => !m.IsDownloadCompleted)),
                                new("completed_downloads", metricsSnapshot.Count(m => m.IsDownloadCompleted)),
                                new("stragglers_identified", stragglerList.Count)
                            ]);

                            foreach (long offset in stragglerList)
                            {
                                if (_perFileDownloadCancellationTokens.TryGetValue(offset, out var cts))
                                {
                                    activity?.AddEvent("cloudfetch.straggler_cancelling", [
                                        new("offset", offset)
                                    ]);

                                    try
                                    {
                                        cts.Cancel();
                                    }
                                    catch (ObjectDisposedException)
                                    {
                                        // Expected race condition: CTS was disposed between TryGetValue and Cancel
                                        // This is harmless - the download has already completed
                                    }
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected when stopping
                        break;
                    }
                    catch (Exception ex)
                    {
                        activity?.AddException(ex, [new("error.context", "cloudfetch.straggler_monitoring_error")]);
                        // Continue monitoring despite errors
                    }
                }
            }, activityName: "MonitorStragglerDownloads");
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
        // IActivityTracer implementation - delegates to statement
        ActivityTrace IActivityTracer.Trace => _statement.Trace;

        string? IActivityTracer.TraceParent => _statement.TraceParent;

        public string AssemblyVersion => _statement.AssemblyVersion;

        public string AssemblyName => _statement.AssemblyName;
    }
}

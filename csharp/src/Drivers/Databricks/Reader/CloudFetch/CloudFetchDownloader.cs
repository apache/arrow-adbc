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
                    totalFiles++;

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
                                Trace.TraceWarning($"Error waiting for downloads to complete: {ex.Message}");
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

                    // Check if the URL is expired or about to expire
                    if (downloadResult.IsExpiredOrExpiringSoon(_urlExpirationBufferSeconds))
                    {
                        // Get a refreshed URL before starting the download
                        var refreshedLink = await _resultFetcher.GetUrlAsync(downloadResult.Link.StartRowOffset, cancellationToken);
                        if (refreshedLink != null)
                        {
                            // Update the download result with the refreshed link
                            downloadResult.UpdateWithRefreshedLink(refreshedLink);
                            Trace.TraceInformation($"Updated URL for file at offset {refreshedLink.StartRowOffset} before download");
                        }
                    }

                    // Acquire a download slot
                    await _downloadSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                    // Start the download task
                    Task downloadTask = DownloadFileAsync(downloadResult, cancellationToken)
                        .ContinueWith(t =>
                        {
                            // Release the download slot
                            _downloadSemaphore.Release();

                            // Remove the task from the dictionary
                            downloadTasks.TryRemove(t, out _);

                            // Handle any exceptions
                            if (t.IsFaulted)
                            {
                                Exception ex = t.Exception?.InnerException ?? new Exception("Unknown error");
                                Trace.TraceError($"Download failed for file {SanitizeUrl(downloadResult.Link.FileLink)}: {ex.Message}");

                                // Set the download as failed
                                downloadResult.SetFailed(ex);
                                failedDownloads++;

                                // Set the error state to stop the download process
                                SetError(ex);

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
                Trace.TraceInformation("Download process was cancelled");
            }
            catch (Exception ex)
            {
                Trace.TraceError($"Error in download loop: {ex.Message}");
                SetError(ex);
            }
            finally
            {
                overallStopwatch.Stop();

                Trace.TraceInformation(
                    $"Download process completed. Total files: {totalFiles}, Successful: {successfulDownloads}, " +
                    $"Failed: {failedDownloads}, Total size: {totalBytes / 1024.0 / 1024.0:F2} MB, Total time: {overallStopwatch.ElapsedMilliseconds / 1000.0:F2} sec");

                // If there's an error, add the error to the result queue
                if (HasError)
                {
                    CompleteWithError();
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

            // Create a stopwatch to track download time
            var stopwatch = Stopwatch.StartNew();

            // Log download start
            Trace.TraceInformation($"Starting download of file {sanitizedUrl}, expected size: {size / 1024.0:F2} KB");

            // Acquire memory before downloading
            await _memoryManager.AcquireMemoryAsync(size, cancellationToken).ConfigureAwait(false);

            // Retry logic for downloading files
            for (int retry = 0; retry < _maxRetries; retry++)
            {
                try
                {
                    // Download the file directly
                    using HttpResponseMessage response = await _httpClient.GetAsync(
                        url,
                        HttpCompletionOption.ResponseHeadersRead,
                        cancellationToken).ConfigureAwait(false);

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

                            Trace.TraceInformation($"URL for file at offset {refreshedLink.StartRowOffset} was refreshed after expired URL response");

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
                        Trace.TraceInformation($"Actual file size for {sanitizedUrl}: {contentLength.Value / 1024.0 / 1024.0:F2} MB");
                    }

                    // Read the file data
                    fileData = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
                    break; // Success, exit retry loop
                }
                catch (Exception ex) when (retry < _maxRetries - 1 && !cancellationToken.IsCancellationRequested)
                {
                    // Log the error and retry
                    Trace.TraceError($"Error downloading file {SanitizeUrl(url)} (attempt {retry + 1}/{_maxRetries}): {ex.Message}");

                    await Task.Delay(_retryDelayMs * (retry + 1), cancellationToken).ConfigureAwait(false);
                }
            }

            if (fileData == null)
            {
                stopwatch.Stop();
                Trace.TraceError($"Failed to download file {sanitizedUrl} after {_maxRetries} attempts. Elapsed time: {stopwatch.ElapsedMilliseconds} ms");

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

                    Trace.TraceInformation($"Decompressed file {sanitizedUrl} in {decompressStopwatch.ElapsedMilliseconds} ms. Compressed size: {actualSize / 1024.0:F2} KB, Decompressed size: {dataStream.Length / 1024.0:F2} KB");

                    actualSize = dataStream.Length;
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    Trace.TraceError($"Error decompressing data for file {sanitizedUrl}: {ex.Message}. Elapsed time: {stopwatch.ElapsedMilliseconds} ms");

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
            Trace.TraceInformation($"Completed download of file {sanitizedUrl}. Size: {actualSize / 1024.0:F2} KB, Latency: {stopwatch.ElapsedMilliseconds} ms, Throughput: {(actualSize / 1024.0 / 1024.0) / (stopwatch.ElapsedMilliseconds / 1000.0):F2} MB/s");

            // Set the download as completed with the original size
            downloadResult.SetCompleted(dataStream, size);
        }

        private void SetError(Exception ex)
        {
            lock (_errorLock)
            {
                if (_error == null)
                {
                    Trace.TraceError($"Setting error state: {ex.Message}");
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
                Trace.TraceError($"Error completing with error: {ex.Message}");
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

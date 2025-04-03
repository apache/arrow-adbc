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

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch
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
        private readonly int _maxParallelDownloads;
        private readonly bool _isLz4Compressed;
        private readonly int _maxRetries;
        private readonly int _retryDelayMs;
        private readonly SemaphoreSlim _downloadSemaphore;
        private Task? _downloadTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private bool _isCompleted;
        private Exception? _error;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloader"/> class.
        /// </summary>
        /// <param name="downloadQueue">The queue of downloads to process.</param>
        /// <param name="resultQueue">The queue to add completed downloads to.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="httpClient">The HTTP client to use for downloads.</param>
        /// <param name="maxParallelDownloads">The maximum number of parallel downloads.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        /// <param name="maxRetries">The maximum number of retry attempts.</param>
        /// <param name="retryDelayMs">The delay between retry attempts in milliseconds.</param>
        public CloudFetchDownloader(
            BlockingCollection<IDownloadResult> downloadQueue,
            BlockingCollection<IDownloadResult> resultQueue,
            ICloudFetchMemoryBufferManager memoryManager,
            HttpClient httpClient,
            int maxParallelDownloads,
            bool isLz4Compressed,
            int maxRetries = 3,
            int retryDelayMs = 500)
        {
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _resultQueue = resultQueue ?? throw new ArgumentNullException(nameof(resultQueue));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _maxParallelDownloads = maxParallelDownloads > 0 ? maxParallelDownloads : throw new ArgumentOutOfRangeException(nameof(maxParallelDownloads));
            _isLz4Compressed = isLz4Compressed;
            _maxRetries = maxRetries > 0 ? maxRetries : throw new ArgumentOutOfRangeException(nameof(maxRetries));
            _retryDelayMs = retryDelayMs > 0 ? retryDelayMs : throw new ArgumentOutOfRangeException(nameof(retryDelayMs));
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
            catch (Exception ex)
            {
                // If there's an error, set the error state and propagate it
                _error = ex;
                throw;
            }
        }

        private async Task DownloadFilesAsync(CancellationToken cancellationToken)
        {
            await Task.Yield();

            try
            {
                // Keep track of active download tasks
                var downloadTasks = new ConcurrentDictionary<Task, IDownloadResult>();

                // Process items from the download queue until it's completed
                foreach (var downloadResult in _downloadQueue.GetConsumingEnumerable(cancellationToken))
                {
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
                                Debug.WriteLine($"Error waiting for downloads to complete: {ex.Message}");
                            }
                        }

                        // Add the guard to the result queue to signal the end of results
                        _resultQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                        _isCompleted = true;
                        break;
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
                                Debug.WriteLine($"Download failed: {t.Exception?.InnerException?.Message}");
                                downloadResult.SetFailed(t.Exception?.InnerException ?? new Exception("Unknown error"));
                            }
                        }, cancellationToken);

                    // Add the task to the dictionary
                    downloadTasks[downloadTask] = downloadResult;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Expected when cancellation is requested
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error in download loop: {ex.Message}");
                _error = ex;
                
                // Add the guard to the result queue to signal the end of results
                try
                {
                    _resultQueue.Add(EndOfResultsGuard.Instance, CancellationToken.None);
                }
                catch (Exception)
                {
                    // Ignore any errors when adding the guard in case of error
                }
                _isCompleted = true;
            }
        }

        private async Task DownloadFileAsync(IDownloadResult downloadResult, CancellationToken cancellationToken)
        {
            string url = downloadResult.Link.FileLink;
            byte[]? fileData = null;
            long contentLength = 0;

            // Try to get the content length first to reserve memory
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Head, url);
                using var headResponse = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
                headResponse.EnsureSuccessStatusCode();

                if (headResponse.Content.Headers.ContentLength.HasValue)
                {
                    contentLength = headResponse.Content.Headers.ContentLength.Value;
                    
                    // Add a buffer for decompression if needed
                    if (_isLz4Compressed)
                    {
                        // LZ4 compression ratio is typically 2:1 to 5:1, so we'll estimate 3:1
                        contentLength *= 3;
                    }
                }
                else
                {
                    // If we can't determine the content length, use a conservative estimate
                    contentLength = 10 * 1024 * 1024; // 10 MB
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error getting content length: {ex.Message}");
                // If we can't determine the content length, use a conservative estimate
                contentLength = 10 * 1024 * 1024; // 10 MB
            }

            // Acquire memory before downloading
            await _memoryManager.AcquireMemoryAsync(contentLength, cancellationToken).ConfigureAwait(false);

            // Retry logic for downloading files
            for (int retry = 0; retry < _maxRetries; retry++)
            {
                try
                {
                    // Download the file
                    using HttpResponseMessage response = await _httpClient.GetAsync(
                        url, 
                        HttpCompletionOption.ResponseHeadersRead, 
                        cancellationToken).ConfigureAwait(false);
                    
                    response.EnsureSuccessStatusCode();

                    // Get the content length if available
                    long? actualContentLength = response.Content.Headers.ContentLength;
                    if (actualContentLength.HasValue && actualContentLength.Value > 0)
                    {
                        Debug.WriteLine($"Downloading file of size: {actualContentLength.Value / 1024.0 / 1024.0:F2} MB");
                    }

                    // Read the file data
                    fileData = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);
                    break; // Success, exit retry loop
                }
                catch (Exception ex) when (retry < _maxRetries - 1 && !cancellationToken.IsCancellationRequested)
                {
                    // Log the error and retry
                    Debug.WriteLine($"Error downloading file (attempt {retry + 1}/{_maxRetries}): {ex.Message}");
                    await Task.Delay(_retryDelayMs * (retry + 1), cancellationToken).ConfigureAwait(false);
                }
            }

            if (fileData == null)
            {
                // Release the memory we acquired
                _memoryManager.ReleaseMemory(contentLength);
                throw new InvalidOperationException($"Failed to download file from {url} after {_maxRetries} attempts.");
            }

            // Process the downloaded file data
            MemoryStream dataStream;
            long actualSize;

            // If the data is LZ4 compressed, decompress it
            if (_isLz4Compressed)
            {
                try
                {
                    dataStream = new MemoryStream();
                    using (var inputStream = new MemoryStream(fileData))
                    using (var decompressor = LZ4Stream.Decode(inputStream))
                    {
                        await decompressor.CopyToAsync(dataStream, 81920, cancellationToken).ConfigureAwait(false);
                    }
                    dataStream.Position = 0;
                    actualSize = dataStream.Length;
                }
                catch (Exception ex)
                {
                    // Release the memory we acquired
                    _memoryManager.ReleaseMemory(contentLength);
                    throw new InvalidOperationException($"Error decompressing data: {ex.Message}", ex);
                }
            }
            else
            {
                dataStream = new MemoryStream(fileData);
                actualSize = dataStream.Length;
            }

            // If the actual size is different from our estimate, adjust the memory allocation
            if (actualSize != contentLength)
            {
                if (actualSize > contentLength)
                {
                    // We need more memory
                    if (!_memoryManager.TryAcquireMemory(actualSize - contentLength))
                    {
                        // If we can't acquire the additional memory, release what we have and fail
                        _memoryManager.ReleaseMemory(contentLength);
                        dataStream.Dispose();
                        throw new InvalidOperationException($"Not enough memory to store decompressed data of size {actualSize} bytes.");
                    }
                }
                else
                {
                    // We can release some memory
                    _memoryManager.ReleaseMemory(contentLength - actualSize);
                }
            }

            // Set the download as completed
            downloadResult.SetCompleted(dataStream, actualSize);

            // Add the result to the result queue
            _resultQueue.Add(downloadResult, cancellationToken);
        }
    }
}

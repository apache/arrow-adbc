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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Manages the CloudFetch download pipeline.
    /// </summary>
    internal sealed class CloudFetchDownloadManager : ICloudFetchDownloadManager
    {
        // Default values
        private const int DefaultParallelDownloads = 3;
        private const int DefaultPrefetchCount = 2;
        private const int DefaultMemoryBufferSizeMB = 200;
        private const bool DefaultPrefetchEnabled = true;
        private const int DefaultTimeoutMinutes = 5;
        private const int DefaultMaxUrlRefreshAttempts = 3;
        private const int DefaultUrlExpirationBufferSeconds = 60;

        private readonly IHiveServer2Statement _statement;
        private readonly Schema _schema;
        private readonly bool _isLz4Compressed;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly BlockingCollection<IDownloadResult> _resultQueue;
        private readonly ICloudFetchResultFetcher _resultFetcher;
        private readonly ICloudFetchDownloader _downloader;
        private readonly HttpClient _httpClient;
        private bool _isDisposed;
        private bool _isStarted;
        private CancellationTokenSource? _cancellationTokenSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloadManager"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        public CloudFetchDownloadManager(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            TFetchResultsResp? initialResults,
            bool isLz4Compressed,
            HttpClient httpClient)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _isLz4Compressed = isLz4Compressed;

            // Get configuration values from connection properties
            var connectionProps = statement.Connection.Properties;

            // Parse parallel downloads
            int parallelDownloads = DefaultParallelDownloads;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchParallelDownloads, out string? parallelDownloadsStr))
            {
                if (int.TryParse(parallelDownloadsStr, out int parsedParallelDownloads) && parsedParallelDownloads > 0)
                {
                    parallelDownloads = parsedParallelDownloads;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchParallelDownloads}: {parallelDownloadsStr}. Expected a positive integer.");
                }
            }

            // Parse prefetch count
            int prefetchCount = DefaultPrefetchCount;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchPrefetchCount, out string? prefetchCountStr))
            {
                if (int.TryParse(prefetchCountStr, out int parsedPrefetchCount) && parsedPrefetchCount > 0)
                {
                    prefetchCount = parsedPrefetchCount;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchPrefetchCount}: {prefetchCountStr}. Expected a positive integer.");
                }
            }

            // Parse memory buffer size
            int memoryBufferSizeMB = DefaultMemoryBufferSizeMB;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchMemoryBufferSize, out string? memoryBufferSizeStr))
            {
                if (int.TryParse(memoryBufferSizeStr, out int parsedMemoryBufferSize) && parsedMemoryBufferSize > 0)
                {
                    memoryBufferSizeMB = parsedMemoryBufferSize;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchMemoryBufferSize}: {memoryBufferSizeStr}. Expected a positive integer.");
                }
            }

            // Parse max retries
            int maxRetries = 3;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchMaxRetries, out string? maxRetriesStr))
            {
                if (int.TryParse(maxRetriesStr, out int parsedMaxRetries) && parsedMaxRetries > 0)
                {
                    maxRetries = parsedMaxRetries;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchMaxRetries}: {maxRetriesStr}. Expected a positive integer.");
                }
            }

            // Parse retry delay
            int retryDelayMs = 500;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchRetryDelayMs, out string? retryDelayStr))
            {
                if (int.TryParse(retryDelayStr, out int parsedRetryDelay) && parsedRetryDelay > 0)
                {
                    retryDelayMs = parsedRetryDelay;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchRetryDelayMs}: {retryDelayStr}. Expected a positive integer.");
                }
            }

            // Parse timeout minutes
            int timeoutMinutes = DefaultTimeoutMinutes;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchTimeoutMinutes, out string? timeoutStr))
            {
                if (int.TryParse(timeoutStr, out int parsedTimeout) && parsedTimeout > 0)
                {
                    timeoutMinutes = parsedTimeout;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchTimeoutMinutes}: {timeoutStr}. Expected a positive integer.");
                }
            }

            // Parse URL expiration buffer seconds
            int urlExpirationBufferSeconds = DefaultUrlExpirationBufferSeconds;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchUrlExpirationBufferSeconds, out string? urlExpirationBufferStr))
            {
                if (int.TryParse(urlExpirationBufferStr, out int parsedUrlExpirationBuffer) && parsedUrlExpirationBuffer > 0)
                {
                    urlExpirationBufferSeconds = parsedUrlExpirationBuffer;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchUrlExpirationBufferSeconds}: {urlExpirationBufferStr}. Expected a positive integer.");
                }
            }

            // Parse max URL refresh attempts
            int maxUrlRefreshAttempts = DefaultMaxUrlRefreshAttempts;
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchMaxUrlRefreshAttempts, out string? maxUrlRefreshAttemptsStr))
            {
                if (int.TryParse(maxUrlRefreshAttemptsStr, out int parsedMaxUrlRefreshAttempts) && parsedMaxUrlRefreshAttempts > 0)
                {
                    maxUrlRefreshAttempts = parsedMaxUrlRefreshAttempts;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchMaxUrlRefreshAttempts}: {maxUrlRefreshAttemptsStr}. Expected a positive integer.");
                }
            }

            // Initialize the memory manager
            _memoryManager = new CloudFetchMemoryBufferManager(memoryBufferSizeMB);

            // Initialize the queues with bounded capacity
            _downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), prefetchCount * 2);
            _resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), prefetchCount * 2);

            _httpClient = httpClient;
            _httpClient.Timeout = TimeSpan.FromMinutes(timeoutMinutes);

            // Initialize the result fetcher with URL management capabilities
            _resultFetcher = new CloudFetchResultFetcher(
                _statement,
                response,
                initialResults,
                _memoryManager,
                _downloadQueue,
                _statement.BatchSize,
                urlExpirationBufferSeconds);

            // Initialize the downloader
            _downloader = new CloudFetchDownloader(
                _downloadQueue,
                _resultQueue,
                _memoryManager,
                _httpClient,
                _resultFetcher,
                parallelDownloads,
                _isLz4Compressed,
                maxRetries,
                retryDelayMs,
                maxUrlRefreshAttempts,
                urlExpirationBufferSeconds);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchDownloadManager"/> class.
        /// This constructor is intended for testing purposes only.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        /// <param name="resultFetcher">The result fetcher.</param>
        /// <param name="downloader">The downloader.</param>
        internal CloudFetchDownloadManager(
            DatabricksStatement statement,
            Schema schema,
            bool isLz4Compressed,
            ICloudFetchResultFetcher resultFetcher,
            ICloudFetchDownloader downloader)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _isLz4Compressed = isLz4Compressed;
            _resultFetcher = resultFetcher ?? throw new ArgumentNullException(nameof(resultFetcher));
            _downloader = downloader ?? throw new ArgumentNullException(nameof(downloader));

            // Create empty collections for the test
            _memoryManager = new CloudFetchMemoryBufferManager(DefaultMemoryBufferSizeMB);
            _downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            _resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            _httpClient = new HttpClient();
        }

        /// <inheritdoc />
        public bool HasMoreResults => !_downloader.IsCompleted || !_resultQueue.IsCompleted;

        /// <inheritdoc />
        public async Task<IDownloadResult?> GetNextDownloadedFileAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            if (!_isStarted)
            {
                throw new InvalidOperationException("Download manager has not been started.");
            }

            try
            {
                return await _downloader.GetNextDownloadedFileAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (_resultFetcher.HasError)
            {
                throw new AggregateException("Errors in download pipeline", new[] { ex, _resultFetcher.Error! });
            }
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            ThrowIfDisposed();

            if (_isStarted)
            {
                throw new InvalidOperationException("Download manager is already started.");
            }

            // Create a new cancellation token source
            _cancellationTokenSource = new CancellationTokenSource();

            // Start the result fetcher
            await _resultFetcher.StartAsync(_cancellationTokenSource.Token).ConfigureAwait(false);

            // Start the downloader
            await _downloader.StartAsync(_cancellationTokenSource.Token).ConfigureAwait(false);

            _isStarted = true;
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            if (!_isStarted)
            {
                return;
            }

            // Cancel the token to signal all operations to stop
            _cancellationTokenSource?.Cancel();

            // Stop the downloader
            await _downloader.StopAsync().ConfigureAwait(false);

            // Stop the result fetcher
            await _resultFetcher.StopAsync().ConfigureAwait(false);

            // Dispose the cancellation token source
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;

            _isStarted = false;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }

            // Stop the pipeline
            StopAsync().GetAwaiter().GetResult();

            // Dispose the HTTP client
            _httpClient.Dispose();

            // Dispose the cancellation token source if it hasn't been disposed yet
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null;

            // Mark the queues as completed to release any waiting threads
            _downloadQueue.CompleteAdding();
            _resultQueue.CompleteAdding();

            // Dispose any remaining results
            foreach (var result in _resultQueue.GetConsumingEnumerable(CancellationToken.None))
            {
                result.Dispose();
            }

            foreach (var result in _downloadQueue.GetConsumingEnumerable(CancellationToken.None))
            {
                result.Dispose();
            }

            _downloadQueue.Dispose();
            _resultQueue.Dispose();

            _isDisposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(CloudFetchDownloadManager));
            }
        }
    }
}

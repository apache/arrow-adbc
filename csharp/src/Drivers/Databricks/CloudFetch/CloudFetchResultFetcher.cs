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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch
{
    /// <summary>
    /// Fetches result chunks from the Thrift server and manages URL caching and refreshing.
    /// </summary>
    internal class CloudFetchResultFetcher : ICloudFetchResultFetcher
    {
        private readonly IHiveServer2Statement _statement;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly SemaphoreSlim _fetchLock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<long, TSparkArrowResultLink> _urlsByOffset = new ConcurrentDictionary<long, TSparkArrowResultLink>();
        private readonly int _expirationBufferSeconds;
        private readonly IClock _clock;
        private long _startOffset;
        private long _lastFetchedOffset = 0;
        private bool _hasMoreResults;
        private bool _isCompleted;
        private Task? _fetchTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private Exception? _error;
        private long _batchSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchResultFetcher"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement interface.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The queue to add download tasks to.</param>
        /// <param name="batchSize">The number of rows to fetch in each batch.</param>
        /// <param name="expirationBufferSeconds">Buffer time in seconds before URL expiration to trigger refresh.</param>
        /// <param name="clock">Clock implementation for time operations. If null, uses system clock.</param>
        public CloudFetchResultFetcher(
            IHiveServer2Statement statement,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            long batchSize,
            int expirationBufferSeconds = 60,
            IClock? clock = null)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _batchSize = batchSize;
            _expirationBufferSeconds = expirationBufferSeconds;
            _clock = clock ?? new SystemClock();
            _hasMoreResults = true;
            _isCompleted = false;
        }

        /// <inheritdoc />
        public bool HasMoreResults => _hasMoreResults;

        /// <inheritdoc />
        public bool IsCompleted => _isCompleted;

        /// <inheritdoc />
        public bool HasError => _error != null;

        /// <inheritdoc />
        public Exception? Error => _error;

        /// <inheritdoc />
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_fetchTask != null)
            {
                throw new InvalidOperationException("Fetcher is already running.");
            }

            // Reset state
            _startOffset = 0;
            _lastFetchedOffset = 0;
            _hasMoreResults = true;
            _isCompleted = false;
            _error = null;
            _urlsByOffset.Clear();

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _fetchTask = FetchResultsAsync(_cancellationTokenSource.Token);

            // Wait for the fetch task to start
            await Task.Yield();
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            if (_fetchTask == null)
            {
                return;
            }

            _cancellationTokenSource?.Cancel();

            try
            {
                await _fetchTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error stopping fetcher: {ex.Message}");
            }
            finally
            {
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
                _fetchTask = null;
            }
        }

        /// <summary>
        /// Gets a URL for the specified offset, fetching or refreshing as needed.
        /// </summary>
        /// <param name="offset">The row offset for which to get a URL.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The URL link for the specified offset, or null if not available.</returns>
        public async Task<TSparkArrowResultLink?> GetUrlAsync(long offset, CancellationToken cancellationToken)
        {
            // Need to fetch or refresh the URL
            await _fetchLock.WaitAsync(cancellationToken);
            try
            {
                // Determine if we need to fetch new URLs or refresh existing ones
                if (!_urlsByOffset.ContainsKey(offset) && _hasMoreResults)
                {
                    // This is a new offset we haven't seen before - fetch new URLs
                    var links = await FetchUrlBatchAsync(offset, 100, cancellationToken);
                    return links.FirstOrDefault(l => l.StartRowOffset == offset);
                }
                else
                {
                    // We have the URL but it's expired - refresh it
                    return await RefreshUrlAsync(offset, cancellationToken);
                }
            }
            finally
            {
                _fetchLock.Release();
            }
        }

        /// <summary>
        /// Checks if any URLs are expired or about to expire.
        /// </summary>
        /// <returns>True if any URLs are expired or about to expire, false otherwise.</returns>
        public bool HasExpiredOrExpiringSoonUrls()
        {
            return _urlsByOffset.Values.Any(IsUrlExpiredOrExpiringSoon);
        }

        /// <summary>
        /// Proactively refreshes URLs that are expired or about to expire.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        public async Task RefreshExpiredUrlsAsync(CancellationToken cancellationToken)
        {
            // Find the earliest offset that needs refreshing
            long? earliestExpiredOffset = null;

            foreach (var entry in _urlsByOffset)
            {
                if (IsUrlExpiredOrExpiringSoon(entry.Value))
                {
                    if (!earliestExpiredOffset.HasValue || entry.Key < earliestExpiredOffset.Value)
                    {
                        earliestExpiredOffset = entry.Key;
                    }
                }
            }

            if (earliestExpiredOffset.HasValue)
            {
                await _fetchLock.WaitAsync(cancellationToken);
                try
                {
                    Trace.TraceInformation($"Proactively refreshing URLs starting from offset {earliestExpiredOffset.Value}");
                    await FetchUrlBatchAsync(earliestExpiredOffset.Value, 100, cancellationToken);
                }
                finally
                {
                    _fetchLock.Release();
                }
            }
        }

        /// <summary>
        /// Checks if a URL is expired or will expire soon.
        /// </summary>
        public bool IsUrlExpiredOrExpiringSoon(TSparkArrowResultLink link)
        {
            // Convert expiry time to DateTime
            var expiryTime = DateTimeOffset.FromUnixTimeMilliseconds(link.ExpiryTime).UtcDateTime;

            // Check if the URL is already expired or will expire soon
            return _clock.UtcNow.AddSeconds(_expirationBufferSeconds) >= expiryTime;
        }

        /// <summary>
        /// Gets all currently cached URLs.
        /// </summary>
        /// <returns>A dictionary mapping offsets to their URL links.</returns>
        public Dictionary<long, TSparkArrowResultLink> GetAllCachedUrls()
        {
            return new Dictionary<long, TSparkArrowResultLink>(_urlsByOffset);
        }

        /// <summary>
        /// Clears all cached URLs.
        /// </summary>
        public void ClearCache()
        {
            _urlsByOffset.Clear();
            _lastFetchedOffset = 0;
        }

        private async Task FetchResultsAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Process direct results first, if available
                if (_statement.HasDirectResults && _statement.DirectResults?.ResultSet?.Results?.ResultLinks?.Count > 0)
                {
                    // Yield execution so the download queue doesn't get blocked before downloader is started
                    await Task.Yield();
                    ProcessDirectResultsAsync(cancellationToken);
                }

                // Continue fetching as needed
                while (_hasMoreResults && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Fetch more results from the server
                        await FetchNextResultBatchAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        // Expected when cancellation is requested
                        break;
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"Error fetching results: {ex.Message}");
                        _error = ex;
                        _hasMoreResults = false;
                        throw;
                    }
                }

                // Add the end of results guard to the queue
                _downloadQueue.Add(EndOfResultsGuard.Instance, cancellationToken);
                _isCompleted = true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Unhandled error in fetcher: {ex.Message}");
                _error = ex;
                _hasMoreResults = false;
                _isCompleted = true;

                // Add the end of results guard to the queue even in case of error
                try
                {
                    _downloadQueue.Add(EndOfResultsGuard.Instance, CancellationToken.None);
                }
                catch (Exception)
                {
                    // Ignore any errors when adding the guard in case of error
                }
            }
        }

        private async Task FetchNextResultBatchAsync(CancellationToken cancellationToken)
        {
            // Create fetch request
            TFetchResultsReq request = new TFetchResultsReq(_statement.OperationHandle!, TFetchOrientation.FETCH_NEXT, _batchSize);

            // Set the start row offset if we have processed some links already
            if (_startOffset > 0)
            {
                request.StartRowOffset = _startOffset;
            }

            // Fetch results
            TFetchResultsResp response;
            try
            {
                response = await _statement.Client.FetchResults(request, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error fetching results from server: {ex.Message}");
                _hasMoreResults = false;
                throw;
            }

            // Check if we have URL-based results
            if (response.Results.__isset.resultLinks &&
                response.Results.ResultLinks != null &&
                response.Results.ResultLinks.Count > 0)
            {
                List<TSparkArrowResultLink> resultLinks = response.Results.ResultLinks;

                // Cache the URLs
                foreach (var link in resultLinks)
                {
                    _urlsByOffset[link.StartRowOffset] = link;

                    // Update the last fetched offset
                    _lastFetchedOffset = Math.Max(_lastFetchedOffset, link.StartRowOffset + link.RowCount);
                }

                // Add each link to the download queue
                foreach (var link in resultLinks)
                {
                    var downloadResult = new DownloadResult(link, _memoryManager);
                    _downloadQueue.Add(downloadResult, cancellationToken);
                }

                // Update the start offset for the next fetch
                if (resultLinks.Count > 0)
                {
                    var lastLink = resultLinks[resultLinks.Count - 1];
                    _startOffset = lastLink.StartRowOffset + lastLink.RowCount;
                }

                // Update whether there are more results
                _hasMoreResults = response.HasMoreRows;
            }
            else
            {
                // No more results
                _hasMoreResults = false;
            }
        }

        private void ProcessDirectResultsAsync(CancellationToken cancellationToken)
        {
            List<TSparkArrowResultLink> resultLinks = _statement.DirectResults!.ResultSet.Results.ResultLinks;

            // Cache the URLs
            foreach (var link in resultLinks)
            {
                _urlsByOffset[link.StartRowOffset] = link;

                // Update the last fetched offset
                _lastFetchedOffset = Math.Max(_lastFetchedOffset, link.StartRowOffset + link.RowCount);
            }

            // Add each link to the download queue
            foreach (var link in resultLinks)
            {
                var downloadResult = new DownloadResult(link, _memoryManager);
                _downloadQueue.Add(downloadResult, cancellationToken);
            }

            // Update the start offset for the next fetch
            if (resultLinks.Count > 0)
            {
                var lastLink = resultLinks[resultLinks.Count - 1];
                _startOffset = lastLink.StartRowOffset + lastLink.RowCount;
            }

            _hasMoreResults = _statement.DirectResults!.ResultSet.HasMoreRows;
        }

        /// <summary>
        /// Fetches a batch of URLs starting from the specified offset.
        /// </summary>
        private async Task<List<TSparkArrowResultLink>> FetchUrlBatchAsync(
            long startOffset, int batchSize, CancellationToken cancellationToken)
        {
            try
            {
                Trace.TraceInformation($"Fetching URL batch starting at offset {startOffset}");

                // Create fetch request
                TFetchResultsReq request = new TFetchResultsReq(
                    _statement.OperationHandle!,
                    TFetchOrientation.FETCH_NEXT,
                    batchSize);

                request.StartRowOffset = startOffset;

                // Fetch results
                TFetchResultsResp response = await _statement.Client.FetchResults(request, cancellationToken);

                // Process the results
                if (response.Status.StatusCode == TStatusCode.SUCCESS_STATUS &&
                    response.Results.__isset.resultLinks &&
                    response.Results.ResultLinks != null &&
                    response.Results.ResultLinks.Count > 0)
                {
                    Trace.TraceInformation($"Received {response.Results.ResultLinks.Count} URLs in batch");

                    // Update our cached URLs
                    foreach (var link in response.Results.ResultLinks)
                    {
                        _urlsByOffset[link.StartRowOffset] = link;

                        // Update the last fetched offset
                        _lastFetchedOffset = Math.Max(_lastFetchedOffset, link.StartRowOffset + link.RowCount);
                    }

                    // Update whether we have more results
                    _hasMoreResults = response.HasMoreRows;

                    return response.Results.ResultLinks;
                }
                else
                {
                    Trace.TraceWarning("No URLs received in batch");
                    _hasMoreResults = false;
                    return new List<TSparkArrowResultLink>();
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError($"Error fetching URL batch: {ex.Message}");
                return new List<TSparkArrowResultLink>();
            }
        }

        /// <summary>
        /// Refreshes a specific URL.
        /// </summary>
        private async Task<TSparkArrowResultLink?> RefreshUrlAsync(long offset, CancellationToken cancellationToken)
        {
            try
            {
                Trace.TraceInformation($"Refreshing URL for offset {offset}");

                // Create fetch request for the specific offset
                TFetchResultsReq request = new TFetchResultsReq(
                    _statement.OperationHandle!,
                    TFetchOrientation.FETCH_NEXT,
                    1);

                request.StartRowOffset = offset;

                // Fetch results
                TFetchResultsResp response = await _statement.Client.FetchResults(request, cancellationToken);

                // Process the results
                if (response.Status.StatusCode == TStatusCode.SUCCESS_STATUS &&
                    response.Results.__isset.resultLinks &&
                    response.Results.ResultLinks != null &&
                    response.Results.ResultLinks.Count > 0)
                {
                    var refreshedLink = response.Results.ResultLinks.FirstOrDefault(l => l.StartRowOffset == offset);
                    if (refreshedLink != null)
                    {
                        Trace.TraceInformation($"Successfully refreshed URL for offset {offset}");
                        _urlsByOffset[offset] = refreshedLink;
                        return refreshedLink;
                    }
                }

                Trace.TraceWarning($"Failed to refresh URL for offset {offset}");
                return null;
            }
            catch (Exception ex)
            {
                Trace.TraceError($"Error refreshing URL: {ex.Message}");
                return null;
            }
        }
    }
}

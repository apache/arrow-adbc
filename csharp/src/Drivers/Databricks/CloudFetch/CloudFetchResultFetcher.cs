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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Databricks.CloudFetch
{
    /// <summary>
    /// Fetches result chunks from the Thrift server.
    /// </summary>
    internal sealed class CloudFetchResultFetcher : ICloudFetchResultFetcher
    {
        private readonly DatabricksStatement _statement;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private long _startOffset;
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
        /// <param name="prefetchCount">The number of result chunks to prefetch.</param>
        public CloudFetchResultFetcher(
            DatabricksStatement statement,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            long batchSize)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _hasMoreResults = true;
            _isCompleted = false;
            _batchSize = batchSize;
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
            _hasMoreResults = true;
            _isCompleted = false;
            _error = null;

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
                        break;
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
                // Use thread-safe method to fetch results
                response = await _statement.FetchResultsAsync(request, cancellationToken).ConfigureAwait(false);
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
    }
}

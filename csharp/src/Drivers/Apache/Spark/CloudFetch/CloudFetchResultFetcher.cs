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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch
{
    /// <summary>
    /// Fetches result chunks from the Thrift server.
    /// </summary>
    internal sealed class CloudFetchResultFetcher : ICloudFetchResultFetcher
    {
        private readonly IHiveServer2StatementProperties _statement;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly int _prefetchCount;
        private readonly SemaphoreSlim _refreshSemaphore = new SemaphoreSlim(1, 1);
        private long _startOffset;
        private long _currentBatchOffset;
        private long _currentBatchSize;
        private bool _hasMoreResults;
        private bool _isCompleted;
        private Task? _fetchTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private Exception? _error;
        private DateTime _lastRefreshTime = DateTime.MinValue;
        private Dictionary<long, TSparkArrowResultLink> _lastRefreshedLinks = new Dictionary<long, TSparkArrowResultLink>();

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchResultFetcher"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The queue to add download tasks to.</param>
        /// <param name="prefetchCount">The number of result chunks to prefetch.</param>
        public CloudFetchResultFetcher(
            HiveServer2Statement statement,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            int prefetchCount) : this(
                new HiveServer2StatementAdapter(statement),
                memoryManager,
                downloadQueue,
                prefetchCount)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchResultFetcher"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement interface.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        /// <param name="downloadQueue">The queue to add download tasks to.</param>
        /// <param name="prefetchCount">The number of result chunks to prefetch.</param>
        public CloudFetchResultFetcher(
            IHiveServer2StatementProperties statement,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            int prefetchCount)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _downloadQueue = downloadQueue ?? throw new ArgumentNullException(nameof(downloadQueue));
            _prefetchCount = prefetchCount > 0 ? prefetchCount : throw new ArgumentOutOfRangeException(nameof(prefetchCount));
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
            _currentBatchOffset = 0;
            _currentBatchSize = _statement.BatchSize;
            _hasMoreResults = true;
            _isCompleted = false;
            _error = null;
            _lastRefreshTime = DateTime.MinValue;
            _lastRefreshedLinks = new Dictionary<long, TSparkArrowResultLink>();

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

        /// <inheritdoc />
        public async Task<TSparkArrowResultLink?> RefreshLinkAsync(long startRowOffset, CancellationToken cancellationToken)
        {
            // Acquire the semaphore to prevent concurrent refreshes
            await _refreshSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                // Check if we've refreshed recently (within the last few seconds)
                if ((DateTime.UtcNow - _lastRefreshTime).TotalSeconds < 5 && 
                    _lastRefreshedLinks.TryGetValue(startRowOffset, out var cachedLink))
                {
                    return cachedLink;
                }
                
                // If the requested offset is not in our current batch, we need to fetch a new batch
                if (startRowOffset < _currentBatchOffset || startRowOffset >= _currentBatchOffset + _currentBatchSize)
                {
                    // Create fetch request for the specific offset
                    TFetchResultsReq request = new TFetchResultsReq(_statement.OperationHandle!, TFetchOrientation.FETCH_ABSOLUTE, _statement.BatchSize);
                    request.StartRowOffset = startRowOffset;
                    
                    // Fetch results
                    TFetchResultsResp response;
                    try
                    {
                        // Check if we're using a test statement (for unit testing)
                        if (_statement is ITestableHiveServer2Statement testStatement)
                        {
                            response = await testStatement.FetchResults(request, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            response = await _statement.Client.FetchResults(request, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"Error refreshing results from server: {ex.Message}");
                        throw;
                    }
                    
                    // Check if we have URL-based results
                    if (response.Results.__isset.resultLinks &&
                        response.Results.ResultLinks != null &&
                        response.Results.ResultLinks.Count > 0)
                    {
                        List<TSparkArrowResultLink> resultLinks = response.Results.ResultLinks;
                        
                        // Update our cache
                        _lastRefreshTime = DateTime.UtcNow;
                        _lastRefreshedLinks = new Dictionary<long, TSparkArrowResultLink>();
                        foreach (var link in resultLinks)
                        {
                            _lastRefreshedLinks[link.StartRowOffset] = link;
                        }
                        
                        // Try to find the matching link by startRowOffset
                        if (_lastRefreshedLinks.TryGetValue(startRowOffset, out var newLink))
                        {
                            return newLink;
                        }
                    }
                    
                    return null;
                }
                
                // Refresh the current batch
                var refreshedLinks = await RefreshCurrentBatchAsync(cancellationToken).ConfigureAwait(false);
                
                // Try to find the matching link by startRowOffset
                if (refreshedLinks.TryGetValue(startRowOffset, out var matchedLink))
                {
                    return matchedLink;
                }
                
                return null;
            }
            finally
            {
                _refreshSemaphore.Release();
            }
        }

        /// <inheritdoc />
        public async Task<Dictionary<long, TSparkArrowResultLink>> RefreshCurrentBatchAsync(CancellationToken cancellationToken)
        {
            // Create fetch request for the current batch
            TFetchResultsReq request = new TFetchResultsReq(_statement.OperationHandle!, TFetchOrientation.FETCH_NEXT, _statement.BatchSize);
            
            // Set the start row offset to the beginning of the current batch
            request.StartRowOffset = _currentBatchOffset;

            // Fetch results
            TFetchResultsResp response;
            try
            {
                // Check if we're using a test statement (for unit testing)
                if (_statement is ITestableHiveServer2Statement testStatement)
                {
                    response = await testStatement.FetchResults(request, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    response = await _statement.Client.FetchResults(request, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error refreshing results from server: {ex.Message}");
                throw;
            }

            // Create a mapping from startRowOffset to the new link
            var refreshedLinks = new Dictionary<long, TSparkArrowResultLink>();
            
            // Check if we have URL-based results
            if (response.Results.__isset.resultLinks &&
                response.Results.ResultLinks != null &&
                response.Results.ResultLinks.Count > 0)
            {
                List<TSparkArrowResultLink> resultLinks = response.Results.ResultLinks;

                // Create a mapping from startRowOffset to the new link
                foreach (var link in resultLinks)
                {
                    refreshedLinks[link.StartRowOffset] = link;
                }
                
                // Update our cache
                _lastRefreshTime = DateTime.UtcNow;
                _lastRefreshedLinks = refreshedLinks;
            }
            
            return refreshedLinks;
        }

        private async Task FetchResultsAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Initial prefetch
                if (_prefetchCount > 0 && _hasMoreResults && !cancellationToken.IsCancellationRequested)
                {
                    // Perform initial prefetch of multiple batches
                    for (int i = 0; i < _prefetchCount && _hasMoreResults && !cancellationToken.IsCancellationRequested; i++)
                    {
                        try
                        {
                            await FetchNextResultBatchAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            // Expected when cancellation is requested
                            break;
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine($"Error during initial prefetch: {ex.Message}");
                            _error = ex;
                            _hasMoreResults = false;
                            break;
                        }
                    }
                }
                
                // Continue fetching as needed
                while (_hasMoreResults && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Check if we need to fetch more results
                        // We want to keep the queue filled with at least _prefetchCount items
                        if (_downloadQueue.Count >= _prefetchCount)
                        {
                            // Wait a bit before checking again
                            await Task.Delay(10, cancellationToken).ConfigureAwait(false);
                            continue;
                        }

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
            // Store the current batch offset for potential refreshes
            _currentBatchOffset = _startOffset;
            _currentBatchSize = _statement.BatchSize;
            
            // Create fetch request
            TFetchResultsReq request = new TFetchResultsReq(_statement.OperationHandle!, TFetchOrientation.FETCH_NEXT, _statement.BatchSize);

            // Set the start row offset if we have processed some links already
            if (_startOffset > 0)
            {
                request.StartRowOffset = _startOffset;
            }

            // Fetch results
            TFetchResultsResp response;
            try
            {
                // Check if we're using a test statement (for unit testing)
                if (_statement is ITestableHiveServer2Statement testStatement)
                {
                    response = await testStatement.FetchResults(request, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    response = await _statement.Client.FetchResults(request, cancellationToken).ConfigureAwait(false);
                }
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

                // Update our cache with the fresh links
                _lastRefreshTime = DateTime.UtcNow;
                _lastRefreshedLinks = new Dictionary<long, TSparkArrowResultLink>();
                foreach (var link in resultLinks)
                {
                    _lastRefreshedLinks[link.StartRowOffset] = link;
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
    }

    /// <summary>
    /// Adapter class to implement IHiveServer2StatementProperties from HiveServer2Statement
    /// </summary>
    internal class HiveServer2StatementAdapter : IHiveServer2StatementProperties
    {
        private readonly HiveServer2Statement _statement;

        public HiveServer2StatementAdapter(HiveServer2Statement statement)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
        }

        public TOperationHandle? OperationHandle => _statement.OperationHandle;

        public long BatchSize => _statement.BatchSize;

        public TCLIService.Client Client => _statement.Connection.Client;
    }
}
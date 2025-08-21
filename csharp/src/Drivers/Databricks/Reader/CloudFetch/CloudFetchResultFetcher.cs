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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Fetches result chunks from the Thrift server and manages URL caching and refreshing.
    /// </summary>
    internal class CloudFetchResultFetcher : ICloudFetchResultFetcher
    {
        private readonly IHiveServer2Statement _statement;
        private readonly IResponse _response;
        private readonly TFetchResultsResp? _initialResults;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly SemaphoreSlim _fetchLock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<long, IDownloadResult> _urlsByOffset = new ConcurrentDictionary<long, IDownloadResult>();
        private readonly int _expirationBufferSeconds;
        private readonly IClock _clock;
        private long _startOffset;
        private bool _hasMoreResults;
        private bool _isCompleted;
        private Task? _fetchTask;
        private CancellationTokenSource? _cancellationTokenSource;
        private Exception? _error;

        #region Debug Helpers

        private void WriteCloudFetchDebug(string message)
        {
            try
            {
                var debugFile = System.IO.Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "adbc-cloudfetch-debug.log"
                );
                var timestamped = $"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] CloudFetchResultFetcher: {message}";
                System.IO.File.AppendAllText(debugFile, timestamped + Environment.NewLine);
            }
            catch
            {
                // Ignore any debug logging errors to avoid interfering with main functionality
            }
        }

        #endregion
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
            IResponse response,
            TFetchResultsResp? initialResults,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            long batchSize,
            int expirationBufferSeconds = 60,
            IClock? clock = null)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _response = response;
            _initialResults = initialResults;
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
            WriteCloudFetchDebug($"🎬 CloudFetchResultFetcher.StartAsync CALLED - About to start URL fetching process");

            if (_fetchTask != null)
            {
                WriteCloudFetchDebug($"❌ CloudFetchResultFetcher.StartAsync: Fetcher is already running!");
                throw new InvalidOperationException("Fetcher is already running.");
            }

            // Reset state
            _startOffset = 0;
            _hasMoreResults = true;
            _isCompleted = false;
            _error = null;
            _urlsByOffset.Clear();

            WriteCloudFetchDebug($"🔧 CloudFetchResultFetcher.StartAsync: State reset, about to create fetch task");

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _fetchTask = FetchResultsAsync(_cancellationTokenSource.Token);

            WriteCloudFetchDebug($"✅ CloudFetchResultFetcher.StartAsync: Fetch task created, yielding control");

            // Wait for the fetch task to start
            await Task.Yield();

            WriteCloudFetchDebug($"🚀 CloudFetchResultFetcher.StartAsync: COMPLETED - Fetch task should now be running");
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
        public async Task<TSparkArrowResultLink?> GetUrlAsync(long offset, CancellationToken cancellationToken)
        {
            // Check if we have a non-expired URL in the cache
            if (_urlsByOffset.TryGetValue(offset, out var cachedResult) && !cachedResult.IsExpiredOrExpiringSoon(_expirationBufferSeconds))
            {
                return cachedResult.Link;
            }

            // Need to fetch or refresh the URL
            await _fetchLock.WaitAsync(cancellationToken);
            try
            {
                // Create fetch request for the specific offset
                TFetchResultsReq request = new TFetchResultsReq(
                    _response.OperationHandle!,
                    TFetchOrientation.FETCH_NEXT,
                    1);

                request.StartRowOffset = offset;

                // Cancelling mid-request breaks the client; Dispose() should not break the underlying client
                CancellationToken expiringToken = ApacheUtility.GetCancellationToken(_statement.QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);

                // Fetch results
                TFetchResultsResp response = await _statement.Client.FetchResults(request, expiringToken);

                // Process the results
                if (response.Status.StatusCode == TStatusCode.SUCCESS_STATUS &&
                    response.Results.__isset.resultLinks &&
                    response.Results.ResultLinks != null &&
                    response.Results.ResultLinks.Count > 0)
                {
                    var refreshedLink = response.Results.ResultLinks.FirstOrDefault(l => l.StartRowOffset == offset);
                    if (refreshedLink != null)
                    {
                        Trace.TraceInformation($"Successfully fetched URL for offset {offset}");

                        // Create a download result for the refreshed link
                        var downloadResult = new DownloadResult(refreshedLink, _memoryManager);
                        _urlsByOffset[offset] = downloadResult;

                        return refreshedLink;
                    }
                }

                System.Diagnostics.Trace.TraceWarning($"Failed to fetch URL for offset {offset}");
                return null;
            }
            finally
            {
                _fetchLock.Release();
            }
        }

        /// <summary>
        /// Gets all currently cached URLs.
        /// </summary>
        /// <returns>A dictionary mapping offsets to their URL links.</returns>
        public Dictionary<long, TSparkArrowResultLink> GetAllCachedUrls()
        {
            return _urlsByOffset.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Link);
        }

        /// <summary>
        /// Clears all cached URLs.
        /// </summary>
        public void ClearCache()
        {
            _urlsByOffset.Clear();
        }

        private async Task FetchResultsAsync(CancellationToken cancellationToken)
        {
            int totalLinksRetrieved = 0;
            int fetchIteration = 0;

            WriteCloudFetchDebug($"🚀 CloudFetchResultFetcher.FetchResultsAsync STARTED - Will retrieve cloud file links from server");
            WriteCloudFetchDebug($"📊 CloudFetchResultFetcher: Initial state - has_more_results={_hasMoreResults}, is_completed={_isCompleted}");

            try
            {
                // Process direct results first, if available
                if ((_statement.TryGetDirectResults(_response, out TSparkDirectResults? directResults)
                    && directResults!.ResultSet?.Results?.ResultLinks?.Count > 0)
                    || _initialResults?.Results?.ResultLinks?.Count > 0)
                {
                    // Count links from direct results and initial results
                    int directResultLinks = directResults?.ResultSet?.Results?.ResultLinks?.Count ?? 0;
                    int initialResultLinks = _initialResults?.Results?.ResultLinks?.Count ?? 0;
                    int directLinksTotal = directResultLinks + initialResultLinks;
                    totalLinksRetrieved += directLinksTotal;

                    WriteCloudFetchDebug($"🔗 DIRECT RESULTS PROCESSING - Found {directLinksTotal} cloud file links (direct: {directResultLinks}, initial: {initialResultLinks})");
                    WriteCloudFetchDebug($"📊 TOTAL LINKS SO FAR: {totalLinksRetrieved}");

                    // Yield execution so the download queue doesn't get blocked before downloader is started
                    await Task.Yield();
                    ProcessDirectResultsAsync(cancellationToken);
                }

                // Continue fetching as needed
                WriteCloudFetchDebug($"🔄 CloudFetchResultFetcher: About to enter main fetch loop - has_more_results={_hasMoreResults}, cancellation_requested={cancellationToken.IsCancellationRequested}");
                while (_hasMoreResults && !cancellationToken.IsCancellationRequested)
                {
                    fetchIteration++;
                    int downloadQueueCountBefore = _downloadQueue.Count;

                    try
                    {
                        WriteCloudFetchDebug($"🔄 ITERATION #{fetchIteration} - About to fetch next result batch from server");
                        WriteCloudFetchDebug($"📊 Download queue count before fetch: {downloadQueueCountBefore}");

                        // Fetch more results from the server
                        await FetchNextResultBatchAsync(null, cancellationToken).ConfigureAwait(false);

                        int downloadQueueCountAfter = _downloadQueue.Count;
                        int newLinksThisIteration = downloadQueueCountAfter - downloadQueueCountBefore;
                        totalLinksRetrieved += newLinksThisIteration;

                        WriteCloudFetchDebug($"✅ ITERATION #{fetchIteration} COMPLETED - Added {newLinksThisIteration} new cloud file links");
                        WriteCloudFetchDebug($"📊 Download queue count after fetch: {downloadQueueCountAfter}");
                        WriteCloudFetchDebug($"🗃️ TOTAL LINKS RETRIEVED SO FAR: {totalLinksRetrieved} (across {fetchIteration} fetch iterations)");
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        WriteCloudFetchDebug($"❌ ITERATION #{fetchIteration} CANCELLED - Operation was cancelled");
                        // Expected when cancellation is requested
                        break;
                    }
                    catch (Exception ex)
                    {
                        WriteCloudFetchDebug($"❌ ITERATION #{fetchIteration} FAILED - Error fetching results: {ex.Message}");
                        Debug.WriteLine($"Error fetching results: {ex.Message}");
                        _error = ex;
                        _hasMoreResults = false;
                        throw;
                    }
                }

                // Log final summary before adding end guard
                WriteCloudFetchDebug($"🏁 FETCH PROCESS COMPLETED");
                WriteCloudFetchDebug($"🏆 FINAL SUMMARY - Total fetch iterations: {fetchIteration}, Total cloud file links retrieved: {totalLinksRetrieved}");
                if (fetchIteration > 0)
                {
                    double averageLinksPerIteration = (double)totalLinksRetrieved / fetchIteration;
                    WriteCloudFetchDebug($"📊 EFFICIENCY - Average links per iteration: {averageLinksPerIteration:F1}");
                }

                // Add the end of results guard to the queue
                WriteCloudFetchDebug($"🔒 Adding EndOfResultsGuard to download queue");
                _downloadQueue.Add(EndOfResultsGuard.Instance, cancellationToken);

                // CRITICAL FIX: Mark download queue as completed after adding EndOfResultsGuard
                WriteCloudFetchDebug($"🔒 Marking download queue as COMPLETED - no more URLs will be fetched");
                _downloadQueue.CompleteAdding();
                _isCompleted = true;
            }
            catch (Exception ex)
            {
                WriteCloudFetchDebug($"💥 UNHANDLED ERROR in fetcher: {ex.Message}");
                WriteCloudFetchDebug($"🏆 ERROR SUMMARY - Completed {fetchIteration} fetch iterations, Retrieved {totalLinksRetrieved} cloud file links before error");

                Debug.WriteLine($"Unhandled error in fetcher: {ex.Message}");
                _error = ex;
                _hasMoreResults = false;
                _isCompleted = true;

                // Add the end of results guard to the queue even in case of error
                try
                {
                    WriteCloudFetchDebug($"🔒 Adding EndOfResultsGuard to download queue after error");
                    _downloadQueue.TryAdd(EndOfResultsGuard.Instance, 0);

                    WriteCloudFetchDebug($"🔒 Marking download queue as COMPLETED after error");
                    _downloadQueue.CompleteAdding();
                }
                catch (Exception completionEx)
                {
                    WriteCloudFetchDebug($"❌ Failed to add EndOfResultsGuard or complete download queue after error: {completionEx.Message}");
                    // Still try to mark the queue as completed
                    try
                    {
                        _downloadQueue.CompleteAdding();
                    }
                    catch (Exception)
                    {
                        WriteCloudFetchDebug($"❌ Failed to complete download queue after error - this may cause hangs");
                    }
                }
            }
        }

        private async Task FetchNextResultBatchAsync(long? offset, CancellationToken cancellationToken)
        {
            WriteCloudFetchDebug($"🌐 FetchNextResultBatchAsync STARTED - offset={offset}, start_offset={_startOffset}");

            // Create fetch request
            TFetchResultsReq request = new TFetchResultsReq(_response.OperationHandle!, TFetchOrientation.FETCH_NEXT, _batchSize);

            // Set the start row offset
            long startOffset = offset ?? _startOffset;
            if (startOffset > 0)
            {
                request.StartRowOffset = startOffset;
            }

            WriteCloudFetchDebug($"🌐 FetchNextResultBatchAsync: About to call Thrift Client.FetchResults - timeout={_statement.QueryTimeoutSeconds}s");

            // Fetch results
            TFetchResultsResp response;
            try
            {
                // Use the statement's configured query timeout

                using var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(_statement.QueryTimeoutSeconds));
                using var combinedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutTokenSource.Token);

                WriteCloudFetchDebug($"🌐 FetchNextResultBatchAsync: CALLING Thrift _statement.Client.FetchResults() - THIS MAY HANG");
                response = await _statement.Client.FetchResults(request, combinedTokenSource.Token).ConfigureAwait(false);
                WriteCloudFetchDebug($"🌐 FetchNextResultBatchAsync: Thrift Client.FetchResults COMPLETED successfully");
            }
            catch (Exception ex)
            {
                WriteCloudFetchDebug($"❌ FetchNextResultBatchAsync: Error fetching results from server: {ex.Message}");
                Debug.WriteLine($"Error fetching results from server: {ex.Message}");
                _hasMoreResults = false;
                throw;
            }

            WriteCloudFetchDebug($"🌐 FetchNextResultBatchAsync: Processing response - checking for result links");

            // Check if we have URL-based results
            if (response.Results.__isset.resultLinks &&
                response.Results.ResultLinks != null &&
                response.Results.ResultLinks.Count > 0)
            {
                List<TSparkArrowResultLink> resultLinks = response.Results.ResultLinks;
                long maxOffset = 0;

                // Process each link
                foreach (var link in resultLinks)
                {
                    // Create download result
                    var downloadResult = new DownloadResult(link, _memoryManager);

                    // Add to download queue and cache
                    _downloadQueue.Add(downloadResult, cancellationToken);
                    _urlsByOffset[link.StartRowOffset] = downloadResult;

                    // Track the maximum offset for future fetches
                    long endOffset = link.StartRowOffset + link.RowCount;
                    maxOffset = Math.Max(maxOffset, endOffset);
                }

                // Update the start offset for the next fetch
                if (!offset.HasValue)  // Only update if this was a sequential fetch
                {
                    _startOffset = maxOffset;
                }

                // Update whether there are more results
                _hasMoreResults = response.HasMoreRows;
                WriteCloudFetchDebug($"✅ FetchNextResultBatchAsync: Added {resultLinks.Count} result links, has_more_results={_hasMoreResults}");
            }
            else
            {
                // No more results
                _hasMoreResults = false;
                WriteCloudFetchDebug($"🏁 FetchNextResultBatchAsync: No more result links found, has_more_results={_hasMoreResults}");
            }

            WriteCloudFetchDebug($"🌐 FetchNextResultBatchAsync COMPLETED - has_more_results={_hasMoreResults}");
        }

        private void ProcessDirectResultsAsync(CancellationToken cancellationToken)
        {
            TFetchResultsResp fetchResults;
            if (_statement.TryGetDirectResults(_response, out TSparkDirectResults? directResults)
                && directResults!.ResultSet?.Results?.ResultLinks?.Count > 0)
            {
                fetchResults = directResults.ResultSet;
            }
            else
            {
                fetchResults = _initialResults!;
            }

            List<TSparkArrowResultLink> resultLinks = fetchResults.Results.ResultLinks;

            long maxOffset = 0;

            // Process each link
            foreach (var link in resultLinks)
            {
                // Create download result
                var downloadResult = new DownloadResult(link, _memoryManager);

                // Add to download queue and cache
                _downloadQueue.Add(downloadResult, cancellationToken);
                _urlsByOffset[link.StartRowOffset] = downloadResult;

                // Track the maximum offset for future fetches
                long endOffset = link.StartRowOffset + link.RowCount;
                maxOffset = Math.Max(maxOffset, endOffset);
            }

            // Update the start offset for the next fetch
            _startOffset = maxOffset;

            // Update whether there are more results
            _hasMoreResults = fetchResults.HasMoreRows;
        }
    }
}

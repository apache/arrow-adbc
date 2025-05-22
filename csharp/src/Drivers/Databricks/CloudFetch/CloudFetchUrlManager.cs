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
    /// Abstraction for time operations to enable testing with controlled time.
    /// </summary>
    public interface IClock
    {
        /// <summary>
        /// Gets the current UTC time.
        /// </summary>
        DateTime UtcNow { get; }
    }

    /// <summary>
    /// Default implementation that uses system time.
    /// </summary>
    internal class SystemClock : IClock
    {
        public DateTime UtcNow => DateTime.UtcNow;
    }

    /// <summary>
    /// Test implementation that allows controlling time for testing scenarios.
    /// </summary>
    public class ControllableClock : IClock
    {
        private DateTime _currentTime;

        public ControllableClock(DateTime? initialTime = null)
        {
            _currentTime = initialTime ?? DateTime.UtcNow;
        }

        public DateTime UtcNow => _currentTime;

        /// <summary>
        /// Advances the clock by the specified time span.
        /// </summary>
        /// <param name="timeSpan">The amount of time to advance.</param>
        public void AdvanceTime(TimeSpan timeSpan)
        {
            _currentTime = _currentTime.Add(timeSpan);
        }

        /// <summary>
        /// Sets the clock to a specific time.
        /// </summary>
        /// <param name="time">The time to set.</param>
        public void SetTime(DateTime time)
        {
            _currentTime = time;
        }

        /// <summary>
        /// Resets the clock to the current system time.
        /// </summary>
        public void Reset()
        {
            _currentTime = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// Manages CloudFetch URLs, handling both initial fetching and refreshing of expired URLs.
    /// </summary>
    internal class CloudFetchUrlManager
    {
        private readonly IHiveServer2Statement _statement;
        private readonly SemaphoreSlim _fetchLock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<long, TSparkArrowResultLink> _urlsByOffset = new ConcurrentDictionary<long, TSparkArrowResultLink>();
        private readonly int _expirationBufferSeconds;
        private readonly IClock _clock;
        private long _lastFetchedOffset = 0;
        private bool _hasMoreResults = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchUrlManager"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement to use for fetching URLs.</param>
        /// <param name="expirationBufferSeconds">Buffer time in seconds before URL expiration to trigger refresh.</param>
        /// <param name="clock">Clock implementation for time operations. If null, uses system clock.</param>
        public CloudFetchUrlManager(IHiveServer2Statement statement, int expirationBufferSeconds = 60, IClock? clock = null)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _expirationBufferSeconds = expirationBufferSeconds;
            _clock = clock ?? new SystemClock();
        }

        /// <summary>
        /// Gets a URL for the specified offset, fetching or refreshing as needed.
        /// </summary>
        /// <param name="offset">The row offset for which to get a URL.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The URL link for the specified offset, or null if not available.</returns>
        public async Task<TSparkArrowResultLink?> GetUrlAsync(long offset, CancellationToken cancellationToken)
        {
            // Check if we already have a valid URL for this offset
            if (_urlsByOffset.TryGetValue(offset, out var link) && !IsUrlExpiredOrExpiringSoon(link))
            {
                return link;
            }

            // Need to fetch or refresh the URL
            await _fetchLock.WaitAsync(cancellationToken);
            try
            {
                // Check again in case another thread already fetched/refreshed while we were waiting
                if (_urlsByOffset.TryGetValue(offset, out link) && !IsUrlExpiredOrExpiringSoon(link))
                {
                    return link;
                }

                // Determine if we need to fetch new URLs or refresh existing ones
                if (!_urlsByOffset.ContainsKey(offset) && _hasMoreResults)
                {
                    // This is a new offset we haven't seen before - fetch new URLs
                    return await FetchNewUrlsAsync(offset, cancellationToken);
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
        /// Gets URLs for a range of offsets, fetching or refreshing as needed.
        /// </summary>
        /// <param name="startOffset">The starting row offset.</param>
        /// <param name="count">The number of URLs to get.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A dictionary mapping offsets to their URL links.</returns>
        public async Task<Dictionary<long, TSparkArrowResultLink>> GetUrlRangeAsync(
            long startOffset, int count, CancellationToken cancellationToken)
        {
            var result = new Dictionary<long, TSparkArrowResultLink>();

            // First check which URLs we already have that are valid
            var offsetsToFetch = new List<long>();
            for (long i = startOffset; i < startOffset + count; i++)
            {
                if (_urlsByOffset.TryGetValue(i, out var link) && !IsUrlExpiredOrExpiringSoon(link))
                {
                    result[i] = link;
                }
                else
                {
                    offsetsToFetch.Add(i);
                }
            }

            // If we have all URLs and they're valid, return early
            if (offsetsToFetch.Count == 0)
            {
                return result;
            }

            // Need to fetch or refresh some URLs
            await _fetchLock.WaitAsync(cancellationToken);
            try
            {
                // Determine if we need to fetch new URLs or refresh existing ones
                bool allOffsetsAreNew = offsetsToFetch.All(offset => !_urlsByOffset.ContainsKey(offset));

                if (offsetsToFetch.Count > 0 && allOffsetsAreNew && _hasMoreResults)
                {
                    // Fetch new URLs
                    var newLinks = await FetchUrlBatchAsync(offsetsToFetch[0], count, cancellationToken);
                    foreach (var link in newLinks)
                    {
                        result[link.StartRowOffset] = link;
                    }
                }
                else
                {
                    // Refresh existing URLs
                    foreach (var offset in offsetsToFetch)
                    {
                        var refreshedLink = await RefreshUrlAsync(offset, cancellationToken);
                        if (refreshedLink != null)
                        {
                            result[offset] = refreshedLink;
                        }
                    }
                }

                // Fill in any remaining URLs we already have
                for (long i = startOffset; i < startOffset + count; i++)
                {
                    if (!result.ContainsKey(i) && _urlsByOffset.TryGetValue(i, out var link))
                    {
                        result[i] = link;
                    }
                }

                return result;
            }
            finally
            {
                _fetchLock.Release();
            }
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
        /// Checks if any URLs are expired or about to expire.
        /// </summary>
        /// <returns>True if any URLs are expired or about to expire, false otherwise.</returns>
        public bool HasExpiredOrExpiringSoonUrls()
        {
            return _urlsByOffset.Values.Any(IsUrlExpiredOrExpiringSoon);
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
            _hasMoreResults = true;
        }

        /// <summary>
        /// Fetches new URLs starting from the specified offset.
        /// </summary>
        private async Task<TSparkArrowResultLink?> FetchNewUrlsAsync(long offset, CancellationToken cancellationToken)
        {
            var links = await FetchUrlBatchAsync(offset, 100, cancellationToken);
            return links.FirstOrDefault(l => l.StartRowOffset == offset);
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

        /// <summary>
        /// Checks if a URL is expired or will expire soon.
        /// </summary>
        private bool IsUrlExpiredOrExpiringSoon(TSparkArrowResultLink link)
        {
            // Convert expiry time to DateTime
            var expiryTime = DateTimeOffset.FromUnixTimeMilliseconds(link.ExpiryTime).UtcDateTime;

            // Check if the URL is already expired or will expire soon
            return _clock.UtcNow.AddSeconds(_expirationBufferSeconds) >= expiryTime;
        }
    }
}

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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.CloudFetch
{
    /// <summary>
    /// Tests for CloudFetchResultFetcher
    /// </summary>
    public class CloudFetchResultFetcherTest
    {
        private readonly Mock<IHiveServer2Statement> _mockStatement;
        private readonly Mock<IResponse> _mockResponse;
        private readonly Mock<TCLIService.IAsync> _mockClient;
        private readonly MockClock _mockClock;
        private readonly CloudFetchResultFetcherWithMockClock _resultFetcher;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly Mock<ICloudFetchMemoryBufferManager> _mockMemoryManager;

        public CloudFetchResultFetcherTest()
        {
            _mockClient = new Mock<TCLIService.IAsync>();
            _mockStatement = new Mock<IHiveServer2Statement>();
            _mockResponse = CreateResponse();

            _mockStatement.Setup(s => s.Client).Returns(_mockClient.Object);

            // Set a mock querytimeout as 30s
            _mockStatement.Setup(s => s.QueryTimeoutSeconds).Returns(30); // 30 seconds

            _mockClock = new MockClock();
            _downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            _mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();

            _resultFetcher = new CloudFetchResultFetcherWithMockClock(
                _mockStatement.Object,
                _mockResponse.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                100, // batchSize
                _mockClock,
                60); // expirationBufferSeconds
        }

        #region URL Management Tests

        [Fact]
        public async Task GetUrlAsync_FetchesNewUrl_WhenNotCached()
        {
            // Arrange
            long offset = 0;
            var resultLink = CreateTestResultLink(offset, 100, "http://test.com/file1", 3600);
            SetupMockClientFetchResults(new List<TSparkArrowResultLink> { resultLink }, true);

            // Act
            var result = await _resultFetcher.GetUrlAsync(offset, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(offset, result.StartRowOffset);
            Assert.Equal("http://test.com/file1", result.FileLink);
            _mockClient.Verify(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task GetUrlRangeAsync_FetchesMultipleUrls()
        {
            // Arrange
            var resultLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/file1", 3600),
                CreateTestResultLink(100, 100, "http://test.com/file2", 3600),
                CreateTestResultLink(200, 100, "http://test.com/file3", 3600)
            };

            // Set hasMoreRows to false so the fetcher doesn't keep trying to fetch more results
            SetupMockClientFetchResults(resultLinks, false);

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the links and complete
            await Task.Delay(200);

            // Get all cached URLs
            var cachedUrls = _resultFetcher.GetAllCachedUrls();

            // Assert
            Assert.Equal(3, cachedUrls.Count);
            Assert.Equal("http://test.com/file1", cachedUrls[0].FileLink);
            Assert.Equal("http://test.com/file2", cachedUrls[100].FileLink);
            Assert.Equal("http://test.com/file3", cachedUrls[200].FileLink);
            _mockClient.Verify(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()), Times.Once);

            // Verify the fetcher completed
            Assert.True(_resultFetcher.IsCompleted);
            Assert.False(_resultFetcher.HasMoreResults);

            // No need to stop explicitly as it should have completed naturally,
            // but it's good practice to clean up
            await _resultFetcher.StopAsync();
        }

        [Fact]
        public async Task ClearCache_RemovesAllCachedUrls()
        {
            // Arrange
            var resultLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/file1", 3600),
                CreateTestResultLink(100, 100, "http://test.com/file2", 3600)
            };

            // Set hasMoreRows to false so the fetcher doesn't keep trying to fetch more results
            SetupMockClientFetchResults(resultLinks, false);

            // Cache the URLs
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the links and complete
            await Task.Delay(200);

            // Act
            _resultFetcher.ClearCache();
            var cachedUrls = _resultFetcher.GetAllCachedUrls();

            // Assert
            Assert.Empty(cachedUrls);

            // Verify the fetcher completed
            Assert.True(_resultFetcher.IsCompleted);
            Assert.False(_resultFetcher.HasMoreResults);

            // Cleanup
            await _resultFetcher.StopAsync();
        }

        [Fact]
        public async Task GetUrlAsync_RefreshesExpiredUrl()
        {
            // Arrange
            long offset = 0;
            // Create a URL that will expire soon
            var expiredLink = CreateTestResultLink(offset, 100, "http://test.com/expired", 30);
            var refreshedLink = CreateTestResultLink(offset, 100, "http://test.com/refreshed", 3600);

            // First return the expired link, then the refreshed one
            _mockClient.SetupSequence(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateFetchResultsResponse(new List<TSparkArrowResultLink> { expiredLink }, true))
                .ReturnsAsync(CreateFetchResultsResponse(new List<TSparkArrowResultLink> { refreshedLink }, true));

            // First fetch to cache the soon-to-expire URL
            await _resultFetcher.GetUrlAsync(offset, CancellationToken.None);

            // Advance time so the URL is now expired
            _mockClock.AdvanceTime(TimeSpan.FromSeconds(40));

            // Act - This should refresh the URL
            var result = await _resultFetcher.GetUrlAsync(offset, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.Equal("http://test.com/refreshed", result.FileLink);
            _mockClient.Verify(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        #endregion

        #region Core Functionality Tests (Restored)

        [Fact]
        public async Task StartAsync_CalledTwice_ThrowsException()
        {
            // Arrange
            SetupMockClientFetchResults(new List<TSparkArrowResultLink>(), false);

            // Act & Assert
            await _resultFetcher.StartAsync(CancellationToken.None);
            await Assert.ThrowsAsync<InvalidOperationException>(() => _resultFetcher.StartAsync(CancellationToken.None));

            // Cleanup
            await _resultFetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_SuccessfullyFetchesResults()
        {
            // Arrange
            var resultLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/file1", 3600),
                CreateTestResultLink(100, 100, "http://test.com/file2", 3600),
                CreateTestResultLink(200, 100, "http://test.com/file3", 3600)
            };

            SetupMockClientFetchResults(resultLinks, false);

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the results
            await Task.Delay(100);

            // Assert
            // The download queue should contain our result links
            Assert.True(_downloadQueue.Count >= resultLinks.Count,
                $"Expected at least {resultLinks.Count} items in queue, but found {_downloadQueue.Count}");

            // Take all items from the queue and verify they match our result links
            var downloadResults = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                // Skip the end of results guard
                if (result == EndOfResultsGuard.Instance)
                {
                    continue;
                }
                downloadResults.Add(result);
            }

            Assert.Equal(resultLinks.Count, downloadResults.Count);

            // Verify each download result has the correct link
            for (int i = 0; i < resultLinks.Count; i++)
            {
                Assert.Equal(resultLinks[i].FileLink, downloadResults[i].Link.FileLink);
                Assert.Equal(resultLinks[i].StartRowOffset, downloadResults[i].Link.StartRowOffset);
                Assert.Equal(resultLinks[i].RowCount, downloadResults[i].Link.RowCount);
            }

            // Verify the fetcher state
            Assert.False(_resultFetcher.HasMoreResults);
            Assert.True(_resultFetcher.IsCompleted);
            Assert.False(_resultFetcher.HasError);
            Assert.Null(_resultFetcher.Error);

            // Cleanup
            await _resultFetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithMultipleBatches_FetchesAllResults()
        {
            // Arrange
            var firstBatchLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/file1", 3600),
                CreateTestResultLink(100, 100, "http://test.com/file2", 3600)
            };

            var secondBatchLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(200, 100, "http://test.com/file3", 3600),
                CreateTestResultLink(300, 100, "http://test.com/file4", 3600)
            };

            _mockClient.SetupSequence(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateFetchResultsResponse(firstBatchLinks, true))
                .ReturnsAsync(CreateFetchResultsResponse(secondBatchLinks, false));

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process all results
            await Task.Delay(200);

            // Assert
            // The download queue should contain all result links (both batches)
            Assert.True(_downloadQueue.Count >= firstBatchLinks.Count + secondBatchLinks.Count,
                $"Expected at least {firstBatchLinks.Count + secondBatchLinks.Count} items in queue, but found {_downloadQueue.Count}");

            // Take all items from the queue
            var downloadResults = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                // Skip the end of results guard
                if (result == EndOfResultsGuard.Instance)
                {
                    continue;
                }
                downloadResults.Add(result);
            }

            Assert.Equal(firstBatchLinks.Count + secondBatchLinks.Count, downloadResults.Count);

            // Verify the fetcher state
            Assert.False(_resultFetcher.HasMoreResults);
            Assert.True(_resultFetcher.IsCompleted);
            Assert.False(_resultFetcher.HasError);

            // Cleanup
            await _resultFetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithEmptyResults_CompletesGracefully()
        {
            // Arrange
            SetupMockClientFetchResults(new List<TSparkArrowResultLink>(), false);

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the results
            await Task.Delay(100);

            // Assert
            // The download queue should be empty except for the end guard
            var nonGuardItems = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                if (result != EndOfResultsGuard.Instance)
                {
                    nonGuardItems.Add(result);
                }
            }
            Assert.Empty(nonGuardItems);

            // Verify the fetcher state
            Assert.False(_resultFetcher.HasMoreResults);
            Assert.True(_resultFetcher.IsCompleted);
            Assert.False(_resultFetcher.HasError);

            // Cleanup
            await _resultFetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithServerError_SetsErrorState()
        {
            // Arrange
            _mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Test server error"));

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the error
            await Task.Delay(100);

            // Assert
            // Verify the fetcher state
            Assert.False(_resultFetcher.HasMoreResults);
            Assert.True(_resultFetcher.IsCompleted);
            Assert.True(_resultFetcher.HasError);
            Assert.NotNull(_resultFetcher.Error);
            Assert.IsType<InvalidOperationException>(_resultFetcher.Error);

            // The download queue should have the end guard
            Assert.True(_downloadQueue.Count <= 1, "Expected at most 1 item (end guard) in queue");

            // Cleanup
            await _resultFetcher.StopAsync();
        }

        [Fact]
        public async Task StopAsync_CancelsFetching()
        {
            // Arrange
            var fetchStarted = new TaskCompletionSource<bool>();
            var fetchCancelled = new TaskCompletionSource<bool>();

            _mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .Returns(async (TFetchResultsReq req, CancellationToken token) =>
                {
                    fetchStarted.TrySetResult(true);

                    try
                    {
                        // Wait for a long time or until cancellation
                        await Task.Delay(10000, token);
                    }
                    catch (OperationCanceledException)
                    {
                        fetchCancelled.TrySetResult(true);
                        throw;
                    }

                    // Return empty results if not cancelled
                    return CreateFetchResultsResponse(new List<TSparkArrowResultLink>(), false);
                });

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetch to start
            await fetchStarted.Task;

            // Stop the fetcher
            await _resultFetcher.StopAsync();

            // Assert
            // Wait a short time for cancellation to propagate
            var cancelled = await Task.WhenAny(fetchCancelled.Task, Task.Delay(1000)) == fetchCancelled.Task;
            Assert.True(cancelled, "Fetch operation should have been cancelled");

            // Verify the fetcher state
            Assert.True(_resultFetcher.IsCompleted);
        }

        [Fact]
        public async Task StopAsync_Timeout()
        {
            // Arrange
            var fetchStarted = new TaskCompletionSource<bool>();
            var fetchTimedOut = new TaskCompletionSource<bool>();

            // Temporarily override the QueryTimeoutSeconds for this test only
            _mockStatement.Setup(s => s.QueryTimeoutSeconds).Returns(2); // 2 second timeout

            _mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .Returns(async (TFetchResultsReq req, CancellationToken token) =>
                {
                    fetchStarted.TrySetResult(true);

                    try
                    {
                        // Wait longer than the timeout (5 seconds), but DO respond to the timeout token
                        // The timeout token should cancel this after 2 seconds
                        await Task.Delay(5000, token);
                    }
                    catch (OperationCanceledException)
                    {
                        fetchTimedOut.TrySetResult(true);
                        throw;
                    }

                    // This should never be reached due to timeout
                    return CreateFetchResultsResponse(new List<TSparkArrowResultLink>(), false);
                });

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetch to start
            await fetchStarted.Task;

            // Don't call StopAsync - let the timeout mechanism work

            // Assert
            // Wait for timeout to occur (should be within 3-4 seconds)
            var timedOut = await Task.WhenAny(fetchTimedOut.Task, Task.Delay(4000)) == fetchTimedOut.Task;
            Assert.True(timedOut, "Fetch operation should have timed out due to QueryTimeoutSeconds setting");

            // Wait a bit for the fetcher to complete its error handling
            //await Task.Delay(100);

            // Verify the fetcher state
            Assert.True(_resultFetcher.IsCompleted);

            // Clean up
            await _resultFetcher.StopAsync();
        }

        #endregion

        #region Initial Results Tests

        [Fact]
        public async Task InitialResults_ProcessesInitialResultsCorrectly()
        {
            // Arrange
            var initialResultLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/initial1", 3600),
                CreateTestResultLink(100, 100, "http://test.com/initial2", 3600)
            };

            var initialResults = CreateFetchResultsResponse(initialResultLinks, false);
            var fetcherWithInitialResults = CreateResultFetcherWithInitialResults(initialResults);

            // Act
            await fetcherWithInitialResults.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the initial results
            await Task.Delay(200);

            // Assert
            // The download queue should contain our initial result links
            Assert.True(_downloadQueue.Count >= initialResultLinks.Count,
                $"Expected at least {initialResultLinks.Count} items in queue, but found {_downloadQueue.Count}");

            // Take all items from the queue and verify they match our initial result links
            var downloadResults = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                // Skip the end of results guard
                if (result == EndOfResultsGuard.Instance)
                {
                    continue;
                }
                downloadResults.Add(result);
            }

            Assert.Equal(initialResultLinks.Count, downloadResults.Count);

            // Verify each download result has the correct link
            for (int i = 0; i < initialResultLinks.Count; i++)
            {
                Assert.Equal(initialResultLinks[i].FileLink, downloadResults[i].Link.FileLink);
                Assert.Equal(initialResultLinks[i].StartRowOffset, downloadResults[i].Link.StartRowOffset);
                Assert.Equal(initialResultLinks[i].RowCount, downloadResults[i].Link.RowCount);
            }

            // Verify the fetcher completed
            Assert.True(fetcherWithInitialResults.IsCompleted);
            Assert.False(fetcherWithInitialResults.HasMoreResults);

            // Cleanup
            await fetcherWithInitialResults.StopAsync();
        }

        [Fact]
        public async Task InitialResults_WithMoreRows_ContinuesFetching()
        {
            // Arrange
            var initialResultLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/initial1", 3600)
            };

            var additionalResultLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(100, 100, "http://test.com/additional1", 3600)
            };

            // Initial results indicate more rows are available
            var initialResults = CreateFetchResultsResponse(initialResultLinks, true);
            var fetcherWithInitialResults = CreateResultFetcherWithInitialResults(initialResults);

            // Setup mock for additional fetch
            SetupMockClientFetchResults(additionalResultLinks, false);

            // Act
            await fetcherWithInitialResults.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process all results
            await Task.Delay(300);

            // Assert
            // The download queue should contain both initial and additional result links
            var expectedCount = initialResultLinks.Count + additionalResultLinks.Count;
            Assert.True(_downloadQueue.Count >= expectedCount,
                $"Expected at least {expectedCount} items in queue, but found {_downloadQueue.Count}");

            // Take all items from the queue
            var downloadResults = new List<IDownloadResult>();
            while (_downloadQueue.TryTake(out var result))
            {
                // Skip the end of results guard
                if (result == EndOfResultsGuard.Instance)
                {
                    continue;
                }
                downloadResults.Add(result);
            }

            Assert.Equal(expectedCount, downloadResults.Count);

            // Verify the fetcher completed
            Assert.True(fetcherWithInitialResults.IsCompleted);
            Assert.False(fetcherWithInitialResults.HasMoreResults);

            // Cleanup
            await fetcherWithInitialResults.StopAsync();
        }

        private CloudFetchResultFetcherWithMockClock CreateResultFetcherWithInitialResults(TFetchResultsResp initialResults)
        {
            return new CloudFetchResultFetcherWithMockClock(
                _mockStatement.Object,
                _mockResponse.Object,
                initialResults,
                _mockMemoryManager.Object,
                _downloadQueue,
                100, // batchSize
                _mockClock,
                60); // expirationBufferSeconds
        }

        #endregion

        #region Helper Methods

        private TSparkArrowResultLink CreateTestResultLink(long startRowOffset, int rowCount, string fileLink, int expirySeconds)
        {
            return new TSparkArrowResultLink
            {
                StartRowOffset = startRowOffset,
                RowCount = rowCount,
                FileLink = fileLink,
                ExpiryTime = new DateTimeOffset(_mockClock.UtcNow.AddSeconds(expirySeconds)).ToUnixTimeMilliseconds()
            };
        }

        private void SetupMockClientFetchResults(List<TSparkArrowResultLink> resultLinks, bool hasMoreRows)
        {
            var results = new TRowSet { __isset = { resultLinks = true } };
            results.ResultLinks = resultLinks;

            var response = new TFetchResultsResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS },
                HasMoreRows = hasMoreRows,
                Results = results,
                __isset = { results = true, hasMoreRows = true }
            };

            // Clear any previous setups
            _mockClient.Reset();

            // Setup for any fetch request
            _mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);
        }

        private TFetchResultsResp CreateFetchResultsResponse(List<TSparkArrowResultLink> resultLinks, bool hasMoreRows)
        {
            var results = new TRowSet { __isset = { resultLinks = true } };
            results.ResultLinks = resultLinks;

            return new TFetchResultsResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS },
                HasMoreRows = hasMoreRows,
                Results = results,
                __isset = { results = true, hasMoreRows = true }
            };
        }

        private Mock<IResponse> CreateResponse()
        {
            var mockResponse = new Mock<IResponse>();
            mockResponse.Setup(r => r.OperationHandle).Returns(new TOperationHandle
            {
                OperationId = new THandleIdentifier
                {
                    Guid = new byte[16],
                    Secret = new byte[16]
                },
                OperationType = TOperationType.EXECUTE_STATEMENT,
                HasResultSet = true
            });
            return mockResponse;
        }

        #endregion
    }

    /// <summary>
    /// Mock clock implementation for testing time-dependent behavior.
    /// </summary>
    public class MockClock : IClock
    {
        private DateTimeOffset _now;

        public MockClock()
        {
            _now = DateTimeOffset.UtcNow;
        }

        public DateTime UtcNow => _now.UtcDateTime;

        public void AdvanceTime(TimeSpan timeSpan)
        {
            _now = _now.Add(timeSpan);
        }

        public void SetTime(DateTimeOffset time)
        {
            _now = time;
        }
    }

    /// <summary>
    /// Extension of CloudFetchResultFetcher that uses a mock clock for testing.
    /// </summary>
    internal class CloudFetchResultFetcherWithMockClock : CloudFetchResultFetcher
    {
        public CloudFetchResultFetcherWithMockClock(
            IHiveServer2Statement statement,
            IResponse response,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            long batchSize,
            IClock clock,
            int expirationBufferSeconds = 60)
            : base(statement, response, null, memoryManager, downloadQueue, batchSize, expirationBufferSeconds, clock)
        {
        }

        public CloudFetchResultFetcherWithMockClock(
            IHiveServer2Statement statement,
            IResponse response,
            TFetchResultsResp? initialResults,
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            long batchSize,
            IClock clock,
            int expirationBufferSeconds = 60)
            : base(statement, response, initialResults, memoryManager, downloadQueue, batchSize, expirationBufferSeconds, clock)
        {
        }
    }
}

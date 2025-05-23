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
using Apache.Arrow.Adbc.Drivers.Apache.Databricks.Client;
using Apache.Arrow.Adbc.Drivers.Apache.Databricks.CloudFetch;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Databricks.CloudFetch
{
    /// <summary>
    /// Tests for CloudFetchResultFetcher
    /// </summary>
    public class CloudFetchResultFetcherTest
    {
        private readonly Mock<ICloudFetchMemoryBufferManager> _mockMemoryManager;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;

        public CloudFetchResultFetcherTest()
        {
            _mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            _downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
        }

        [Fact]
        public async Task StartAsync_CalledTwice_ThrowsException()
        {
            // Arrange
            var mockClient = new Mock<TCLIService.IAsync>();
            var threadSafeClient = new Mock<ThreadSafeClient>(mockClient.Object);
            threadSafeClient.Setup(c => c.FetchResultsAsync(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateFetchResultsResponse(new List<TSparkArrowResultLink>(), false));

            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.OperationHandle).Returns(CreateOperationHandle());
            mockStatement.Setup(s => s.ThreadSafeClient).Returns(threadSafeClient.Object);

            var fetcher = new CloudFetchResultFetcher(
                mockStatement.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                5); // batchSize

            // Act & Assert
            await fetcher.StartAsync(CancellationToken.None);
            await Assert.ThrowsAsync<InvalidOperationException>(() => fetcher.StartAsync(CancellationToken.None));

            // Cleanup
            await fetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_SuccessfullyFetchesResults()
        {
            // Arrange
            var resultLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/file1"),
                CreateTestResultLink(100, 100, "http://test.com/file2"),
                CreateTestResultLink(200, 100, "http://test.com/file3")
            };
            var mockClient = new Mock<TCLIService.IAsync>();
            var threadSafeClient = new Mock<ThreadSafeClient>(mockClient.Object);
            threadSafeClient.Setup(c => c.FetchResultsAsync(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateFetchResultsResponse(resultLinks, false));

            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.OperationHandle).Returns(CreateOperationHandle());
            mockStatement.Setup(s => s.ThreadSafeClient).Returns(threadSafeClient.Object);

            var fetcher = new CloudFetchResultFetcher(
                mockStatement.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                5); // batchSize

            // Act
            await fetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the results
            await Task.Delay(100);

            // Assert
            // The download queue should contain our result links
            // Note: With prefetch, there might be more items in the queue than just our result links
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
            Assert.False(fetcher.HasMoreResults);
            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);
            Assert.Null(fetcher.Error);

            // Cleanup
            await fetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithMultipleBatches_FetchesAllResults()
        {
            // Arrange
            var firstBatchLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(0, 100, "http://test.com/file1"),
                CreateTestResultLink(100, 100, "http://test.com/file2")
            };

            var secondBatchLinks = new List<TSparkArrowResultLink>
            {
                CreateTestResultLink(200, 100, "http://test.com/file3"),
                CreateTestResultLink(300, 100, "http://test.com/file4")
            };

            var mockClient = new Mock<TCLIService.IAsync>();
            var threadSafeClient = new Mock<ThreadSafeClient>(mockClient.Object);
            threadSafeClient.SetupSequence(c => c.FetchResultsAsync(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateFetchResultsResponse(firstBatchLinks, true))
                .ReturnsAsync(CreateFetchResultsResponse(secondBatchLinks, false));

            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.OperationHandle).Returns(CreateOperationHandle());
            mockStatement.Setup(s => s.ThreadSafeClient).Returns(threadSafeClient.Object);

            var fetcher = new CloudFetchResultFetcher(
                mockStatement.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                5); // batchSize

            // Act
            await fetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process all results
            await Task.Delay(200);

            // Assert
            // The download queue should contain all result links (both batches)
            // Note: With prefetch, there might be more items in the queue than just our result links
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
            Assert.False(fetcher.HasMoreResults);
            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);

            // Cleanup
            await fetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithEmptyResults_CompletesGracefully()
        {
            // Arrange
            var mockClient = new Mock<TCLIService.IAsync>();
            var threadSafeClient = new Mock<ThreadSafeClient>(mockClient.Object);
            threadSafeClient.Setup(c => c.FetchResultsAsync(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateFetchResultsResponse(new List<TSparkArrowResultLink>(), false));

            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.OperationHandle).Returns(CreateOperationHandle());
            mockStatement.Setup(s => s.ThreadSafeClient).Returns(threadSafeClient.Object);

            var fetcher = new CloudFetchResultFetcher(
                mockStatement.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                5); // batchSize

            // Act
            await fetcher.StartAsync(CancellationToken.None);

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
            Assert.False(fetcher.HasMoreResults);
            Assert.True(fetcher.IsCompleted);
            Assert.False(fetcher.HasError);

            // Cleanup
            await fetcher.StopAsync();
        }

        [Fact]
        public async Task FetchResultsAsync_WithServerError_SetsErrorState()
        {
            // Arrange
            var mockClient = new Mock<TCLIService.IAsync>();
            var threadSafeClient = new Mock<ThreadSafeClient>(mockClient.Object);
            threadSafeClient.Setup(c => c.FetchResultsAsync(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("Test server error"));

            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.OperationHandle).Returns(CreateOperationHandle());
            mockStatement.Setup(s => s.ThreadSafeClient).Returns(threadSafeClient.Object);

            var fetcher = new CloudFetchResultFetcher(
                mockStatement.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                5); // batchSize

            // Act
            await fetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the error
            await Task.Delay(100);

            // Assert
            // Verify the fetcher state
            Assert.False(fetcher.HasMoreResults);
            Assert.True(fetcher.IsCompleted);
            Assert.True(fetcher.HasError);
            Assert.NotNull(fetcher.Error);
            Assert.IsType<InvalidOperationException>(fetcher.Error);

            // The download queue should have the end guard
            Assert.Single(_downloadQueue);
            var result = _downloadQueue.Take();
            Assert.Same(EndOfResultsGuard.Instance, result);

            // Cleanup
            await fetcher.StopAsync();
        }

        [Fact]
        public async Task StopAsync_CancelsFetching()
        {
            // Arrange
            var fetchStarted = new TaskCompletionSource<bool>();
            var fetchCancelled = new TaskCompletionSource<bool>();

            var mockClient = new Mock<TCLIService.IAsync>();
            var threadSafeClient = new Mock<ThreadSafeClient>(mockClient.Object);
            threadSafeClient.Setup(c => c.FetchResultsAsync(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
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

            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.OperationHandle).Returns(CreateOperationHandle());
            mockStatement.Setup(s => s.ThreadSafeClient).Returns(threadSafeClient.Object);

            var fetcher = new CloudFetchResultFetcher(
                mockStatement.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                5); // batchSize

            // Act
            await fetcher.StartAsync(CancellationToken.None);

            // Wait for the fetch to start
            await fetchStarted.Task;

            // Stop the fetcher
            await fetcher.StopAsync();

            // Assert
            // Wait a short time for cancellation to propagate
            var cancelled = await Task.WhenAny(fetchCancelled.Task, Task.Delay(1000)) == fetchCancelled.Task;
            Assert.True(cancelled, "Fetch operation should have been cancelled");

            // Verify the fetcher state
            Assert.True(fetcher.IsCompleted);
        }

        private TOperationHandle CreateOperationHandle()
        {
            return new TOperationHandle
            {
                OperationId = new THandleIdentifier
                {
                    Guid = new byte[16],
                    Secret = new byte[16]
                },
                OperationType = TOperationType.EXECUTE_STATEMENT,
                HasResultSet = true
            };
        }

        private TFetchResultsResp CreateFetchResultsResponse(List<TSparkArrowResultLink> resultLinks, bool hasMoreRows)
        {
            var results = new TRowSet();
            results.__isset.resultLinks = true;
            results.ResultLinks = resultLinks;

            return new TFetchResultsResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS },
                HasMoreRows = hasMoreRows,
                Results = results,
                __isset = { results = true, hasMoreRows = true }
            };
        }

        private TSparkArrowResultLink CreateTestResultLink(long startRowOffset, int rowCount, string fileLink)
        {
            return new TSparkArrowResultLink
            {
                StartRowOffset = startRowOffset,
                RowCount = rowCount,
                FileLink = fileLink
            };
        }
    }
}

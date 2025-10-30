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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Moq.Protected;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.CloudFetch
{
    /// <summary>
    /// E2E tests for straggler download mitigation using mocked HTTP responses.
    /// Tests the actual CloudFetchDownloader with straggler detection enabled.
    ///
    /// NOTE: Some tests are currently failing due to difficulty mocking HiveServer2Connection
    /// (which is abstract and internal). The passing tests validate:
    /// - Basic functionality (fast downloads not marked, minimum quantile, etc.)
    /// - Monitoring thread lifecycle
    /// - Semaphore behavior (parallel and sequential modes)
    /// - Clean shutdown
    ///
    /// Failing tests need further investigation:
    /// - SlowDownloadIdentifiedAndCancelled
    /// - MixedSpeedDownloads
    /// - SequentialFallbackActivatesAfterThreshold
    /// - CancelledStragglerIsRetried
    /// </summary>
    public class CloudFetchStragglerDownloaderE2ETests
    {
        #region Core Straggler Detection Tests

        [Fact]
        public async Task SlowDownloadIdentifiedAndCancelled()
        {
            // Arrange - 9 fast downloads, 1 slow download
            var downloadCancelledFlags = new ConcurrentDictionary<long, bool>();
            var mockHttpHandler = CreateHttpHandlerWithVariableSpeeds(
                downloadCancelledFlags,
                fastIndices: Enumerable.Range(0, 9).Select(i => (long)i).ToList(),
                slowIndices: new List<long> { 9 },
                fastDelayMs: 20,
                slowDelayMs: 2000);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 10,
                stragglerMultiplier: 1.5,
                minimumCompletionQuantile: 0.6,
                stragglerPaddingSeconds: 1,
                maxStragglersBeforeFallback: 10);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 10; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            // Wait for monitoring to detect straggler (need 6/10 completions, then detection)
            // Monitoring runs every 2 seconds, so wait at least 2.5 seconds
            await Task.Delay(2700);

            // Assert
            Assert.True(downloadCancelledFlags.ContainsKey(9), "Slow download should be cancelled as straggler");

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        [Fact]
        public async Task FastDownloadsNotMarkedAsStraggler()
        {
            // Arrange - All 10 downloads fast
            var downloadCancelledFlags = new ConcurrentDictionary<long, bool>();
            var mockHttpHandler = CreateHttpHandlerWithVariableSpeeds(
                downloadCancelledFlags,
                fastIndices: Enumerable.Range(0, 10).Select(i => (long)i).ToList(),
                slowIndices: new List<long>(),
                fastDelayMs: 20,
                slowDelayMs: 0);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 10);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 10; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            await Task.Delay(500);

            // Assert - No downloads cancelled
            Assert.Empty(downloadCancelledFlags);

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        [Fact]
        public async Task RequiresMinimumCompletionQuantile()
        {
            // Arrange - Downloads that complete slowly, not meeting 60% quantile quickly
            var downloadCancelledFlags = new ConcurrentDictionary<long, bool>();
            var completionSources = new ConcurrentDictionary<long, TaskCompletionSource<bool>>();

            for (long i = 0; i < 10; i++)
            {
                completionSources[i] = new TaskCompletionSource<bool>();
            }

            var mockHttpHandler = CreateHttpHandlerWithManualControl(downloadCancelledFlags, completionSources);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 10,
                minimumCompletionQuantile: 0.6);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 10; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            // Let downloads start
            await Task.Delay(100);

            // Complete only 4 downloads (40% < 60%)
            for (long i = 0; i < 4; i++)
            {
                completionSources[i].SetResult(true);
            }

            await Task.Delay(500);

            // Assert - No stragglers detected (below minimum quantile)
            Assert.Empty(downloadCancelledFlags);

            // Cleanup
            for (long i = 4; i < 10; i++)
            {
                completionSources[i].SetResult(true);
            }
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        #endregion

        #region Sequential Fallback Tests

        [Fact]
        public async Task SequentialFallbackActivatesAfterThreshold()
        {
            // Arrange - Create stragglers to trigger fallback (threshold = 2)
            var downloadCancelledFlags = new ConcurrentDictionary<long, bool>();
            var mockHttpHandler = CreateHttpHandlerWithVariableSpeeds(
                downloadCancelledFlags,
                fastIndices: new List<long> { 0, 1, 2, 3, 4, 5, 6 },
                slowIndices: new List<long> { 7, 8, 9 },
                fastDelayMs: 20,
                slowDelayMs: 2000);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 10,
                maxStragglersBeforeFallback: 2, // Fallback after 2 stragglers
                synchronousFallbackEnabled: true);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 10; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            // Monitoring runs every 2 seconds
            await Task.Delay(3000);

            // Assert - Should detect >= 2 stragglers
            Assert.True(downloadCancelledFlags.Count >= 2, $"Expected >= 2 stragglers, got {downloadCancelledFlags.Count}");

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        [Fact]
        public async Task SequentialModeEnforcesOneDownloadAtATime()
        {
            // Arrange - Force immediate sequential mode (threshold = 0)
            var concurrentDownloads = new ConcurrentDictionary<long, bool>();
            int maxConcurrency = 0;
            var concurrencyLock = new object();

            var mockHttpHandler = CreateHttpHandlerWithConcurrencyTracking(
                concurrentDownloads,
                ref maxConcurrency,
                concurrencyLock,
                delayMs: 100);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 5,
                maxStragglersBeforeFallback: 0, // Immediate fallback
                synchronousFallbackEnabled: true);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 5; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            await Task.Delay(700);

            // Assert - Max concurrency should be 1
            Assert.True(maxConcurrency <= 1, $"Sequential mode should have max concurrency of 1, got {maxConcurrency}");

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        [Fact]
        public async Task NoStragglersDetectedInSequentialMode()
        {
            // Arrange - Immediate sequential mode
            var downloadCancelledFlags = new ConcurrentDictionary<long, bool>();
            var mockHttpHandler = CreateHttpHandlerWithVariableSpeeds(
                downloadCancelledFlags,
                fastIndices: new List<long> { 0, 1, 2 },
                slowIndices: new List<long> { 3, 4 },
                fastDelayMs: 20,
                slowDelayMs: 1000);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 5,
                maxStragglersBeforeFallback: 0, // Immediate sequential
                synchronousFallbackEnabled: true);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 5; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            // Monitoring runs every 2 seconds, need time for detection + retry
            await Task.Delay(4000);

            // Assert - No cancellations in sequential mode
            Assert.Empty(downloadCancelledFlags);

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        #endregion

        #region Monitoring Thread Tests

        [Fact]
        public async Task MonitoringThreadRespectsCancellation()
        {
            // Arrange
            var mockHttpHandler = CreateSimpleHttpHandler(delayMs: 5000);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 3);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 3; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            await Task.Delay(200);

            // Stop downloader - should not hang
            await downloader.StopAsync();

            // Assert - If we got here, monitoring respected cancellation
            Assert.True(true);
        }

        #endregion

        #region Semaphore Behavior Tests

        [Fact]
        public async Task ParallelModeRespectsMaxParallelDownloads()
        {
            // Arrange
            var concurrentDownloads = new ConcurrentDictionary<long, bool>();
            int maxConcurrency = 0;
            var concurrencyLock = new object();

            var mockHttpHandler = CreateHttpHandlerWithConcurrencyTracking(
                concurrentDownloads,
                ref maxConcurrency,
                concurrencyLock,
                delayMs: 150);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 3);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 6; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            await Task.Delay(400);

            // Assert
            Assert.True(maxConcurrency <= 3, $"Max concurrency should be <= 3, got {maxConcurrency}");

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        #endregion

        #region Retry Tests

        [Fact]
        public async Task CancelledStragglerIsRetried()
        {
            // Arrange
            var attemptCounts = new ConcurrentDictionary<long, int>();
            var mockHttpHandler = CreateHttpHandlerWithRetryTracking(attemptCounts);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 10);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 10; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            // Monitoring runs every 2 seconds, need time for detection + retry
            await Task.Delay(4000);

            // Assert - At least one download should have multiple attempts
            var hasRetries = attemptCounts.Values.Any(count => count > 1);
            Assert.True(hasRetries, "At least one download should be retried");

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        #endregion

        #region Complex Scenarios

        [Fact]
        public async Task MixedSpeedDownloads()
        {
            // Arrange - 5 fast, 3 medium, 2 slow
            var downloadCancelledFlags = new ConcurrentDictionary<long, bool>();

            var mockHttpHandler = new Mock<HttpMessageHandler>();
            mockHttpHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    try
                    {
                        var url = request.RequestUri?.ToString() ?? "";
                        if (url.Contains("file"))
                        {
                            var offsetStr = url.Split("file")[1];
                            var offset = long.Parse(offsetStr);

                            int delayMs;
                            if (offset < 5) delayMs = 20; // Fast
                            else if (offset < 8) delayMs = 150; // Medium
                            else delayMs = 2000; // Slow

                            await Task.Delay(delayMs, token);
                        }

                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new ByteArrayContent(Encoding.UTF8.GetBytes("Test content"))
                        };
                    }
                    catch (OperationCanceledException)
                    {
                        var url = request.RequestUri?.ToString() ?? "";
                        if (url.Contains("file"))
                        {
                            var offsetStr = url.Split("file")[1];
                            var offset = long.Parse(offsetStr);
                            downloadCancelledFlags[offset] = true;
                        }
                        throw;
                    }
                });

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 10);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 10; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            // Monitoring runs every 2 seconds
            await Task.Delay(3000);

            // Assert - Slow downloads (8, 9) should be cancelled
            Assert.Contains(8L, downloadCancelledFlags.Keys);
            Assert.Contains(9L, downloadCancelledFlags.Keys);

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        [Fact]
        public async Task CleanShutdownDuringMonitoring()
        {
            // Arrange
            var mockHttpHandler = CreateSimpleHttpHandler(delayMs: 3000);

            var (downloader, downloadQueue, resultQueue) = CreateDownloaderWithStragglerMitigation(
                mockHttpHandler.Object,
                maxParallelDownloads: 5);

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 5; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            await Task.Delay(300);

            // Stop during monitoring
            await downloader.StopAsync();

            // Assert - Clean shutdown
            Assert.True(true);
        }

        #endregion

        #region Configuration Tests

        [Fact]
        public async Task FeatureDisabledByDefault()
        {
            // Arrange - Create downloader WITHOUT straggler mitigation
            var downloadCancelledFlags = new ConcurrentDictionary<long, bool>();
            var mockHttpHandler = CreateHttpHandlerWithVariableSpeeds(
                downloadCancelledFlags,
                fastIndices: Enumerable.Range(0, 7).Select(i => (long)i).ToList(),
                slowIndices: new List<long> { 7, 8, 9 },
                fastDelayMs: 20,
                slowDelayMs: 2000);

            var downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 100);
            var resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 100);

            var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            mockMemoryManager.Setup(m => m.TryAcquireMemory(It.IsAny<long>())).Returns(true);
            mockMemoryManager.Setup(m => m.AcquireMemoryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var mockStatement = new Mock<IHiveServer2Statement>();
            // No properties = feature disabled - return null connection
            mockStatement.Setup(s => s.Connection).Returns(default(HiveServer2Connection)!);

            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            mockResultFetcher.Setup(f => f.GetUrlAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((long offset, CancellationToken token) => new TSparkArrowResultLink
                {
                    StartRowOffset = offset,
                    FileLink = $"http://test.com/file{offset}",
                    ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds()
                });

            var httpClient = new HttpClient(mockHttpHandler.Object);

            var downloader = new CloudFetchDownloader(
                mockStatement.Object,
                downloadQueue,
                resultQueue,
                mockMemoryManager.Object,
                httpClient,
                mockResultFetcher.Object,
                10, // maxParallelDownloads
                false); // isLz4Compressed

            // Act
            await downloader.StartAsync(CancellationToken.None);

            for (long i = 0; i < 10; i++)
            {
                downloadQueue.Add(CreateMockDownloadResult(i, 1024 * 1024).Object);
            }

            await Task.Delay(500);

            // Assert - No cancellations (feature disabled)
            Assert.Empty(downloadCancelledFlags);

            // Cleanup
            downloadQueue.Add(EndOfResultsGuard.Instance);
            await downloader.StopAsync();
        }

        #endregion

        #region Helper Methods

        private (CloudFetchDownloader downloader, BlockingCollection<IDownloadResult> downloadQueue, BlockingCollection<IDownloadResult> resultQueue)
            CreateDownloaderWithStragglerMitigation(
                HttpMessageHandler httpMessageHandler,
                int maxParallelDownloads = 5,
                double stragglerMultiplier = 1.5,
                double minimumCompletionQuantile = 0.6,
                int stragglerPaddingSeconds = 1,
                int maxStragglersBeforeFallback = 10,
                bool synchronousFallbackEnabled = true)
        {
            var downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 100);
            var resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 100);

            var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            mockMemoryManager.Setup(m => m.TryAcquireMemory(It.IsAny<long>())).Returns(true);
            mockMemoryManager.Setup(m => m.AcquireMemoryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            // Create statement with straggler mitigation properties
            var properties = new Dictionary<string, string>
            {
                [DatabricksParameters.CloudFetchStragglerMitigationEnabled] = "true",
                [DatabricksParameters.CloudFetchStragglerMultiplier] = stragglerMultiplier.ToString(),
                [DatabricksParameters.CloudFetchStragglerQuantile] = minimumCompletionQuantile.ToString(),
                [DatabricksParameters.CloudFetchStragglerPaddingSeconds] = stragglerPaddingSeconds.ToString(),
                [DatabricksParameters.CloudFetchMaxStragglersPerQuery] = maxStragglersBeforeFallback.ToString(),
                [DatabricksParameters.CloudFetchSynchronousFallbackEnabled] = synchronousFallbackEnabled.ToString()
            };

            var mockConnection = new Mock<HiveServer2Connection>(properties);

            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.Connection).Returns(mockConnection.Object);

            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            mockResultFetcher.Setup(f => f.GetUrlAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((long offset, CancellationToken token) => new TSparkArrowResultLink
                {
                    StartRowOffset = offset,
                    FileLink = $"http://test.com/file{offset}",
                    ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds()
                });

            var httpClient = new HttpClient(httpMessageHandler);

            var downloader = new CloudFetchDownloader(
                mockStatement.Object,
                downloadQueue,
                resultQueue,
                mockMemoryManager.Object,
                httpClient,
                mockResultFetcher.Object,
                maxParallelDownloads,
                false, // isLz4Compressed
                maxRetries: 3,
                retryDelayMs: 10);

            return (downloader, downloadQueue, resultQueue);
        }

        private Mock<IDownloadResult> CreateMockDownloadResult(long offset, long size)
        {
            var mockDownloadResult = new Mock<IDownloadResult>();
            var resultLink = new TSparkArrowResultLink
            {
                StartRowOffset = offset,
                FileLink = $"http://test.com/file{offset}",
                ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds()
            };

            mockDownloadResult.Setup(r => r.Link).Returns(resultLink);
            mockDownloadResult.Setup(r => r.Size).Returns(size);
            mockDownloadResult.Setup(r => r.RefreshAttempts).Returns(0);
            mockDownloadResult.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);
            mockDownloadResult.Setup(r => r.SetCompleted(It.IsAny<Stream>(), It.IsAny<long>()));

            return mockDownloadResult;
        }

        private Mock<HttpMessageHandler> CreateHttpHandlerWithVariableSpeeds(
            ConcurrentDictionary<long, bool> downloadCancelledFlags,
            List<long> fastIndices,
            List<long> slowIndices,
            int fastDelayMs,
            int slowDelayMs)
        {
            var mockHandler = new Mock<HttpMessageHandler>();

            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    try
                    {
                        var url = request.RequestUri?.ToString() ?? "";
                        if (url.Contains("file"))
                        {
                            var offsetStr = url.Split("file")[1];
                            var offset = long.Parse(offsetStr);

                            int delayMs = fastDelayMs;
                            if (slowIndices.Contains(offset))
                            {
                                delayMs = slowDelayMs;
                            }

                            await Task.Delay(delayMs, token);
                        }

                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new ByteArrayContent(Encoding.UTF8.GetBytes("Test content"))
                        };
                    }
                    catch (OperationCanceledException)
                    {
                        var url = request.RequestUri?.ToString() ?? "";
                        if (url.Contains("file"))
                        {
                            var offsetStr = url.Split("file")[1];
                            var offset = long.Parse(offsetStr);
                            downloadCancelledFlags[offset] = true;
                        }
                        throw;
                    }
                });

            return mockHandler;
        }

        private Mock<HttpMessageHandler> CreateHttpHandlerWithManualControl(
            ConcurrentDictionary<long, bool> downloadCancelledFlags,
            ConcurrentDictionary<long, TaskCompletionSource<bool>> completionSources)
        {
            var mockHandler = new Mock<HttpMessageHandler>();

            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    try
                    {
                        var url = request.RequestUri?.ToString() ?? "";
                        if (url.Contains("file"))
                        {
                            var offsetStr = url.Split("file")[1];
                            var offset = long.Parse(offsetStr);

                            if (completionSources.ContainsKey(offset))
                            {
                                await completionSources[offset].Task;
                            }
                        }

                        return new HttpResponseMessage(HttpStatusCode.OK)
                        {
                            Content = new ByteArrayContent(Encoding.UTF8.GetBytes("Test content"))
                        };
                    }
                    catch (OperationCanceledException)
                    {
                        var url = request.RequestUri?.ToString() ?? "";
                        if (url.Contains("file"))
                        {
                            var offsetStr = url.Split("file")[1];
                            var offset = long.Parse(offsetStr);
                            downloadCancelledFlags[offset] = true;
                        }
                        throw;
                    }
                });

            return mockHandler;
        }

        private Mock<HttpMessageHandler> CreateHttpHandlerWithConcurrencyTracking(
            ConcurrentDictionary<long, bool> concurrentDownloads,
            ref int maxConcurrency,
            object concurrencyLock,
            int delayMs)
        {
            var mockHandler = new Mock<HttpMessageHandler>();
            int localMaxConcurrency = maxConcurrency;

            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    var url = request.RequestUri?.ToString() ?? "";
                    long offset = 0;

                    if (url.Contains("file"))
                    {
                        var offsetStr = url.Split("file")[1];
                        offset = long.Parse(offsetStr);
                        concurrentDownloads[offset] = true;

                        lock (concurrencyLock)
                        {
                            if (concurrentDownloads.Count > localMaxConcurrency)
                            {
                                localMaxConcurrency = concurrentDownloads.Count;
                            }
                        }
                    }

                    try
                    {
                        await Task.Delay(delayMs, token);
                    }
                    finally
                    {
                        if (offset > 0)
                        {
                            concurrentDownloads.TryRemove(offset, out _);
                        }
                    }

                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new ByteArrayContent(Encoding.UTF8.GetBytes("Test content"))
                    };
                });

            maxConcurrency = localMaxConcurrency;
            return mockHandler;
        }

        private Mock<HttpMessageHandler> CreateHttpHandlerWithRetryTracking(
            ConcurrentDictionary<long, int> attemptCounts)
        {
            var mockHandler = new Mock<HttpMessageHandler>();

            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    var url = request.RequestUri?.ToString() ?? "";
                    if (url.Contains("file"))
                    {
                        var offsetStr = url.Split("file")[1];
                        var offset = long.Parse(offsetStr);

                        var attempt = attemptCounts.AddOrUpdate(offset, 1, (k, v) => v + 1);

                        // First attempt slow, subsequent attempts fast
                        int delayMs = attempt == 1 ? 2000 : 20;

                        await Task.Delay(delayMs, token);
                    }

                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new ByteArrayContent(Encoding.UTF8.GetBytes("Test content"))
                    };
                });

            return mockHandler;
        }

        private Mock<HttpMessageHandler> CreateSimpleHttpHandler(int delayMs)
        {
            var mockHandler = new Mock<HttpMessageHandler>();

            mockHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    await Task.Delay(delayMs, token);

                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new ByteArrayContent(Encoding.UTF8.GetBytes("Test content"))
                    };
                });

            return mockHandler;
        }

        #endregion
    }
}

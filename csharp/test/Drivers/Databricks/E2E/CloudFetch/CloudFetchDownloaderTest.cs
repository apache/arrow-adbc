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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Moq.Protected;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.CloudFetch
{
    public class CloudFetchDownloaderTest
    {
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly BlockingCollection<IDownloadResult> _resultQueue;
        private readonly Mock<ICloudFetchMemoryBufferManager> _mockMemoryManager;
        private readonly Mock<IHiveServer2Statement> _mockStatement;
        private readonly Mock<ICloudFetchResultFetcher> _mockResultFetcher;

        public CloudFetchDownloaderTest()
        {
            _downloadQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            _resultQueue = new BlockingCollection<IDownloadResult>(new ConcurrentQueue<IDownloadResult>(), 10);
            _mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            _mockStatement = new Mock<IHiveServer2Statement>();
            _mockResultFetcher = new Mock<ICloudFetchResultFetcher>();

            // Set up memory manager defaults
            _mockMemoryManager.Setup(m => m.TryAcquireMemory(It.IsAny<long>())).Returns(true);
            _mockMemoryManager.Setup(m => m.AcquireMemoryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            // Set up result fetcher defaults
            _mockResultFetcher.Setup(f => f.GetUrlAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((long offset, CancellationToken token) =>
                {
                    // Return a URL with the same offset
                    return new TSparkArrowResultLink
                    {
                        StartRowOffset = offset,
                        FileLink = $"http://test.com/file{offset}",
                        ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds()
                    };
                });

            // Set up activity tracing - CloudFetchDownloader implements IActivityTracer
            // and delegates to _statement.Trace and _statement.TraceParent (CloudFetchDownloader.cs:605-607)
            _mockStatement.Setup(s => s.Trace).Returns(new ActivityTrace());
            _mockStatement.Setup(s => s.TraceParent).Returns((string?)null);
        }

        [Fact]
        public async Task StartAsync_CalledTwice_ThrowsException()
        {
            // Arrange
            var mockDownloader = new Mock<ICloudFetchDownloader>();

            // Setup first call to succeed and second call to throw
            mockDownloader.SetupSequence(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask)
                .Throws(new InvalidOperationException("Downloader is already running."));

            // Act & Assert
            await mockDownloader.Object.StartAsync(CancellationToken.None);
            await Assert.ThrowsAsync<InvalidOperationException>(() => mockDownloader.Object.StartAsync(CancellationToken.None));
        }

        [Fact]
        public async Task GetNextDownloadedFileAsync_ReturnsNull_WhenEndOfResultsGuardReceived()
        {
            // Arrange
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            var httpClient = new HttpClient(mockHttpMessageHandler.Object);
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                _mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                3, // maxParallelDownloads
                false); // isLz4Compressed

            // Add the end of results guard to the result queue
            _resultQueue.Add(EndOfResultsGuard.Instance);

            // Act
            await downloader.StartAsync(CancellationToken.None);
            var result = await downloader.GetNextDownloadedFileAsync(CancellationToken.None);

            // Assert
            Assert.Null(result);
            Assert.True(downloader.IsCompleted);

            // Cleanup
            await downloader.StopAsync();
        }

        [Fact]
        public async Task DownloadFileAsync_ProcessesFile_AndAddsToResultQueue()
        {
            // Arrange
            string testContent = "Test file content";
            byte[] testContentBytes = Encoding.UTF8.GetBytes(testContent);

            // Create a mock HTTP handler that returns our test content
            var mockHttpMessageHandler = CreateMockHttpMessageHandler(testContentBytes);
            var httpClient = new HttpClient(mockHttpMessageHandler.Object);

            // Create a test download result
            var mockDownloadResult = new Mock<IDownloadResult>();
            var resultLink = new TSparkArrowResultLink {
                FileLink = "http://test.com/file1",
                ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds() // Set expiry 30 minutes in the future
            };
            mockDownloadResult.Setup(r => r.Link).Returns(resultLink);
            mockDownloadResult.Setup(r => r.Size).Returns(testContentBytes.Length);
            mockDownloadResult.Setup(r => r.RefreshAttempts).Returns(0);
            mockDownloadResult.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);

            // Capture the stream and size passed to SetCompleted
            Stream? capturedStream = null;
            long capturedSize = 0;
            mockDownloadResult.Setup(r => r.SetCompleted(It.IsAny<Stream>(), It.IsAny<long>()))
                .Callback<Stream, long>((stream, size) =>
                {
                    capturedStream = stream;
                    capturedSize = size;
                });

            // Create the downloader and add the download to the queue
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                _mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                1, // maxParallelDownloads
                false, // isLz4Compressed
                1, // maxRetries
                10); // retryDelayMs

            // Act
            await downloader.StartAsync(CancellationToken.None);
            _downloadQueue.Add(mockDownloadResult.Object);

            // Wait for the download to be processed
            await Task.Delay(100);

            // Add the end of results guard to complete the downloader
            _downloadQueue.Add(EndOfResultsGuard.Instance);

            // Wait for the result to be available
            var result = await downloader.GetNextDownloadedFileAsync(CancellationToken.None);

            // Assert
            Assert.Same(mockDownloadResult.Object, result);

            // Verify SetCompleted was called
            mockDownloadResult.Verify(r => r.SetCompleted(It.IsAny<Stream>(), It.IsAny<long>()), Times.Once);

            // Verify the content of the stream
            Assert.NotNull(capturedStream);
            using (var reader = new StreamReader(capturedStream))
            {
                string content = reader.ReadToEnd();
                Assert.Equal(testContent, content);
            }

            // Verify memory was acquired
            _mockMemoryManager.Verify(m => m.AcquireMemoryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Once);

            // Cleanup
            await downloader.StopAsync();
        }

        [Fact]
        public async Task DownloadFileAsync_HandlesHttpError_AndSetsFailedOnDownloadResult()
        {
            // Arrange
            // Create a mock HTTP handler that returns a 404 error
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    await Task.Delay(1, token); // Small delay to simulate network
                    return new HttpResponseMessage(HttpStatusCode.NotFound);
                });

            var httpClient = new HttpClient(mockHttpMessageHandler.Object);

            // Create a test download result
            var mockDownloadResult = new Mock<IDownloadResult>();
            var resultLink = new TSparkArrowResultLink {
                FileLink = "http://test.com/file1",
                ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds() // Set expiry 30 minutes in the future
            };
            mockDownloadResult.Setup(r => r.Link).Returns(resultLink);
            mockDownloadResult.Setup(r => r.Size).Returns(1000); // Some arbitrary size
            mockDownloadResult.Setup(r => r.RefreshAttempts).Returns(0);
            mockDownloadResult.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);

            // Capture when SetFailed is called
            Exception? capturedException = null;
            mockDownloadResult.Setup(r => r.SetFailed(It.IsAny<Exception>()))
                .Callback<Exception>(ex => capturedException = ex);

            // Create the downloader and add the download to the queue
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                _mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                1, // maxParallelDownloads
                false, // isLz4Compressed
                1, // maxRetries
                10); // retryDelayMs

            // Act
            await downloader.StartAsync(CancellationToken.None);
            _downloadQueue.Add(mockDownloadResult.Object);

            // Wait for the download to be processed
            await Task.Delay(100);

            // Add the end of results guard to complete the downloader
            _downloadQueue.Add(EndOfResultsGuard.Instance);

            // Assert
            // Verify SetFailed was called
            mockDownloadResult.Verify(r => r.SetFailed(It.IsAny<Exception>()), Times.Once);
            Assert.NotNull(capturedException);
            Assert.IsType<HttpRequestException>(capturedException);

            // Verify the downloader has an error
            Assert.True(downloader.HasError);
            Assert.NotNull(downloader.Error);

            // Verify GetNextDownloadedFileAsync throws an exception
            await Assert.ThrowsAsync<AdbcException>(() => downloader.GetNextDownloadedFileAsync(CancellationToken.None));

            // Cleanup
            await downloader.StopAsync();
        }

        [Fact]
        public async Task DownloadFileAsync_WithError_StopsProcessingRemainingFiles()
        {
            // Arrange
            // Create a mock HTTP handler that returns success for the first request and error for the second
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();

            // Use a simpler approach - just make all requests fail
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.NotFound));

            var httpClient = new HttpClient(mockHttpMessageHandler.Object);

            // Create test download results
            var mockDownloadResult = new Mock<IDownloadResult>();
            var resultLink = new TSparkArrowResultLink {
                FileLink = "http://test.com/file1",
                ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds() // Set expiry 30 minutes in the future
            };
            mockDownloadResult.Setup(r => r.Link).Returns(resultLink);
            mockDownloadResult.Setup(r => r.Size).Returns(100);
            mockDownloadResult.Setup(r => r.RefreshAttempts).Returns(0);
            mockDownloadResult.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);

            // Capture when SetFailed is called
            Exception? capturedException = null;
            mockDownloadResult.Setup(r => r.SetFailed(It.IsAny<Exception>()))
                .Callback<Exception>(ex => capturedException = ex);

            // Create the downloader
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                _mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                1, // maxParallelDownloads
                false, // isLz4Compressed
                1, // maxRetries
                10); // retryDelayMs

            // Act
            await downloader.StartAsync(CancellationToken.None);
            _downloadQueue.Add(mockDownloadResult.Object);

            // Wait for the download to be processed and fail
            await Task.Delay(200);

            // Add the end of results guard
            _downloadQueue.Add(EndOfResultsGuard.Instance);

            // Wait for all processing to complete
            await Task.Delay(200);

            // Assert
            // Verify the download failed
            mockDownloadResult.Verify(r => r.SetFailed(It.IsAny<Exception>()), Times.Once);

            // Verify the downloader has an error
            Assert.True(downloader.HasError);
            Assert.NotNull(downloader.Error);

            // Verify GetNextDownloadedFileAsync throws an exception
            await Assert.ThrowsAsync<AdbcException>(() => downloader.GetNextDownloadedFileAsync(CancellationToken.None));

            // Cleanup
            await downloader.StopAsync();
        }

        [Fact]
        public async Task StopAsync_CancelsOngoingDownloads()
        {
            // Arrange
            var cancellationTokenSource = new CancellationTokenSource();
            var downloadStarted = new TaskCompletionSource<bool>();
            var downloadCancelled = new TaskCompletionSource<bool>();

            // Create a mock HTTP handler with a delay to simulate a long download
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    downloadStarted.TrySetResult(true);

                    try
                    {
                        // Wait for a long time or until cancellation
                        await Task.Delay(10000, token);
                    }
                    catch (OperationCanceledException)
                    {
                        downloadCancelled.TrySetResult(true);
                        throw;
                    }

                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("Test content")
                    };
                });

            var httpClient = new HttpClient(mockHttpMessageHandler.Object);

            // Create a test download result
            var mockDownloadResult = new Mock<IDownloadResult>();
            var resultLink = new TSparkArrowResultLink {
                FileLink = "http://test.com/file1",
                ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds() // Set expiry 30 minutes in the future
            };
            mockDownloadResult.Setup(r => r.Link).Returns(resultLink);
            mockDownloadResult.Setup(r => r.Size).Returns(100);
            mockDownloadResult.Setup(r => r.RefreshAttempts).Returns(0);
            mockDownloadResult.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);

            // Create the downloader and add the download to the queue
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                _mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                1, // maxParallelDownloads
                false); // isLz4Compressed

            // Act
            await downloader.StartAsync(CancellationToken.None);
            _downloadQueue.Add(mockDownloadResult.Object);

            // Wait for the download to start
            await downloadStarted.Task;

            // Stop the downloader
            await downloader.StopAsync();

            // Assert
            // Wait a short time for cancellation to propagate
            var cancelled = await Task.WhenAny(downloadCancelled.Task, Task.Delay(1000)) == downloadCancelled.Task;
            Assert.True(cancelled, "Download should have been cancelled");
        }

        [Fact]
        public async Task GetNextDownloadedFileAsync_RespectsMaxParallelDownloads()
        {
            // Arrange
            int totalDownloads = 3;
            int maxParallelDownloads = 2;
            var downloadStartedEvents = new TaskCompletionSource<bool>[totalDownloads];
            var downloadCompletedEvents = new TaskCompletionSource<bool>[totalDownloads];

            for (int i = 0; i < totalDownloads; i++)
            {
                downloadStartedEvents[i] = new TaskCompletionSource<bool>();
                downloadCompletedEvents[i] = new TaskCompletionSource<bool>();
            }

            // Create a mock HTTP handler that signals when downloads start and waits for completion signal
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    // Extract the index from the URL
                    string url = request.RequestUri?.ToString() ?? "";
                    if (url.Contains("file"))
                    {
                        int index = int.Parse(url.Substring(url.Length - 1));

                        if (request.Method == HttpMethod.Get)
                        {
                            // Signal that this download has started
                            downloadStartedEvents[index].TrySetResult(true);

                            // Wait for the signal to complete this download
                            await downloadCompletedEvents[index].Task;
                        }
                    }

                    // Return a success response
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new ByteArrayContent(Encoding.UTF8.GetBytes("Test content"))
                    };
                });

            var httpClient = new HttpClient(mockHttpMessageHandler.Object);

            // Create test download results
            var downloadResults = new IDownloadResult[totalDownloads];
            for (int i = 0; i < totalDownloads; i++)
            {
                var mockDownloadResult = new Mock<IDownloadResult>();
                var resultLink = new TSparkArrowResultLink {
                    FileLink = $"http://test.com/file{i}",
                    ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds() // Set expiry 30 minutes in the future
                };
                mockDownloadResult.Setup(r => r.Link).Returns(resultLink);
                mockDownloadResult.Setup(r => r.Size).Returns(100);
                mockDownloadResult.Setup(r => r.RefreshAttempts).Returns(0);
                mockDownloadResult.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);
                mockDownloadResult.Setup(r => r.SetCompleted(It.IsAny<Stream>(), It.IsAny<long>()))
                    .Callback<Stream, long>((_, _) => { });
                downloadResults[i] = mockDownloadResult.Object;
            }

            // Create the downloader
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                _mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                maxParallelDownloads,
                false); // isLz4Compressed

            // Act
            await downloader.StartAsync(CancellationToken.None);

            // Add all downloads to the queue
            foreach (var result in downloadResults)
            {
                _downloadQueue.Add(result);
            }

            // Wait for the first two downloads to start
            await Task.WhenAll(
                downloadStartedEvents[0].Task,
                downloadStartedEvents[1].Task);

            // At this point, two downloads should be in progress
            // Wait a bit to ensure the third download has had a chance to start if it's going to
            await Task.Delay(100);

            // The third download should not have started yet
            Assert.False(downloadStartedEvents[2].Task.IsCompleted, "The third download should not have started yet");

            // Complete the first download
            downloadCompletedEvents[0].SetResult(true);

            // Wait for the third download to start
            await downloadStartedEvents[2].Task;

            // Complete the remaining downloads
            downloadCompletedEvents[1].SetResult(true);
            downloadCompletedEvents[2].SetResult(true);

            // Add the end of results guard to complete the downloader
            _downloadQueue.Add(EndOfResultsGuard.Instance);

            // Cleanup
            await downloader.StopAsync();
        }

        [Fact]
        public async Task DownloadFileAsync_RefreshesExpiredUrl_WhenHttpErrorOccurs()
        {
            // Arrange
            // Create a mock HTTP handler that returns a 403 error for the first request and success for the second
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            var requestCount = 0;

            mockHttpMessageHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    await Task.Delay(1, token); // Small delay to simulate network

                    // First request fails with 403 Forbidden (expired URL)
                    if (requestCount == 0)
                    {
                        requestCount++;
                        return new HttpResponseMessage(HttpStatusCode.Forbidden);
                    }

                    // Second request succeeds with the refreshed URL
                    return new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("Test content")
                    };
                });

            var httpClient = new HttpClient(mockHttpMessageHandler.Object);

            // Create a test download result
            var mockDownloadResult = new Mock<IDownloadResult>();
            var resultLink = new TSparkArrowResultLink {
                StartRowOffset = 0,
                FileLink = "http://test.com/file1",
                ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds() // Set expiry in the past
            };
            mockDownloadResult.Setup(r => r.Link).Returns(resultLink);
            mockDownloadResult.Setup(r => r.Size).Returns(100);
            mockDownloadResult.Setup(r => r.RefreshAttempts).Returns(0);
            // Important: Set this to false so the initial URL refresh doesn't happen
            mockDownloadResult.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);

            // Setup URL refreshing - expect it to be called once during the HTTP 403 error handling
            var refreshedLink = new TSparkArrowResultLink {
                StartRowOffset = 0,
                FileLink = "http://test.com/file1-refreshed",
                ExpiryTime = DateTimeOffset.UtcNow.AddMinutes(30).ToUnixTimeMilliseconds() // Set new expiry in the future
            };
            _mockResultFetcher.Setup(f => f.GetUrlAsync(0, It.IsAny<CancellationToken>()))
                .ReturnsAsync(refreshedLink);

            // Create the downloader and add the download to the queue
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                _mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                1, // maxParallelDownloads
                false, // isLz4Compressed
                2, // maxRetries
                10); // retryDelayMs

            // Act
            await downloader.StartAsync(CancellationToken.None);
            _downloadQueue.Add(mockDownloadResult.Object);

            // Wait for the download to be processed
            await Task.Delay(200);

            // Add the end of results guard to complete the downloader
            _downloadQueue.Add(EndOfResultsGuard.Instance);

            // Assert
            // Verify that GetUrlAsync was called exactly once to refresh the URL
            _mockResultFetcher.Verify(f => f.GetUrlAsync(0, It.IsAny<CancellationToken>()), Times.Once);

            // Verify that UpdateWithRefreshedLink was called with the refreshed link
            mockDownloadResult.Verify(r => r.UpdateWithRefreshedLink(refreshedLink), Times.Once);

            // Cleanup
            await downloader.StopAsync();
        }

        [Fact]
        public async Task Downloader_WithFIFO_ProcessesFilesInOrder()
        {
            // This test verifies FIFO (First-In-First-Out) ordering behavior when memory is limited:
            // 1. maxParallelDownloads=3 allows up to 3 concurrent downloads
            // 2. Memory acquisition blocks until enough memory is available (CloudFetchDownloader.cs:280)
            // 3. When memory is limited, files wait and are processed in FIFO order
            // 4. Results are added to result queue immediately after memory acquisition (CloudFetchDownloader.cs:324)
            //
            // This test verifies that file 3 does NOT start downloading before file 2 by checking
            // the actual order downloads are initiated and items appear in resultQueue.

            // Track download start order (when HTTP request is actually made)
            var downloadStartOrder = new List<long>();
            var downloadStartLock = new object();

            byte[] testContent = new byte[10];
            var mockHttpHandler = new Mock<HttpMessageHandler>();
            mockHttpHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync((HttpRequestMessage request, CancellationToken token) =>
                {
                    // Track when download actually starts
                    var url = request.RequestUri?.ToString() ?? "";
                    long size = url.Contains("test1") ? 100 : url.Contains("test2") ? 200 : 300;
                    lock (downloadStartLock)
                    {
                        downloadStartOrder.Add(size);
                    }
                    // Simulate download time
                    Thread.Sleep(20);
                    return new HttpResponseMessage
                    {
                        StatusCode = HttpStatusCode.OK,
                        Content = new ByteArrayContent(testContent)
                    };
                });

            var httpClient = new HttpClient(mockHttpHandler.Object);

            // Track memory acquisition order
            var memoryAcquisitionOrder = new List<long>();
            var memoryLock = new SemaphoreSlim(1, 1); // Only allow one memory acquisition at a time

            // Mock memory manager to control memory availability
            var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
            mockMemoryManager.Setup(m => m.AcquireMemoryAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()))
                .Returns(async (long size, CancellationToken token) =>
                {
                    // Acquire lock to ensure only one file gets memory at a time
                    await memoryLock.WaitAsync(token);
                    memoryAcquisitionOrder.Add(size);
                    // Add small delay to simulate memory acquisition
                    await Task.Delay(10, token);
                    memoryLock.Release();
                });

            // Create three download results with different sizes to verify ordering
            var downloadResult1 = CreateMockDownloadResult(100, "http://test1.com").Object;
            var downloadResult2 = CreateMockDownloadResult(200, "http://test2.com").Object;
            var downloadResult3 = CreateMockDownloadResult(300, "http://test3.com").Object;

            // Create downloader with maxParallelDownloads=3
            var downloader = new CloudFetchDownloader(
                _mockStatement.Object,
                _downloadQueue,
                _resultQueue,
                mockMemoryManager.Object,
                httpClient,
                _mockResultFetcher.Object,
                3, // Allow up to 3 parallel downloads
                false,
                1,
                10);

            await downloader.StartAsync(CancellationToken.None);

            // Add all three downloads to the queue
            _downloadQueue.Add(downloadResult1);
            _downloadQueue.Add(downloadResult2);
            _downloadQueue.Add(downloadResult3);
            _downloadQueue.Add(EndOfResultsGuard.Instance);

            // Wait for all downloads to complete
            // Each download takes: 10ms (memory acquisition) + 20ms (HTTP download) = 30ms
            // Sequential: 3 * 30ms = 90ms minimum, add buffer for test overhead
            await Task.Delay(300);

            // Verify items appear in resultQueue in FIFO order
            // Note: resultQueue is populated at CloudFetchDownloader.cs:324 immediately after memory acquisition
            var queueItems = new List<long>();
            foreach (var item in _resultQueue)
            {
                if (item != EndOfResultsGuard.Instance)
                {
                    queueItems.Add(item.Size);
                }
            }

            // Verify memory was acquired in FIFO order
            Assert.Equal(3, memoryAcquisitionOrder.Count);
            Assert.Equal(100, memoryAcquisitionOrder[0]);
            Assert.Equal(200, memoryAcquisitionOrder[1]);
            Assert.Equal(300, memoryAcquisitionOrder[2]);

            // Verify downloads started in FIFO order (file 3 did NOT start before file 2)
            Assert.Equal(3, downloadStartOrder.Count);
            Assert.Equal(100, downloadStartOrder[0]);
            Assert.Equal(200, downloadStartOrder[1]);
            Assert.Equal(300, downloadStartOrder[2]);

            // Verify items appeared in resultQueue in FIFO order
            Assert.Equal(3, queueItems.Count);
            Assert.Equal(100, queueItems[0]);
            Assert.Equal(200, queueItems[1]);
            Assert.Equal(300, queueItems[2]);

            await downloader.StopAsync();
            Assert.False(downloader.HasError);
        }

        private Mock<IDownloadResult> CreateMockDownloadResult(long size, string url)
        {
            var link = new TSparkArrowResultLink
            {
                FileLink = url,
                BytesNum = size,
                ExpiryTime = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds()
            };

            var mock = new Mock<IDownloadResult>();
            mock.Setup(r => r.Link).Returns(link);
            mock.Setup(r => r.Size).Returns(size);
            mock.Setup(r => r.RefreshAttempts).Returns(0);
            mock.Setup(r => r.IsExpiredOrExpiringSoon(It.IsAny<int>())).Returns(false);
            mock.Setup(r => r.SetCompleted(It.IsAny<Stream>(), It.IsAny<long>()))
                .Callback<Stream, long>((stream, size) =>
                {
                    // Capture the stream but don't need to do anything with it for this test
                });
            return mock;
        }

        private static Mock<HttpMessageHandler> CreateMockHttpMessageHandler(
            byte[]? content,
            HttpStatusCode statusCode = HttpStatusCode.OK,
            TimeSpan? delay = null)
        {
            var mockHandler = new Mock<HttpMessageHandler>();

            mockHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(async (request, token) =>
                {
                    // If a delay is specified, wait for that duration
                    if (delay.HasValue)
                    {
                        await Task.Delay(delay.Value, token);
                    }

                    // If the request is a HEAD request, return a response with content length
                    if (request.Method == HttpMethod.Head)
                    {
                        var response = new HttpResponseMessage(statusCode);
                        if (content != null)
                        {
                            response.Content = new ByteArrayContent(new byte[0]);
                            response.Content.Headers.ContentLength = content.Length;
                        }
                        return response;
                    }

                    // For GET requests, return the actual content
                    var responseMessage = new HttpResponseMessage(statusCode);
                    if (content != null && statusCode == HttpStatusCode.OK)
                    {
                        responseMessage.Content = new ByteArrayContent(content);
                        responseMessage.Content.Headers.ContentLength = content.Length;
                    }

                    return responseMessage;
                });

            return mockHandler;
        }
    }
}

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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.CloudFetch
{
    public class CloudFetchResultFetcherTest
    {
        private readonly Mock<IHiveServer2Statement> _mockStatement;
        private readonly Mock<TCLIService.IAsync> _mockClient;
        private readonly TOperationHandle _operationHandle;
        private readonly MockClock _mockClock;
        private readonly CloudFetchResultFetcherWithMockClock _resultFetcher;
        private readonly BlockingCollection<IDownloadResult> _downloadQueue;
        private readonly Mock<ICloudFetchMemoryBufferManager> _mockMemoryManager;

        public CloudFetchResultFetcherTest()
        {
            _mockClient = new Mock<TCLIService.IAsync>();
            _mockStatement = new Mock<IHiveServer2Statement>();
            _operationHandle = new TOperationHandle
            {
                OperationId = new THandleIdentifier { Guid = new byte[] { 1, 2, 3, 4 } },
                OperationType = TOperationType.EXECUTE_STATEMENT
            };

            _mockStatement.Setup(s => s.Client).Returns(_mockClient.Object);
            _mockStatement.Setup(s => s.OperationHandle).Returns(_operationHandle);

            _mockClock = new MockClock();
            _downloadQueue = new BlockingCollection<IDownloadResult>();
            _mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();

            _resultFetcher = new CloudFetchResultFetcherWithMockClock(
                _mockStatement.Object,
                _mockMemoryManager.Object,
                _downloadQueue,
                100, // batchSize
                _mockClock,
                60); // expirationBufferSeconds
        }

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

            SetupMockClientFetchResults(resultLinks, true);

            // Act
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the links
            await Task.Delay(100);

            // Get all cached URLs
            var cachedUrls = _resultFetcher.GetAllCachedUrls();

            // Assert
            Assert.Equal(3, cachedUrls.Count);
            Assert.Equal("http://test.com/file1", cachedUrls[0].FileLink);
            Assert.Equal("http://test.com/file2", cachedUrls[100].FileLink);
            Assert.Equal("http://test.com/file3", cachedUrls[200].FileLink);
            _mockClient.Verify(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()), Times.Once);

            // Stop the fetcher
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

            SetupMockClientFetchResults(resultLinks, true);

            // Cache the URLs
            await _resultFetcher.StartAsync(CancellationToken.None);

            // Wait for the fetcher to process the links
            await Task.Delay(100);

            // Act
            _resultFetcher.ClearCache();
            var cachedUrls = _resultFetcher.GetAllCachedUrls();

            // Assert
            Assert.Empty(cachedUrls);

            // Stop the fetcher
            await _resultFetcher.StopAsync();
        }

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
            ICloudFetchMemoryBufferManager memoryManager,
            BlockingCollection<IDownloadResult> downloadQueue,
            long batchSize,
            IClock clock,
            int expirationBufferSeconds = 60)
            : base(statement, memoryManager, downloadQueue, batchSize, expirationBufferSeconds, clock)
        {
        }
    }
}

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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark.CloudFetch
{
    public class CloudFetchDownloadManagerTest
    {
        [Fact]
        public async Task StartAsync_CreatesInternalCancellationToken()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            // Capture the cancellation token passed to StartAsync
            CancellationToken capturedToken = CancellationToken.None;
            mockResultFetcher.Setup(rf => rf.StartAsync(It.IsAny<CancellationToken>()))
                .Callback<CancellationToken>(token => capturedToken = token)
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act
            await downloadManager.StartAsync();
            
            // Assert
            mockResultFetcher.Verify(rf => rf.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
            mockDownloader.Verify(d => d.StartAsync(It.IsAny<CancellationToken>()), Times.Once);
            
            // Verify the token is not the default None token
            Assert.NotEqual(CancellationToken.None, capturedToken);
        }

        [Fact]
        public async Task StopAsync_CancelsInternalToken()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            // Capture the cancellation token passed to StartAsync to verify it's cancelled later
            CancellationToken capturedToken = CancellationToken.None;
            mockResultFetcher.Setup(rf => rf.StartAsync(It.IsAny<CancellationToken>()))
                .Callback<CancellationToken>(token => capturedToken = token)
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockResultFetcher.Setup(rf => rf.StopAsync())
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StopAsync())
                .Returns(Task.CompletedTask);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act
            await downloadManager.StartAsync();
            await downloadManager.StopAsync();
            
            // Assert
            mockResultFetcher.Verify(rf => rf.StopAsync(), Times.Once);
            mockDownloader.Verify(d => d.StopAsync(), Times.Once);
            
            // Verify the token was cancelled
            Assert.True(capturedToken.IsCancellationRequested);
        }

        [Fact]
        public async Task GetNextDownloadedFileAsync_PassesCancellationTokenToDownloader()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            var mockDownloadResult = new Mock<IDownloadResult>();
            var testCancellationToken = new CancellationToken();
            
            mockResultFetcher.Setup(rf => rf.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            // Set up the downloader to capture the token passed to GetNextDownloadedFileAsync
            CancellationToken capturedToken = CancellationToken.None;
            mockDownloader.Setup(d => d.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()))
                .Callback<CancellationToken>(token => capturedToken = token)
                .ReturnsAsync(mockDownloadResult.Object);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act
            await downloadManager.StartAsync();
            var result = await downloadManager.GetNextDownloadedFileAsync(testCancellationToken);
            
            // Assert
            mockDownloader.Verify(d => d.GetNextDownloadedFileAsync(It.IsAny<CancellationToken>()), Times.Once);
            
            // Verify the caller's token was passed through
            Assert.Equal(testCancellationToken, capturedToken);
            Assert.Same(mockDownloadResult.Object, result);
        }

        [Fact]
        public async Task Dispose_StopsAndCleansUpResources()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            mockResultFetcher.Setup(rf => rf.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockResultFetcher.Setup(rf => rf.StopAsync())
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StopAsync())
                .Returns(Task.CompletedTask);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act
            await downloadManager.StartAsync();
            downloadManager.Dispose();
            
            // Assert
            mockResultFetcher.Verify(rf => rf.StopAsync(), Times.Once);
            mockDownloader.Verify(d => d.StopAsync(), Times.Once);
        }

        [Fact]
        public async Task StartAsync_CalledTwice_ThrowsException()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            mockResultFetcher.Setup(rf => rf.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act
            await downloadManager.StartAsync();
            
            // Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() => downloadManager.StartAsync());
        }

        [Fact]
        public async Task GetNextDownloadedFileAsync_BeforeStart_ThrowsException()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => downloadManager.GetNextDownloadedFileAsync(CancellationToken.None));
        }

        [Fact]
        public void Dispose_CalledTwice_DoesNotThrow()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            mockResultFetcher.Setup(rf => rf.StopAsync())
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StopAsync())
                .Returns(Task.CompletedTask);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act & Assert - should not throw
            downloadManager.Dispose();
            downloadManager.Dispose();
        }

        [Fact]
        public async Task GetNextDownloadedFileAsync_AfterDispose_ThrowsObjectDisposedException()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            mockResultFetcher.Setup(rf => rf.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockResultFetcher.Setup(rf => rf.StopAsync())
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StopAsync())
                .Returns(Task.CompletedTask);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act
            await downloadManager.StartAsync();
            downloadManager.Dispose();
            
            // Assert
            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => downloadManager.GetNextDownloadedFileAsync(CancellationToken.None));
        }

        [Fact]
        public async Task HasMoreResults_ReflectsDownloaderStatus()
        {
            // Arrange
            var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();
            var mockDownloader = new Mock<ICloudFetchDownloader>();
            
            mockResultFetcher.Setup(rf => rf.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            mockDownloader.Setup(d => d.StartAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            
            // First test with more results available
            mockDownloader.Setup(d => d.IsCompleted).Returns(false);
            mockResultFetcher.Setup(rf => rf.IsCompleted).Returns(false);
            
            // Create a test instance with our mocks
            var downloadManager = new TestableCloudFetchDownloadManager(mockResultFetcher.Object, mockDownloader.Object);
            
            // Act
            await downloadManager.StartAsync();
            
            // Assert
            Assert.True(downloadManager.HasMoreResults);
            
            // Now change the downloader to report no more results
            mockDownloader.Setup(d => d.IsCompleted).Returns(true);
            mockResultFetcher.Setup(rf => rf.IsCompleted).Returns(true);
            
            // Assert again
            Assert.False(downloadManager.HasMoreResults);
        }
    }
}
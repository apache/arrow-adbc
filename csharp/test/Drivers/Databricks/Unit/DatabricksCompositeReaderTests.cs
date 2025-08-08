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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    /// <summary>
    /// Testable subclass of DatabricksCompositeReader for unit testing
    /// </summary>
    internal class TestableDatabricksCompositeReader : DatabricksCompositeReader
    {
        public bool CloudFetchReaderCreated { get; private set; }
        public bool DatabricksReaderCreated { get; private set; }
        public BaseDatabricksReader? MockReader { get; set; }
        public TFetchResultsResp? LastInitialResults { get; private set; }

        public TestableDatabricksCompositeReader(
            IHiveServer2Statement statement,
            Schema schema,
            bool isLz4Compressed,
            HttpClient httpClient,
            IOperationStatusPoller? operationPoller = null)
            : base(statement, schema, isLz4Compressed, httpClient, operationPoller)
        {
        }

        protected override BaseDatabricksReader CreateCloudFetchReader(TFetchResultsResp initialResults)
        {
            CloudFetchReaderCreated = true;
            LastInitialResults = initialResults;
            return MockReader!;
        }

        protected override BaseDatabricksReader CreateDatabricksReader(TFetchResultsResp initialResults)
        {
            DatabricksReaderCreated = true;
            LastInitialResults = initialResults;
            return MockReader!;
        }

        public new BaseDatabricksReader DetermineReader(TFetchResultsResp initialResults)
        {
            return base.DetermineReader(initialResults);
        }
    }

    public class DatabricksCompositeReaderTests : IDisposable
    {
        private readonly Schema _testSchema;
        private readonly HttpClient _httpClient;

        public DatabricksCompositeReaderTests()
        {
            _testSchema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, true))
                .Field(new Field("name", StringType.Default, true))
                .Build();

            _httpClient = new HttpClient();
        }

        [Fact]
        public void Constructor_WithValidParameters_InitializesSuccessfully()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(false);
            var mockPoller = new Mock<IOperationStatusPoller>();

            // Act
            using var reader = new DatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object);

            // Assert
            Assert.NotNull(reader);
            Assert.Equal(_testSchema, reader.Schema);
            mockPoller.Verify(p => p.Start(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public void Constructor_WithDirectResultsAndNoMoreRows_DoesNotStartPoller()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            var directResults = new TSparkDirectResults
            {
                ResultSet = new TFetchResultsResp
                {
                    HasMoreRows = false,
                    Results = new TRowSet()
                }
            };
            mockStatement.Setup(s => s.HasDirectResults).Returns(true);
            mockStatement.Setup(s => s.DirectResults).Returns(directResults);

            var mockPoller = new Mock<IOperationStatusPoller>();

            // Act
            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object);

            // Assert
            mockPoller.Verify(p => p.Start(It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public void DetermineReader_WithResultLinks_CreatesCloudFetchReader()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(false);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, false);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet
                {
                    ResultLinks = new List<TSparkArrowResultLink>
                    {
                        new TSparkArrowResultLink
                        {}
                    }
                }
            };

            // Act
            var result = reader.DetermineReader(fetchResults);

            // Assert
            Assert.True(reader.CloudFetchReaderCreated);
            Assert.False(reader.DatabricksReaderCreated);
            Assert.Equal(fetchResults, reader.LastInitialResults);
            Assert.Equal(mockReader.Object, result);
        }

        [Fact]
        public void DetermineReader_WithoutResultLinks_CreatesDatabricksReader()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(false);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, false);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet()
            };

            // Act
            var result = reader.DetermineReader(fetchResults);

            // Assert
            Assert.False(reader.CloudFetchReaderCreated);
            Assert.True(reader.DatabricksReaderCreated);
            Assert.Equal(fetchResults, reader.LastInitialResults);
            Assert.Equal(mockReader.Object, result);
        }

        [Fact]
        public void DetermineReader_WithEmptyResultLinks_CreatesDatabricksReader()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(false);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, false);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet
                {
                    ResultLinks = new List<TSparkArrowResultLink>
                    {}
                }
            };

            // Act
            var result = reader.DetermineReader(fetchResults);

            // Assert
            Assert.False(reader.CloudFetchReaderCreated);
            Assert.True(reader.DatabricksReaderCreated);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_WithNoActiveReader_FetchesAndDelegates()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(false);
            mockStatement.Setup(s => s.OperationHandle).Returns(new TOperationHandle());
            mockStatement.Setup(s => s.BatchSize).Returns(1000);

            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };

            var mockClient = new Mock<TCLIService.IAsync>();
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            mockStatement.Setup(s => s.Client).Returns(mockClient.Object);

            var mockPoller = new Mock<IOperationStatusPoller>();

            var expectedBatch = new RecordBatch(_testSchema, new IArrowArray[] { }, 0);
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(expectedBatch);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Act
            var result = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal(expectedBatch, result);
            mockClient.Verify(c => c.FetchResults(
                It.Is<TFetchResultsReq>(req => req.Orientation == TFetchOrientation.FETCH_NEXT && req.MaxRows == 1000),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_WhenReturnsNull_StopsPoller()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(false);
            mockStatement.Setup(s => s.OperationHandle).Returns(new TOperationHandle());
            mockStatement.Setup(s => s.BatchSize).Returns(1000);

            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };

            var mockClient = new Mock<TCLIService.IAsync>();
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            mockStatement.Setup(s => s.Client).Returns(mockClient.Object);

            var mockPoller = new Mock<IOperationStatusPoller>();
            mockPoller.Setup(p => p.IsStarted).Returns(true);

            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((RecordBatch?)null);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Act
            var result = await reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.Null(result);
            mockPoller.Verify(p => p.Stop(), Times.Once);
        }

        [Fact]
        public void Dispose_DisposesActiveReaderAndPoller()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(true);
            mockStatement.Setup(s => s.DirectResults).Returns(new TSparkDirectResults
            {
                ResultSet = new TFetchResultsResp
                {
                    HasMoreRows = true,
                    Results = new TRowSet()
                }
            });

            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, false);

            var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Act
            reader.Dispose();

            // Assert
            mockPoller.Verify(p => p.Stop(), Times.Once);
            mockPoller.Verify(p => p.Dispose(), Times.Once);
            // todo: add assertion to verify mockReader was disposed
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_WithCancellation_PropagatesCancellation()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.HasDirectResults).Returns(false);
            mockStatement.Setup(s => s.OperationHandle).Returns(new TOperationHandle());
            mockStatement.Setup(s => s.BatchSize).Returns(1000);

            var cts = new CancellationTokenSource();
            cts.Cancel();

            var mockClient = new Mock<TCLIService.IAsync>();
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new OperationCanceledException());
            mockStatement.Setup(s => s.Client).Returns(mockClient.Object);

            var mockPoller = new Mock<IOperationStatusPoller>();

            using var reader = new DatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object);

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                reader.ReadNextRecordBatchAsync(cts.Token).AsTask());
        }

        [Fact]
        public void Constructor_WithDirectResultsAndMoreRows_StartsPoller()
        {
            // Arrange
            var mockStatement = new Mock<IHiveServer2Statement>();
            var directResults = new TSparkDirectResults
            {
                ResultSet = new TFetchResultsResp
                {
                    HasMoreRows = true,
                    Results = new TRowSet()
                }
            };
            mockStatement.Setup(s => s.HasDirectResults).Returns(true);
            mockStatement.Setup(s => s.DirectResults).Returns(directResults);

            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, false);

            // Act
            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Assert
            Assert.True(reader.DatabricksReaderCreated); // Should create reader from direct results
            mockPoller.Verify(p => p.Start(It.IsAny<CancellationToken>()), Times.Once);
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }
}
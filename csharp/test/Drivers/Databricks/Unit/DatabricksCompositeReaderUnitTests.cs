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

        public TestableDatabricksCompositeReader(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            bool isLz4Compressed,
            HttpClient httpClient,
            IOperationStatusPoller? operationPoller = null)
            : base(statement, schema, response, isLz4Compressed, httpClient, operationPoller)
        {
        }

        protected override BaseDatabricksReader CreateCloudFetchReader(TFetchResultsResp initialResults)
        {
            CloudFetchReaderCreated = true;
            return MockReader!;
        }

        protected override BaseDatabricksReader CreateDatabricksReader(TFetchResultsResp initialResults)
        {
            DatabricksReaderCreated = true;
            return MockReader!;
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

        private Mock<IHiveServer2Statement> CreateMockStatement(Mock<TCLIService.IAsync>? mockClient = null, TSparkDirectResults? directResults = null)
        {
            var mockStatement = new Mock<IHiveServer2Statement>();
            mockStatement.Setup(s => s.QueryTimeoutSeconds).Returns(10);

            if (mockClient != null)
            {
                mockStatement.Setup(s => s.Client).Returns(mockClient.Object);
            }

            if (directResults != null)
            {
                mockStatement
                    .Setup(s => s.TryGetDirectResults(It.IsAny<IResponse>(), out It.Ref<TSparkDirectResults?>.IsAny))
                    .Returns((IResponse response, out TSparkDirectResults? result) =>
                    {
                        result = directResults;
                        return true;
                    });
            }
            else
            {
                TSparkDirectResults? nullDirectResults = null;
                mockStatement.Setup(s => s.TryGetDirectResults(It.IsAny<IResponse>(), out nullDirectResults))
                    .Returns(false);
            }

            return mockStatement;
        }

        private Mock<TCLIService.IAsync> CreateMockClient()
        {
            var mockClient = new Mock<TCLIService.IAsync>();
            var closeOperationResponse = new TCloseOperationResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS }
            };
            mockClient.Setup(c => c.CloseOperation(It.IsAny<TCloseOperationReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(closeOperationResponse);
            return mockClient;
        }

        private Mock<IResponse> CreateMockResponse(TSparkDirectResults? directResults = null, TOperationHandle? operationHandle = null)
        {
            var mockResponse = new Mock<IResponse>();
            mockResponse.Setup(r => r.OperationHandle).Returns(operationHandle ?? new TOperationHandle());

            if (directResults != null)
            {
                mockResponse.Setup(r => r.DirectResults).Returns(directResults);
            }

            return mockResponse;
        }

        [Fact]
        public void Constructor_WithValidParameters_InitializesSuccessfully()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockResponse = CreateMockResponse();

            // Act
            using var reader = new DatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
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
            var directResults = new TSparkDirectResults
            {
                ResultSet = new TFetchResultsResp
                {
                    HasMoreRows = false,
                    Results = new TRowSet()
                },
                ResultSetMetadata = new TGetResultSetMetadataResp
                {},
                __isset = new TSparkDirectResults.Isset { resultSet = true }
            };

            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient, directResults: directResults);
            var mockResponse = CreateMockResponse(directResults: directResults);
            var mockPoller = new Mock<IOperationStatusPoller>();

            // Act
            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object);

            // Assert
            mockPoller.Verify(p => p.Start(It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public void ShouldUseCloudFetch_WithResultLinks_ReturnsTrue()
        {
            // Arrange
            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet
                {
                    ResultLinks = new List<TSparkArrowResultLink>
                    {
                        new TSparkArrowResultLink {}
                    }
                }
            };

            // Act
            var result = DatabricksCompositeReader.ShouldUseCloudFetch(fetchResults);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ShouldUseCloudFetch_WithoutResultLinks_ReturnsFalse()
        {
            // Arrange
            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet()
            };

            // Act
            var result = DatabricksCompositeReader.ShouldUseCloudFetch(fetchResults);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ShouldUseCloudFetch_WithEmptyResultLinks_ReturnsFalse()
        {
            // Arrange
            var fetchResults = new TFetchResultsResp
            {
                Results = new TRowSet
                {
                    ResultLinks = new List<TSparkArrowResultLink> {}
                }
            };

            // Act
            var result = DatabricksCompositeReader.ShouldUseCloudFetch(fetchResults);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_WithNoActiveReader_FetchesAndDelegates()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            var mockStatement = CreateMockStatement(mockClient);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockResponse = CreateMockResponse();
            var expectedBatch = new RecordBatch(_testSchema, new IArrowArray[] { }, 0);
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(expectedBatch);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
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
                It.IsAny<TFetchResultsReq>(),
                It.IsAny<CancellationToken>()), Times.Once);
            // verify that the reader was created
            Assert.True(reader.DatabricksReaderCreated);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_WhenReturnsNull_StopsPoller()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            var mockStatement = CreateMockStatement(mockClient);
            var mockPoller = new Mock<IOperationStatusPoller>();
            mockPoller.Setup(p => p.IsStarted).Returns(true);
            var mockResponse = CreateMockResponse();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((RecordBatch?)null);

            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
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
        public void Constructor_WithDirectResultsAndMoreRows_StartsPoller()
        {
            // Arrange
            var directResults = new TSparkDirectResults
            {
                ResultSet = new TFetchResultsResp
                {
                    HasMoreRows = true,
                    Results = new TRowSet()
                },
                __isset = new TSparkDirectResults.Isset { resultSet = true }
            };

            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient, directResults: directResults);
            var mockResponse = CreateMockResponse(directResults: directResults);
            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);

            // Act
            using var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
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

        [Fact]
        public void Dispose_WithNoActiveReader_CallsCloseOperationDirectly()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var mockStatement = CreateMockStatement(mockClient);

            var mockPoller = new Mock<IOperationStatusPoller>();
            var operationHandle = new TOperationHandle();
            var mockResponse = CreateMockResponse(directResults: null, operationHandle: operationHandle);

            var reader = new DatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object);

            // Act
            reader.Dispose();

            // Assert
            mockClient.Verify(c => c.CloseOperation(
                It.Is<TCloseOperationReq>(req => req.OperationHandle == operationHandle),
                It.IsAny<CancellationToken>()), Times.Once);
            mockPoller.Verify(p => p.Stop(), Times.Once);
            mockPoller.Verify(p => p.Dispose(), Times.Once);
        }

        [Fact]
        public async Task Dispose_WithActiveReader_CallsReaderCloseOperation()
        {
            // Arrange
            var mockClient = CreateMockClient();
            var fetchResponse = new TFetchResultsResp
            {
                HasMoreRows = false,
                Results = new TRowSet()
            };
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResponse);
            var mockStatement = CreateMockStatement(mockClient);

            var mockPoller = new Mock<IOperationStatusPoller>();
            var mockResponse = CreateMockResponse();
            var mockReader = new Mock<BaseDatabricksReader>(mockStatement.Object, _testSchema, mockResponse.Object, false);
            mockReader.Setup(r => r.ReadNextRecordBatchAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((RecordBatch?)null);
            mockReader.Setup(r => r.Dispose());

            var reader = new TestableDatabricksCompositeReader(
                mockStatement.Object,
                _testSchema,
                mockResponse.Object,
                false,
                _httpClient,
                mockPoller.Object)
            {
                MockReader = mockReader.Object
            };

            // Trigger creation of active reader
            _ = await reader.ReadNextRecordBatchAsync();

            // Act
            reader.Dispose();

            // Assert
            mockReader.Verify(r => r.Dispose(), Times.Once);
            mockPoller.Verify(p => p.Stop(), Times.Once);
            mockPoller.Verify(p => p.Dispose(), Times.Once);
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }
}

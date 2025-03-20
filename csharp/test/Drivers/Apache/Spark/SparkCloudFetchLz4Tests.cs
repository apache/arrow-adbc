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
using System.IO;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using K4os.Compression.LZ4;
using Moq;
using Moq.Protected;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Tests for the LZ4 decompression functionality in SparkCloudFetchReader.
    /// </summary>
    public class SparkCloudFetchLz4Tests : IDisposable
    {
        private readonly string _testDataDir;
        private readonly HttpClient _httpClient;
        private readonly Mock<HttpMessageHandler> _mockHttpHandler;

        public SparkCloudFetchLz4Tests()
        {
            // Create a test data directory
            _testDataDir = Path.Combine(Path.GetTempPath(), "SparkCloudFetchLz4Tests");
            Directory.CreateDirectory(_testDataDir);

            // Set up HTTP client with mock handler for testing
            _mockHttpHandler = new Mock<HttpMessageHandler>();
            _httpClient = new HttpClient(_mockHttpHandler.Object);
        }

        public void Dispose()
        {
            _httpClient.Dispose();
            
            try
            {
                Directory.Delete(_testDataDir, true);
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }

        /// <summary>
        /// Creates test Arrow data.
        /// </summary>
        private byte[] CreateTestArrowData()
        {
            // Create a simple schema
            var builder = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, false))
                .Field(new Field("name", StringType.Default, true))
                .Field(new Field("value", DoubleType.Default, true));
            var schema = builder.Build();

            // Create record batch
            var idBuilder = new Int32Array.Builder().Reserve(5);
            var nameBuilder = new StringArray.Builder().Reserve(5);
            var valueBuilder = new DoubleArray.Builder().Reserve(5);

            for (int i = 0; i < 5; i++)
            {
                idBuilder.Append(i);
                nameBuilder.Append($"Name {i}");
                valueBuilder.Append(i * 1.5);
            }

            var arrays = new IArrowArray[] 
            {
                idBuilder.Build(),
                nameBuilder.Build(),
                valueBuilder.Build()
            };
            var recordBatch = new RecordBatch(schema, arrays, 5);

            // Write to memory stream
            using (var memoryStream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(memoryStream, schema))
                {
                    writer.WriteRecordBatch(recordBatch);
                }
                return memoryStream.ToArray();
            }
        }

        /// <summary>
        /// Compresses data using LZ4.
        /// </summary>
        private byte[] CompressWithLz4(byte[] data)
        {
            var maxLength = LZ4Codec.MaximumOutputSize(data.Length);
            var output = new byte[maxLength];
            var encodedLength = LZ4Codec.Encode(
                data.AsSpan(),
                output.AsSpan(),
                LZ4Level.L00_FAST);
            
            return output.AsSpan(0, encodedLength).ToArray();
        }

        /// <summary>
        /// Tests that the SparkCloudFetchReader can decompress LZ4 compressed data.
        /// </summary>
        [Fact]
        public async Task TestCloudFetchReaderDecompressesLz4Data()
        {
            // Create test Arrow data
            byte[] arrowData = CreateTestArrowData();
            
            // Compress the data with LZ4
            byte[] compressedData = CompressWithLz4(arrowData);
            
            // Set up the mock HTTP handler to return our compressed test data
            string testUrl = "https://example.com/test.arrow.lz4";
            _mockHttpHandler.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.Is<HttpRequestMessage>(req => req.RequestUri != null && req.RequestUri.ToString() == testUrl),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new ByteArrayContent(compressedData)
                });

            // Create a mock statement and connection
            var mockConnection = new Mock<SparkDatabricksConnection>(new Dictionary<string, string>()) { CallBase = true };
            var mockStatement = new Mock<HiveServer2Statement>(mockConnection.Object) { CallBase = true };
            var mockClient = new Mock<TCLIService.Client>();

            // Set up the mock client to return URL-based results
            var operationHandle = new TOperationHandle { HasResultSet = true };
            
            // Mock FetchResults
            var fetchResp = new TFetchResultsResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS },
                HasMoreRows = false,
                Results = new TRowSet
                {
                    StartRowOffset = 0,
                    Rows = new List<TRow>(),
                    ResultLinks = new List<TSparkArrowResultLink>
                    {
                        new TSparkArrowResultLink
                        {
                            FileLink = testUrl,
                            ExpiryTime = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds(),
                            StartRowOffset = 0,
                            RowCount = 5,
                            BytesNum = arrowData.Length // Uncompressed size
                        }
                    }
                }
            };
            fetchResp.Results.__isset.resultLinks = true;
            
            mockClient.Setup(c => c.FetchResults(It.IsAny<TFetchResultsReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchResp);

            // Set up the mock connection
            mockConnection.Setup(c => c.Client).Returns(mockClient.Object);
            
            // Create a schema for testing
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, false))
                .Field(new Field("name", StringType.Default, true))
                .Field(new Field("value", DoubleType.Default, true))
                .Build();
            
            // Create the CloudFetchReader with our mocked objects and LZ4 compression enabled
            var reader = new SparkCloudFetchReader(mockStatement.Object, schema, true);
            var httpClientField = typeof(SparkCloudFetchReader).GetField("_httpClient", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new InvalidOperationException("Could not find _httpClient field");
            httpClientField.SetValue(reader, _httpClient);
            
            // Read the data
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            
            // Verify the data
            Assert.NotNull(recordBatch);
            Assert.Equal(5, recordBatch.Length);
            Assert.Equal(3, recordBatch.ColumnCount);
            
            // Verify column values
            var idColumn = recordBatch.Column(0) as Int32Array;
            var nameColumn = recordBatch.Column(1) as StringArray;
            var valueColumn = recordBatch.Column(2) as DoubleArray;
            
            Assert.NotNull(idColumn);
            Assert.NotNull(nameColumn);
            Assert.NotNull(valueColumn);
            
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(i, idColumn.GetValue(i));
                Assert.Equal($"Name {i}", nameColumn.GetString(i));
                Assert.Equal(i * 1.5, valueColumn.GetValue(i));
            }
            
            // Verify there are no more batches
            Assert.Null(await reader.ReadNextRecordBatchAsync());
            
            // Verify that the HTTP request was made
            _mockHttpHandler.Protected().Verify(
                "SendAsync",
                Times.Exactly(1),
                ItExpr.Is<HttpRequestMessage>(req => req.RequestUri != null && req.RequestUri.ToString() == testUrl),
                ItExpr.IsAny<CancellationToken>());
        }
    }
}
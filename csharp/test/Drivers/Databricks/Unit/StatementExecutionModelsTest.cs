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
using System.Text.Json;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// Unit tests for Statement Execution API model serialization and deserialization.
    /// </summary>
    public class StatementExecutionModelsTest
    {
        private readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        };

        /// <summary>
        /// Tests CreateSessionRequest serialization.
        /// </summary>
        [Fact]
        public void CreateSessionRequest_Serialization_IsCorrect()
        {
            // Arrange
            var request = new CreateSessionRequest
            {
                WarehouseId = "test-warehouse-id",
                Catalog = "test-catalog",
                Schema = "test-schema",
                SessionConfigs = new Dictionary<string, string>
                {
                    ["spark.sql.shuffle.partitions"] = "200",
                    ["spark.sql.adaptive.enabled"] = "true"
                }
            };

            // Act
            var json = JsonSerializer.Serialize(request, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<CreateSessionRequest>(json, _jsonOptions);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Equal(request.WarehouseId, deserialized.WarehouseId);
            Assert.Equal(request.Catalog, deserialized.Catalog);
            Assert.Equal(request.Schema, deserialized.Schema);
            Assert.Equal(2, deserialized.SessionConfigs?.Count);
            Assert.Contains("warehouse_id", json);
        }

        /// <summary>
        /// Tests CreateSessionResponse deserialization.
        /// </summary>
        [Fact]
        public void CreateSessionResponse_Deserialization_IsCorrect()
        {
            // Arrange
            var json = @"{""session_id"": ""session-123-456""}";

            // Act
            var response = JsonSerializer.Deserialize<CreateSessionResponse>(json, _jsonOptions);

            // Assert
            Assert.NotNull(response);
            Assert.Equal("session-123-456", response.SessionId);
        }

        /// <summary>
        /// Tests ExecuteStatementRequest serialization with all fields.
        /// </summary>
        [Fact]
        public void ExecuteStatementRequest_FullSerialization_IsCorrect()
        {
            // Arrange
            var request = new ExecuteStatementRequest
            {
                SessionId = "session-123",
                Statement = "SELECT * FROM table",
                Catalog = "main",
                Schema = "default",
                Parameters = new List<StatementParameter>
                {
                    new StatementParameter { Name = "id", Type = "INT", Value = 42 }
                },
                Disposition = DatabricksConstants.ResultDispositions.ExternalLinks,
                Format = DatabricksConstants.ResultFormats.ArrowStream,
                ResultCompression = DatabricksConstants.ResultCompressions.Lz4,
                WaitTimeout = "10s",
                RowLimit = 1000
            };

            // Act
            var json = JsonSerializer.Serialize(request, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<ExecuteStatementRequest>(json, _jsonOptions);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Equal(request.SessionId, deserialized.SessionId);
            Assert.Equal(request.Statement, deserialized.Statement);
            Assert.Equal(request.Disposition, deserialized.Disposition);
            Assert.Equal(request.Format, deserialized.Format);
            Assert.Equal(request.RowLimit, deserialized.RowLimit);
            Assert.Contains("session_id", json);
            Assert.Contains("result_compression", json);
        }

        /// <summary>
        /// Tests ExecuteStatementRequest serialization with minimal fields.
        /// </summary>
        [Fact]
        public void ExecuteStatementRequest_MinimalSerialization_IsCorrect()
        {
            // Arrange
            var request = new ExecuteStatementRequest
            {
                WarehouseId = "warehouse-123",
                Statement = "SHOW TABLES"
            };

            // Act
            var json = JsonSerializer.Serialize(request, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<ExecuteStatementRequest>(json, _jsonOptions);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Equal(request.WarehouseId, deserialized.WarehouseId);
            Assert.Equal(request.Statement, deserialized.Statement);
        }

        /// <summary>
        /// Tests ExecuteStatementResponse deserialization with successful result.
        /// </summary>
        [Fact]
        public void ExecuteStatementResponse_SuccessDeserialization_IsCorrect()
        {
            // Arrange
            var json = @"{
                ""statement_id"": ""stmt-123"",
                ""status"": {
                    ""state"": ""SUCCEEDED""
                },
                ""manifest"": {
                    ""format"": ""arrow_stream"",
                    ""total_chunk_count"": 1,
                    ""total_row_count"": 100,
                    ""total_byte_count"": 1024,
                    ""schema"": {
                        ""columns"": []
                    }
                }
            }";

            // Act
            var response = JsonSerializer.Deserialize<ExecuteStatementResponse>(json, _jsonOptions);

            // Assert
            Assert.NotNull(response);
            Assert.Equal("stmt-123", response.StatementId);
            Assert.Equal("SUCCEEDED", response.Status.State);
            Assert.NotNull(response.Manifest);
            Assert.Equal(100, response.Manifest.TotalRowCount);
        }

        /// <summary>
        /// Tests StatementStatus deserialization with error.
        /// </summary>
        [Fact]
        public void StatementStatus_ErrorDeserialization_IsCorrect()
        {
            // Arrange
            var json = @"{
                ""state"": ""FAILED"",
                ""error"": {
                    ""error_code"": ""PARSE_ERROR"",
                    ""message"": ""Syntax error in SQL statement"",
                    ""sql_state"": ""42000""
                }
            }";

            // Act
            var status = JsonSerializer.Deserialize<StatementStatus>(json, _jsonOptions);

            // Assert
            Assert.NotNull(status);
            Assert.Equal("FAILED", status.State);
            Assert.NotNull(status.Error);
            Assert.Equal("PARSE_ERROR", status.Error.ErrorCode);
            Assert.Equal("Syntax error in SQL statement", status.Error.Message);
            Assert.Equal("42000", status.Error.SqlState);
        }

        /// <summary>
        /// Tests ResultManifest deserialization with chunks.
        /// </summary>
        [Fact]
        public void ResultManifest_WithChunks_DeserializationIsCorrect()
        {
            // Arrange
            var json = @"{
                ""format"": ""arrow_stream"",
                ""total_chunk_count"": 2,
                ""total_row_count"": 1000,
                ""total_byte_count"": 50000,
                ""result_compression"": ""lz4"",
                ""schema"": {
                    ""columns"": [
                        {
                            ""name"": ""id"",
                            ""type_name"": ""INT"",
                            ""position"": 0,
                            ""nullable"": false
                        }
                    ]
                },
                ""chunks"": [
                    {
                        ""chunk_index"": 0,
                        ""row_count"": 500,
                        ""row_offset"": 0,
                        ""byte_count"": 25000
                    }
                ]
            }";

            // Act
            var manifest = JsonSerializer.Deserialize<ResultManifest>(json, _jsonOptions);

            // Assert
            Assert.NotNull(manifest);
            Assert.Equal("arrow_stream", manifest.Format);
            Assert.Equal(2, manifest.TotalChunkCount);
            Assert.Equal(1000, manifest.TotalRowCount);
            Assert.Equal("lz4", manifest.ResultCompression);
            Assert.NotNull(manifest.Schema);
            Assert.Single(manifest.Schema.Columns!);
            Assert.Single(manifest.Chunks!);
            Assert.Equal(500, manifest.Chunks![0].RowCount);
        }

        /// <summary>
        /// Tests ResultChunk with external links deserialization.
        /// </summary>
        [Fact]
        public void ResultChunk_WithExternalLinks_DeserializationIsCorrect()
        {
            // Arrange
            var json = @"{
                ""chunk_index"": 0,
                ""row_count"": 1000,
                ""row_offset"": 0,
                ""byte_count"": 50000,
                ""external_links"": [
                    {
                        ""external_link"": ""https://s3.amazonaws.com/bucket/file"",
                        ""expiration"": ""2025-12-31T23:59:59Z"",
                        ""chunk_index"": 0,
                        ""row_count"": 1000,
                        ""row_offset"": 0,
                        ""byte_count"": 50000
                    }
                ]
            }";

            // Act
            var chunk = JsonSerializer.Deserialize<ResultChunk>(json, _jsonOptions);

            // Assert
            Assert.NotNull(chunk);
            Assert.Equal(0, chunk.ChunkIndex);
            Assert.Equal(1000, chunk.RowCount);
            Assert.NotNull(chunk.ExternalLinks);
            Assert.Single(chunk.ExternalLinks);
            Assert.Equal("https://s3.amazonaws.com/bucket/file", chunk.ExternalLinks[0].ExternalLinkUrl);
            Assert.Equal("2025-12-31T23:59:59Z", chunk.ExternalLinks[0].Expiration);
        }

        /// <summary>
        /// Tests ExternalLink with HTTP headers deserialization.
        /// </summary>
        [Fact]
        public void ExternalLink_WithHttpHeaders_DeserializationIsCorrect()
        {
            // Arrange
            var json = @"{
                ""external_link"": ""https://storage.azure.com/container/file"",
                ""expiration"": ""2025-12-31T23:59:59Z"",
                ""chunk_index"": 0,
                ""row_count"": 500,
                ""row_offset"": 0,
                ""byte_count"": 25000,
                ""http_headers"": {
                    ""x-ms-blob-type"": ""BlockBlob"",
                    ""Authorization"": ""Bearer token""
                }
            }";

            // Act
            var link = JsonSerializer.Deserialize<ExternalLink>(json, _jsonOptions);

            // Assert
            Assert.NotNull(link);
            Assert.Equal("https://storage.azure.com/container/file", link.ExternalLinkUrl);
            Assert.NotNull(link.HttpHeaders);
            Assert.Equal(2, link.HttpHeaders.Count);
            Assert.Equal("BlockBlob", link.HttpHeaders["x-ms-blob-type"]);
        }

        /// <summary>
        /// Tests ColumnInfo deserialization with all fields.
        /// </summary>
        [Fact]
        public void ColumnInfo_FullDeserialization_IsCorrect()
        {
            // Arrange
            var json = @"{
                ""name"": ""price"",
                ""type_name"": ""DECIMAL"",
                ""type_text"": ""decimal(10,2)"",
                ""precision"": 10,
                ""scale"": 2,
                ""position"": 5,
                ""nullable"": true,
                ""comment"": ""Product price""
            }";

            // Act
            var columnInfo = JsonSerializer.Deserialize<ColumnInfo>(json, _jsonOptions);

            // Assert
            Assert.NotNull(columnInfo);
            Assert.Equal("price", columnInfo.Name);
            Assert.Equal("DECIMAL", columnInfo.TypeName);
            Assert.Equal("decimal(10,2)", columnInfo.TypeText);
            Assert.Equal(10, columnInfo.Precision);
            Assert.Equal(2, columnInfo.Scale);
            Assert.Equal(5, columnInfo.Position);
            Assert.True(columnInfo.Nullable);
            Assert.Equal("Product price", columnInfo.Comment);
        }

        /// <summary>
        /// Tests StatementParameter serialization.
        /// </summary>
        [Fact]
        public void StatementParameter_Serialization_IsCorrect()
        {
            // Arrange
            var parameter = new StatementParameter
            {
                Name = "userId",
                Type = "BIGINT",
                Value = 12345L
            };

            // Act
            var json = JsonSerializer.Serialize(parameter, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<StatementParameter>(json, _jsonOptions);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Equal(parameter.Name, deserialized.Name);
            Assert.Equal(parameter.Type, deserialized.Type);
            Assert.Contains("name", json);
            Assert.Contains("type", json);
        }

        /// <summary>
        /// Tests that null optional fields are handled correctly.
        /// </summary>
        [Fact]
        public void Models_NullOptionalFields_AreHandledCorrectly()
        {
            // Arrange
            var request = new ExecuteStatementRequest
            {
                WarehouseId = "warehouse-123",
                Statement = "SELECT 1",
                Catalog = null,
                Schema = null,
                Parameters = null
            };

            // Act
            var json = JsonSerializer.Serialize(request, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<ExecuteStatementRequest>(json, _jsonOptions);

            // Assert
            Assert.NotNull(deserialized);
            Assert.Null(deserialized.Catalog);
            Assert.Null(deserialized.Schema);
            Assert.Null(deserialized.Parameters);
        }

        /// <summary>
        /// Tests ResultChunk with inline data array.
        /// </summary>
        [Fact]
        public void ResultChunk_WithInlineData_DeserializationIsCorrect()
        {
            // Arrange
            var json = @"{
                ""chunk_index"": 0,
                ""row_count"": 2,
                ""row_offset"": 0,
                ""byte_count"": 100,
                ""data_array"": [
                    [1, ""Alice""],
                    [2, ""Bob""]
                ]
            }";

            // Act
            var chunk = JsonSerializer.Deserialize<ResultChunk>(json, _jsonOptions);

            // Assert
            Assert.NotNull(chunk);
            Assert.Equal(2, chunk.RowCount);
            Assert.NotNull(chunk.DataArray);
            Assert.Equal(2, chunk.DataArray.Count);
            Assert.Equal(2, chunk.DataArray[0].Count);
        }

        /// <summary>
        /// Tests GetStatementResponse deserialization.
        /// </summary>
        [Fact]
        public void GetStatementResponse_Deserialization_IsCorrect()
        {
            // Arrange
            var json = @"{
                ""statement_id"": ""stmt-456"",
                ""status"": {
                    ""state"": ""RUNNING""
                }
            }";

            // Act
            var response = JsonSerializer.Deserialize<GetStatementResponse>(json, _jsonOptions);

            // Assert
            Assert.NotNull(response);
            Assert.Equal("stmt-456", response.StatementId);
            Assert.Equal("RUNNING", response.Status.State);
        }

        /// <summary>
        /// Tests that empty collections are handled correctly.
        /// </summary>
        [Fact]
        public void Models_EmptyCollections_AreHandledCorrectly()
        {
            // Arrange
            var request = new CreateSessionRequest
            {
                WarehouseId = "warehouse-123",
                SessionConfigs = new Dictionary<string, string>()
            };

            // Act
            var json = JsonSerializer.Serialize(request, _jsonOptions);
            var deserialized = JsonSerializer.Deserialize<CreateSessionRequest>(json, _jsonOptions);

            // Assert
            Assert.NotNull(deserialized);
            Assert.NotNull(deserialized.SessionConfigs);
            Assert.Empty(deserialized.SessionConfigs);
        }
    }
}

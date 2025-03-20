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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Tests for the SparkDatabricksConnection and SparkCloudFetchReader integration.
    /// </summary>
    public class SparkCloudFetchIntegrationTests
    {
        /// <summary>
        /// Tests that the SparkDatabricksConnection correctly creates a SparkCloudFetchReader when URL_BASED_SET is specified.
        /// </summary>
        [Fact]
        public void TestConnectionUsesCloudFetchReader()
        {
            // Create a mock connection
            var mockConnection = new Mock<SparkDatabricksConnection>(new Dictionary<string, string>()) { CallBase = true };
            var mockStatement = new Mock<SparkStatement>(mockConnection.Object) { CallBase = true };
            
            // Create a schema for testing
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, false))
                .Field(new Field("name", StringType.Default, true))
                .Build();
            
            // Set the result format to URL_BASED_SET
            var metadata = new TGetResultSetMetadataResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS },
                ResultFormat = TSparkRowSetType.URL_BASED_SET,
                Lz4Compressed = true
            };
            metadata.__isset.resultFormat = true;
            metadata.__isset.lz4Compressed = true;
            
            // Process the metadata
            var connection = mockConnection.Object;
            connection.ProcessResultSetMetadata(metadata);
            
            // Create a reader
            var reader = connection.NewReader(mockStatement.Object, schema);
            
            // Verify that the reader is a SparkCloudFetchReader
            Assert.IsType<SparkCloudFetchReader>(reader);
            
            // Verify that the reader has the correct properties
            var readerType = reader.GetType();
            var isLz4CompressedField = readerType.GetField("isLz4Compressed", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(isLz4CompressedField);
            var isLz4Compressed = (bool)(isLz4CompressedField?.GetValue(reader) ?? false);
            Assert.True(isLz4Compressed);
        }
        
        /// <summary>
        /// Tests that the SparkDatabricksConnection correctly creates a SparkDatabricksReader when ARROW_BASED_SET is specified.
        /// </summary>
        [Fact]
        public void TestConnectionUsesArrowReader()
        {
            // Create a mock connection
            var mockConnection = new Mock<SparkDatabricksConnection>(new Dictionary<string, string>()) { CallBase = true };
            var mockStatement = new Mock<SparkStatement>(mockConnection.Object) { CallBase = true };
            
            // Create a schema for testing
            var schema = new Schema.Builder()
                .Field(new Field("id", Int32Type.Default, false))
                .Field(new Field("name", StringType.Default, true))
                .Build();
            
            // Set the result format to ARROW_BASED_SET
            var metadata = new TGetResultSetMetadataResp
            {
                Status = new TStatus { StatusCode = TStatusCode.SUCCESS_STATUS },
                ResultFormat = TSparkRowSetType.ARROW_BASED_SET,
                Lz4Compressed = false
            };
            metadata.__isset.resultFormat = true;
            metadata.__isset.lz4Compressed = true;
            
            // Process the metadata
            var connection = mockConnection.Object;
            connection.ProcessResultSetMetadata(metadata);
            
            // Create a reader
            var reader = connection.NewReader(mockStatement.Object, schema);
            
            // Verify that the reader is a SparkDatabricksReader
            Assert.IsType<SparkDatabricksReader>(reader);
        }
        
        /// <summary>
        /// Tests that the SparkStatement correctly sets the CloudFetch options.
        /// </summary>
        [Fact]
        public void TestStatementSetsCloudFetchOptions()
        {
            // Create a mock connection and statement
            var mockConnection = new Mock<SparkConnection>(new Dictionary<string, string>()) { CallBase = true };
            var statement = new SparkStatement(mockConnection.Object);
            
            // Set the CloudFetch options
            statement.SetUseCloudFetch(true);
            statement.SetCanDecompressLz4(true);
            statement.SetMaxBytesPerFile(10 * 1024 * 1024); // 10MB
            
            // Create a TExecuteStatementReq
            var req = new TExecuteStatementReq();
            
            // Use reflection to call the protected SetStatementProperties method
            var method = typeof(SparkStatement).GetMethod("SetStatementProperties", 
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);
            method!.Invoke(statement, new object[] { req });
            
            // Verify that the options were set correctly
            Assert.True(req.CanDownloadResult);
            Assert.True(req.CanDecompressLZ4Result);
            Assert.Equal(10 * 1024 * 1024, req.MaxBytesPerFile);
        }
        
        /// <summary>
        /// Tests that the SparkStatement options can be set via the AdbcStatement interface.
        /// </summary>
        [Fact]
        public void TestStatementOptionsViaAdbcStatement()
        {
            // Create a connection
            var mockConnection = new Mock<SparkConnection>(new Dictionary<string, string>()) { CallBase = true };
            
            // Create an AdbcStatement with our SparkStatement
            var sparkStatement = new SparkStatement(mockConnection.Object);
            var adbcStatement = new SparkStatement(mockConnection.Object);
            
            // Set the options via the AdbcStatement interface
            adbcStatement.SetOption(SparkStatement.Options.UseCloudFetch, "false");
            adbcStatement.SetOption(SparkStatement.Options.CanDecompressLz4, "true");
            adbcStatement.SetOption(SparkStatement.Options.MaxBytesPerFile, "5242880"); // 5MB
            
            // Create a TExecuteStatementReq
            var req = new TExecuteStatementReq();
            
            // Use reflection to call the protected SetStatementProperties method
            var method = typeof(SparkStatement).GetMethod("SetStatementProperties", 
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);
            method!.Invoke(sparkStatement, new object[] { req });
            
            // Verify that the options were set correctly
            Assert.False(req.CanDownloadResult);
            Assert.True(req.CanDecompressLZ4Result);
            Assert.Equal(5242880, req.MaxBytesPerFile);
        }
    }
}
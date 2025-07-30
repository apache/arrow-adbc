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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    public class BaseDatabricksReaderTests
    {
        private readonly TestDatabricksStatement _testStatement;
        private readonly DatabricksOperationStatusPoller _poller;
        private readonly Schema _schema;
        private readonly TestableDatabricksReader _reader;
        private readonly Mock<TCLIService.IAsync> _mockClient;

        public BaseDatabricksReaderTests()
        {
            _mockClient = new Mock<TCLIService.IAsync>();
            _testStatement = new TestDatabricksStatement(_mockClient.Object);
            _poller = new DatabricksOperationStatusPoller(_testStatement, 1);
            _schema = new Schema(new[]
            {
                new Field("test", StringType.Default, false)
            }, new System.Collections.Generic.Dictionary<string, string>());
            
            _reader = new TestableDatabricksReader(_testStatement, _schema, _poller);
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_StopsPollerOnNull()
        {
            // Arrange
            _reader.SetNextResult(null);

            // Act
            var result = await _reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.Null(result);
            Assert.True(_reader.StopOperationStatusPollerCalled, "StopOperationStatusPoller should be called when null is returned");
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_StopsPollerOnException()
        {
            // Arrange
            var expectedException = new Exception("Test exception");
            _reader.SetNextException(expectedException);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(async () => await _reader.ReadNextRecordBatchAsync());
            Assert.Same(expectedException, exception);
            Assert.True(_reader.StopOperationStatusPollerCalled, "StopOperationStatusPoller should be called when exception is thrown");
        }

        [Fact]
        public async Task ReadNextRecordBatchAsync_DoesNotStopPollerOnSuccess()
        {
            // Arrange
            var recordBatch = new RecordBatch(_schema, new[] 
            { 
                new StringArray.Builder().Append("test").Build() 
            }, 1);
            _reader.SetNextResult(recordBatch);

            // Act
            var result = await _reader.ReadNextRecordBatchAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal(1, result.Length);
            Assert.False(_reader.StopOperationStatusPollerCalled, "StopOperationStatusPoller should not be called when data is returned");
        }

        [Fact]
        public void Dispose_StopsPoller()
        {
            // Act
            _reader.Dispose();

            // Assert
            Assert.True(_reader.StopOperationStatusPollerCalled, "StopOperationStatusPoller should be called on dispose");
            // Note: We can't directly verify Dispose was called on the poller, but disposing the reader should dispose the poller
        }

        // Test implementation of BaseDatabricksReader
        private class TestableDatabricksReader : BaseDatabricksReader
        {
            private RecordBatch? _nextResult;
            private Exception? _nextException;
            public bool StopOperationStatusPollerCalled { get; private set; }

            public TestableDatabricksReader(TestDatabricksStatement statement, Schema schema, DatabricksOperationStatusPoller? poller = null) 
                : base(statement, schema, true)
            {
                if (poller != null)
                {
                    operationStatusPoller = poller;
                }
                StopOperationStatusPollerCalled = false; // Explicit initialization
            }

            public void SetNextResult(RecordBatch? result)
            {
                _nextResult = result;
                _nextException = null;
            }

            public void SetNextException(Exception exception)
            {
                _nextException = exception;
                _nextResult = null;
            }

            protected override ValueTask<RecordBatch?> ReadNextRecordBatchInternalAsync(CancellationToken cancellationToken)
            {
                if (_nextException != null)
                {
                    throw _nextException;
                }
                return new ValueTask<RecordBatch?>(_nextResult);
            }

            protected override void StopOperationStatusPoller()
            {
                StopOperationStatusPollerCalled = true;
                base.StopOperationStatusPoller();
            }

            public override string AssemblyName => "TestAssembly";
            public override string AssemblyVersion => "1.0.0";
        }

        // Test implementation of DatabricksStatement that doesn't require a real connection
        private class TestDatabricksStatement : DatabricksStatement
        {
            private readonly TCLIService.IAsync _mockClient;

            // Create a minimal mock connection that satisfies the constructor
            private static TestDatabricksConnection CreateTestConnection(TCLIService.IAsync mockClient)
            {
                var properties = new Dictionary<string, string>
                {
                    [SparkParameters.HostName] = "test.databricks.com",
                    [SparkParameters.AuthType] = "token",
                    [SparkParameters.Token] = "test-token"
                };
                return new TestDatabricksConnection(properties, mockClient);
            }

            public TestDatabricksStatement(TCLIService.IAsync mockClient) : base(CreateTestConnection(mockClient))
            {
                _mockClient = mockClient;
            }
        }

        // Test implementation of DatabricksConnection
        private class TestDatabricksConnection : DatabricksConnection
        {
            private readonly TCLIService.IAsync _mockClient;

            public TestDatabricksConnection(IReadOnlyDictionary<string, string> properties, TCLIService.IAsync mockClient) : base(properties)
            {
                _mockClient = mockClient;
            }

            protected override TCLIService.IAsync CreateTCLIServiceClient(Thrift.Protocol.TProtocol protocol)
            {
                // Return the injected mock client to avoid real connections
                return _mockClient;
            }
        }
    }
}
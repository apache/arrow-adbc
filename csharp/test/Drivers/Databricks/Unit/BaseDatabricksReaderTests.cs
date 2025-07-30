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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Types;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    public class BaseDatabricksReaderTests
    {
        private readonly Mock<DatabricksStatement> _mockStatement;
        private readonly Mock<DatabricksOperationStatusPoller> _mockPoller;
        private readonly Schema _schema;
        private readonly TestableDatabricksReader _reader;

        public BaseDatabricksReaderTests()
        {
            _mockStatement = new Mock<DatabricksStatement>();
            _mockPoller = new Mock<DatabricksOperationStatusPoller>(_mockStatement.Object, 1);
            _schema = new Schema(new[]
            {
                new Field("test", StringType.Default, false)
            }, new System.Collections.Generic.Dictionary<string, string>());
            
            _reader = new TestableDatabricksReader(_mockStatement.Object, _schema, _mockPoller.Object);
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
            _mockPoller.Verify(p => p.Stop(), Times.Once);
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
            _mockPoller.Verify(p => p.Stop(), Times.Once);
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
            _mockPoller.Verify(p => p.Stop(), Times.Never);
        }

        [Fact]
        public void Dispose_StopsPoller()
        {
            // Act
            _reader.Dispose();

            // Assert
            _mockPoller.Verify(p => p.Stop(), Times.Once);
            _mockPoller.Verify(p => p.Dispose(), Times.Once);
        }

        // Test implementation of BaseDatabricksReader
        private class TestableDatabricksReader : BaseDatabricksReader
        {
            private RecordBatch? _nextResult;
            private Exception? _nextException;

            public TestableDatabricksReader(DatabricksStatement statement, Schema schema, DatabricksOperationStatusPoller? poller = null) 
                : base(statement, schema, true)
            {
                if (poller != null)
                {
                    operationStatusPoller = poller;
                }
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

            public override string AssemblyName => "TestAssembly";
            public override string AssemblyVersion => "1.0.0";
        }
    }
}
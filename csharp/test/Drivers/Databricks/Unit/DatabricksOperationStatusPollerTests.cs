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
using Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Hive.Service.Rpc.Thrift;
using Moq;
using System.Collections.Generic;
using Xunit;
using Xunit.Abstractions;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    public class DatabricksOperationStatusPollerTests
    {
        private readonly ITestOutputHelper _outputHelper;
        private readonly Mock<IHiveServer2Statement> _mockStatement;
        private readonly Mock<TCLIService.IAsync> _mockClient;
        private readonly TOperationHandle _operationHandle;

        private readonly int _heartbeatIntervalSeconds = 1;

        public DatabricksOperationStatusPollerTests(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
            _mockClient = new Mock<TCLIService.IAsync>();
            _mockStatement = new Mock<IHiveServer2Statement>();
            _operationHandle = new TOperationHandle
            {
                OperationId = new THandleIdentifier { Guid = new byte[] { 1, 2, 3, 4 } },
                OperationType = TOperationType.EXECUTE_STATEMENT
            };

            _mockStatement.Setup(s => s.Client).Returns(_mockClient.Object);
            _mockStatement.Setup(s => s.OperationHandle).Returns(_operationHandle);
        }

        [Fact]
        public async Task StartPollsOperationStatusAtInterval()
        {
            // Arrange
            var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait for 3 seconds to allow multiple polls

            // Assert
            Assert.True(pollCount > 0, "Should have polled at least once");
            _mockClient.Verify(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }

        [Fact]
        public async Task DisposeStopsPolling()
        {
            // Arrange
            var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp())
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Let it poll for a bit
            poller.Dispose();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait to see if it continues polling

            // Assert
            int finalPollCount = pollCount;
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait another second
            Assert.Equal(finalPollCount, pollCount); // Poll count should not increase after disposal
        }

        [Fact]
        public async Task StopsPollingOnAllTerminalOperationStates()
        {
            var terminalStates = new[]
            {
                TOperationState.CANCELED_STATE,
                TOperationState.ERROR_STATE,
                TOperationState.CLOSED_STATE,
                TOperationState.TIMEDOUT_STATE,
                TOperationState.UKNOWN_STATE
            };

            foreach (var terminalState in terminalStates)
            {
                // Arrange
                var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _heartbeatIntervalSeconds);
                var pollCount = 0;
                _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new TGetOperationStatusResp { OperationState = terminalState })
                    .Callback(() => pollCount++);

                // Act
                poller.Start();
                await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait longer than heartbeat interval

                // Assert
                Assert.Equal(1, pollCount);

                poller.Dispose();
            }
        }

        [Fact]
        public async Task ContinuesPollingOnRunningState()
        {
            // Arrange
            var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TGetOperationStatusResp { OperationState = TOperationState.RUNNING_STATE })
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait longer than heartbeat interval

            // Assert
            // Should continue polling when in running state
            Assert.True(pollCount > 1, $"Expected multiple polls but got {pollCount}");
            poller.Dispose();
        }

        [Fact]
        public async Task StopsPollingOnException()
        {
            // Arrange
            var poller = new DatabricksOperationStatusPoller(_mockStatement.Object, _heartbeatIntervalSeconds);
            var pollCount = 0;
            _mockClient.Setup(c => c.GetOperationStatus(It.IsAny<TGetOperationStatusReq>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Test exception"))
                .Callback(() => pollCount++);

            // Act
            poller.Start();
            await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds * 3)); // Wait longer than heartbeat interval

            // Assert
            // Should stop polling after the exception
            Assert.Equal(1, pollCount);
            try
            {
                poller.Dispose();
            }
            catch (Exception)
            {

            }
        }
    }
}

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
using Apache.Arrow.Adbc.Drivers.Apache.Databricks.CloudFetch;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Service that periodically polls the operation status of a Databricks warehouse query to keep it alive.
    /// This is used to maintain the command results and session when reading results takes a long time.
    /// </summary>
    internal class DatabricksOperationStatusPoller : IDisposable
    {
        private readonly IHiveServer2Statement _statement;
        private readonly int _heartbeatIntervalSeconds;
        private readonly CancellationTokenSource _cts;
        private Task? _operationStatusPollingTask;
        private bool _isDisposed;

        public DatabricksOperationStatusPoller(IHiveServer2Statement statement, int heartbeatIntervalSeconds = DatabricksConstants.DefaultOperationStatusPollingIntervalSeconds)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _heartbeatIntervalSeconds = heartbeatIntervalSeconds;
            _cts = new CancellationTokenSource();
        }

        public void Start()
        {
            if (_heartbeatIntervalSeconds <= 0) return;
            if (_operationStatusPollingTask == null)
            {
                _operationStatusPollingTask = Task.Run(() => PollOperationStatus(_cts.Token));
            } else {
                throw new InvalidOperationException("Operation status poller already started");
            }
        }

        private async Task PollOperationStatus(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var operationHandle = _statement.OperationHandle;
                if (operationHandle == null) break;

                var request = new TGetOperationStatusReq(operationHandle);
                var response = await _statement.Client.GetOperationStatus(request, cancellationToken);

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_heartbeatIntervalSeconds), cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break;
                }

                // end the heartbeat if the command has terminated
                if (response.OperationState == TOperationState.CANCELED_STATE ||
                    response.OperationState == TOperationState.ERROR_STATE)
                {
                    break;
                }
            }
        }

        public void Dispose()
        {
            if (_isDisposed) return;

            _cts.Cancel();
            _operationStatusPollingTask?.Wait();
            _cts.Dispose();
            _isDisposed = true;
        }
    }
}

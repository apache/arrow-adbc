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
    /// Service that sends periodic heartbeats to the Databricks warehouse to keep it alive. 
    /// Specifically, this is to keep the command results and session alive, if reading results takes a long time.
    /// </summary>
    internal class DatabricksHeartbeatService : IDisposable
    {
        private readonly IHiveServer2Statement _statement;
        private readonly int _heartbeatIntervalSeconds;
        private readonly CancellationTokenSource _cts;
        private Task? _heartbeatTask;
        private bool _isDisposed;

        public DatabricksHeartbeatService(IHiveServer2Statement statement, int intervalSeconds)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _heartbeatIntervalSeconds = intervalSeconds;
            _cts = new CancellationTokenSource();
        }

        public void Start()
        {
            if (_heartbeatIntervalSeconds <= 0) return;
            _heartbeatTask = Task.Run(() => PollOperationStatus(_cts.Token));
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
            }
        }

        public void Dispose()
        {
            if (_isDisposed) return;

            _cts.Cancel();
            _heartbeatTask?.Wait();
            _cts.Dispose();
            _isDisposed = true;
        }
    }
}
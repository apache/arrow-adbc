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
    /// Service that sends periodic heartbeats to the Databricks cluster/warehouse to keep it alive.
    /// </summary>
    internal class DatabricksHeartbeatService : IDisposable
    {
        private readonly IHiveServer2Statement _statement;
        private readonly int _heartbeatIntervalSeconds;
        private Timer? _heartbeatTimer;
        private CancellationTokenSource _cancellationTokenSource;
        private bool _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabricksHeartbeatService"/> class.
        /// </summary>
        /// <param name="statement">The statement to use for sending heartbeats.</param>
        public DatabricksHeartbeatService(IHiveServer2Statement statement)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _heartbeatIntervalSeconds = DatabricksConstants.DefaultHeartbeatIntervalSeconds;
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public void Start()
        {
            if (_heartbeatIntervalSeconds <= 0)
            {
                return; // Heartbeats are disabled
            }

            _heartbeatTimer = new Timer(
                async _ => await SendHeartbeatAsync(),
                null,
                TimeSpan.FromSeconds(_heartbeatIntervalSeconds),
                TimeSpan.FromSeconds(_heartbeatIntervalSeconds));
        }

        public void Stop()
        {
            if (_heartbeatTimer != null)
            {
                _heartbeatTimer.Dispose();
                _heartbeatTimer = null;
            }

            _cancellationTokenSource.Cancel();
        }

        private async Task SendHeartbeatAsync()
        {
            try
            {
                if (_isDisposed || _cancellationTokenSource.IsCancellationRequested)
                {
                    return;
                }

                var operationHandle = _statement.OperationHandle;
                if (operationHandle == null)
                {
                    return;
                }

                var request = new TGetOperationStatusReq(operationHandle);
                await _statement.Client.GetOperationStatus(request, _cancellationTokenSource.Token);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Heartbeat error: {ex.Message}");
            }
        }

        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }

            Stop();
            _cancellationTokenSource.Dispose();
            _isDisposed = true;
        }
    }
}
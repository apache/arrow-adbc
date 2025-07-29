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
using System.Globalization;
using System.Reflection;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using System.Text.Json;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class TelemetryHelper
    {
        private List<TelemetryFrontendLog> _eventsBatch;
        private readonly object _eventsBatchLock;
        private long _lastFlushTimeMillis;
        private readonly Timer _flushTimer;

        private TelemetryClient? _telemetryClient;

        private ClientContext? _clientContext;
        private string? _accessToken;
        private string? _hostUrl;
        private DriverConnectionParameters? _connectionParameters;
        private readonly DriverSystemConfiguration _systemConfiguration;

        public TelemetryHelper(string? hostUrl, string? accessToken)
        {
            _hostUrl = hostUrl;
            _accessToken = accessToken;
            _lastFlushTimeMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _flushTimer = new Timer(TimerFlushEvents, null, DatabricksConnectionConfig.FLUSH_INTERVAL_MILLIS, DatabricksConnectionConfig.FLUSH_INTERVAL_MILLIS);
            _eventsBatch = new List<TelemetryFrontendLog>();
            _eventsBatchLock = new object();
            _systemConfiguration = new DriverSystemConfiguration()
            {
                DriverVersion = Util.GetDriverVersion(),
                DriverName = Util.GetDriverName(),
                OsName = Environment.OSVersion.Platform.ToString(),
                OsVersion = Environment.OSVersion.Version.ToString(),
                OsArch = Environment.OSVersion.Platform.ToString(),
                RuntimeName = Assembly.GetExecutingAssembly().GetName().Name,
                RuntimeVersion = Assembly.GetExecutingAssembly().GetName().Version?.ToString(),
                RuntimeVendor = RuntimeInformation.FrameworkDescription,
                ClientAppName = null,
                LocaleName = CultureInfo.CurrentCulture.Name,
                ProcessName = Process.GetCurrentProcess().ProcessName
            };
        }

        public void SetParameters(DriverConnectionParameters connectionParameters, ClientContext clientContext)
        {
            _connectionParameters = connectionParameters;
            _clientContext = clientContext;
        }

        public void InitializeTelemetryClient(HttpClient httpClient)
        {
            if (_telemetryClient == null && _connectionParameters?.HostInfo != null)
            {
                _telemetryClient = new TelemetryClient(httpClient, _connectionParameters.HostInfo.HostUrl, _accessToken);
            }
        }

        public void AddSqlExecutionEvent(SqlExecutionEvent sqlExecutionEvent, long? latencyMs)
        {
            var telemetryEvent = new TelemetryEvent();
            var telemetryFrontendLog = new TelemetryFrontendLog();
            var frontendLogEntry = new FrontendLogEntry();
            var frontendLogContext = new FrontendLogContext();

            telemetryEvent.DriverConnectionParameters = _connectionParameters;
            telemetryEvent.SystemConfiguration = _systemConfiguration;
            telemetryEvent.SqlExecutionEvent = sqlExecutionEvent;
            telemetryEvent.LatencyMs = latencyMs;
            frontendLogContext.ClientContext = _clientContext;
            frontendLogEntry.SqlDriverLog = telemetryEvent;
            telemetryFrontendLog.Entry = frontendLogEntry;
            telemetryFrontendLog.Context = frontendLogContext;

            lock (_eventsBatchLock)
            {
                _eventsBatch.Add(telemetryFrontendLog);
                if (_eventsBatch.Count >= DatabricksConnectionConfig.MAX_BATCH_SIZE)
                {
                    Task.Run(() => FlushEvents());
                }
            };
        }

        private void TimerFlushEvents(object? state)
        {
            var currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (currentTime - _lastFlushTimeMillis >= DatabricksConnectionConfig.FLUSH_INTERVAL_MILLIS)
            {
                Task.Run(() => FlushEvents());
            }
        }

        private async Task FlushEvents()
        {
            if (_telemetryClient == null)
            {
                return;
            }

            List<TelemetryFrontendLog>? eventsToFlush = null;

            lock (_eventsBatchLock)
            {
                if (_eventsBatch.Count == 0)
                {
                    return;
                }

                eventsToFlush = new List<TelemetryFrontendLog>(_eventsBatch);
                _eventsBatch.Clear();
                _lastFlushTimeMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }

            if (eventsToFlush != null && eventsToFlush.Count > 0)
            {
                try
                {
                    var success = await _telemetryClient.SendTelemetryBatchAsync(eventsToFlush);
                    
                    if (!success)
                    {
                        System.Diagnostics.Debug.WriteLine("Failed to send telemetry batch");
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Exception while flushing telemetry events: {ex.Message}");
                }
            }
        }

        public async Task ForceFlushAsync()
        {
            await FlushEvents();
        }

        public void Dispose()
        {
            _flushTimer?.Dispose();
            
            Task.Run(async () => await FlushEvents()).Wait(TimeSpan.FromSeconds(5));
        }
    }
}
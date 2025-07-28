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
        private static List<TelemetryFrontendLog> _eventsBatch = new List<TelemetryFrontendLog>();
        private static readonly object _eventsBatchLock = new object();
        private static long _lastFlushTimeMillis;
        private static readonly Timer _flushTimer;

        private static TelemetryClient? _telemetryClient;
        private static DatabricksActivityListener? _activityListener;

        private static ClientContext? _clientContext;
        private static string? _accessToken;
        private static DriverConnectionParameters? _connectionParameters;
        private static readonly DriverSystemConfiguration _systemConfiguration = new DriverSystemConfiguration()
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

        static TelemetryHelper()
        {
            _lastFlushTimeMillis = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            _flushTimer = new Timer(TimerFlushEvents, null, DatabricksConnectionConfig.FLUSH_INTERVAL_MILLIS, DatabricksConnectionConfig.FLUSH_INTERVAL_MILLIS);
            _activityListener = new DatabricksActivityListener();

        }

        public static void SetParameters(DriverConnectionParameters connectionParameters, ClientContext clientContext, string? accessToken)
        {
            _connectionParameters = connectionParameters;
            _clientContext = clientContext;
            _accessToken = accessToken;
        }

        public static void InitializeTelemetryClient(HttpClient httpClient)
        {
            if (_telemetryClient == null && _connectionParameters?.HostInfo != null)
            {
                _telemetryClient = new TelemetryClient(httpClient, _connectionParameters.HostInfo.HostUrl, _accessToken);
            }
        }

        public static void AddSqlExecutionEvent(SqlExecutionEvent sqlExecutionEvent)
        {
            var telemetryEvent = new TelemetryEvent();
            var telemetryFrontendLog = new TelemetryFrontendLog();
            var frontendLogEntry = new FrontendLogEntry();
            var frontendLogContext = new FrontendLogContext();

            telemetryEvent.DriverConnectionParameters = _connectionParameters;
            telemetryEvent.SystemConfiguration = _systemConfiguration;
            telemetryEvent.SqlExecutionEvent = sqlExecutionEvent;
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

        private static void TimerFlushEvents(object? state)
        {
            var currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (currentTime - _lastFlushTimeMillis >= DatabricksConnectionConfig.FLUSH_INTERVAL_MILLIS)
            {
                Task.Run(() => FlushEvents());
            }
        }

        private static async Task FlushEvents()
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

        public static async Task ForceFlushAsync()
        {
            await FlushEvents();
        }

        public static void Dispose()
        {
            _flushTimer?.Dispose();
            _activityListener?.Dispose();

            Task.Run(async () => await FlushEvents()).Wait(TimeSpan.FromSeconds(5));
        }
    }
}
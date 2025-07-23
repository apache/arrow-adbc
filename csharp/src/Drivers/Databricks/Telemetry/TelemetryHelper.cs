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
using System.Globalization;
using System.Reflection;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using System.Text.Json;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class TelemetryHelper
    {
        private static ConcurrentDictionary<string, DriverConnectionParameters> _connectionParameters = new ConcurrentDictionary<string, DriverConnectionParameters>();
        private static HttpClient _httpClient = new HttpClient();
        private static DriverSystemConfiguration _systemConfiguration = new DriverSystemConfiguration()
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

        public static AuthMech StringToAuthMech(String authType)
        {
            switch (authType)
            {
                case "none":
                    return AuthMech.OTHER;
                case "basic":
                    return AuthMech.OTHER;
                case "token":
                    return AuthMech.PAT;
                case "oauth":
                    return AuthMech.OAUTH;
                default:
                    return AuthMech.TYPE_UNSPECIFIED;
            }
        }

        public static string ExportTelemetry( DriverConnectionParameters connectionParameters, ClientContext clientContext)
        {
            var telemetryEvent = new TelemetryEvent();
            var telemetryFrontendLog = new TelemetryFrontendLog();
            var frontendLogEntry = new FrontendLogEntry();
            var frontendLogContext = new FrontendLogContext();
            telemetryEvent.DriverConnectionParameters = connectionParameters;
            telemetryEvent.SystemConfiguration = _systemConfiguration;
            frontendLogContext.ClientContext = clientContext;
            frontendLogEntry.SqlDriverLog = telemetryEvent;
            telemetryFrontendLog.Entry = frontendLogEntry;
            telemetryFrontendLog.Context = frontendLogContext;

            var request = new HttpRequestMessage(HttpMethod.Post, $"https://{connectionParameters.HostInfo?.HostUrl ?? ""}/telemetry");
            var requestHeader = 
            request.Content = new StringContent(JsonSerializer.Serialize(telemetryFrontendLog));
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            var response = _httpClient.SendAsync(request);
            return response.Result.StatusCode.ToString();
        }
    }
}
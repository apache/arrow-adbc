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
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using System.IO;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class TelemetryClient
    {
        private readonly HttpClient _httpClient;
        private readonly string? _telemetryUrl;

        //logging to file
        private readonly string _logFilePath = Path.Combine(Path.GetTempPath(), $"databricks/{DateTime.Now:yyyyMMdd-HHmmss}.log");
        private readonly StreamWriter _logWriter;

        public TelemetryClient(string? hostUrl, bool isAuthenticated, HttpMessageHandler httpMessageHandler)
        {
            _httpClient = new HttpClient(httpMessageHandler);
            _telemetryUrl = !string.IsNullOrEmpty(hostUrl) ? isAuthenticated ? $"https://{hostUrl}/telemetry" : $"https://{hostUrl}/telemetry-unauth" : null;
            _logWriter = new StreamWriter(_logFilePath);
        }

        /// <summary>
        /// Sends a batch of telemetry events asynchronously
        /// </summary>
        /// <param name="telemetryBatch">List of telemetry events to send</param>
        /// <returns>Task representing the async operation</returns>
        public async Task<bool> SendTelemetryBatchAsync(List<TelemetryFrontendLog> telemetryBatch)
        {
            if (string.IsNullOrEmpty(_telemetryUrl) || telemetryBatch.Count == 0)
            {
                return false;
            }

            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, _telemetryUrl);
                
                // Serialize the batch to JSON
                var telemetryRequest = new TelemetryRequest();
                telemetryRequest.UploadTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                telemetryRequest.ProtoLogs = telemetryBatch.Select(x => JsonSerializer.Serialize(x)).ToList();
                request.Content = new StringContent(JsonSerializer.Serialize(telemetryRequest).Replace("\\u0022", "\""));
                
                // Set headers
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                var response = await _httpClient.SendAsync(request);
                _logWriter.WriteLine($"Telemetry sent: {response.IsSuccessStatusCode}");
                _logWriter.WriteLine($"Telemetry: {JsonSerializer.Serialize(telemetryRequest).Replace("\\u0022", "\"")}");
                _logWriter.WriteLine($"Telemetry URL: {_telemetryUrl}");
                _logWriter.WriteLine($"Response: {response.Content.ReadAsStringAsync().Result}");
                _logWriter.WriteLine($"Response: {response.RequestMessage}");
                _logWriter.Flush();
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                // Log the exception but don't throw to prevent telemetry failures from affecting main functionality
                System.Diagnostics.Debug.WriteLine($"Failed to send telemetry: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Sends a single telemetry event asynchronously
        /// </summary>
        /// <param name="telemetryEvent">Single telemetry event to send</param>
        /// <returns>Task representing the async operation</returns>
        public async Task<bool> SendTelemetryAsync(TelemetryFrontendLog telemetryEvent)
        {
            if (string.IsNullOrEmpty(_telemetryUrl))
            {
                return false;
            }

            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, _telemetryUrl);
                
                // Serialize the event to JSON
                var telemetryRequest = new TelemetryRequest();
                telemetryRequest.UploadTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                telemetryRequest.ProtoLogs = new List<string> { JsonSerializer.Serialize(telemetryEvent) };
                request.Content = new StringContent(JsonSerializer.Serialize(telemetryRequest).Replace("\\u0022", "\""));
                
                // Set headers
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                var response = await _httpClient.SendAsync(request);
                _logWriter.WriteLine($"Telemetry sent: {response.IsSuccessStatusCode}");
                _logWriter.WriteLine($"Telemetry URL: {_telemetryUrl}");
                _logWriter.WriteLine($"Response: {response.Content}");
                _logWriter.WriteLine($"Response: {response.RequestMessage}");
                _logWriter.Flush();
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                // Log the exception but don't throw to prevent telemetry failures from affecting main functionality
                System.Diagnostics.Debug.WriteLine($"Failed to send telemetry: {ex.Message}");
                return false;
            }
        }
    }
}
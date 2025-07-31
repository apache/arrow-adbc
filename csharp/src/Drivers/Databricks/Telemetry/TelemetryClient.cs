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
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class TelemetryClient
    {
        private readonly HttpClient _httpClient;
        private readonly string? _telemetryUrl;
        private readonly string? _accessToken;

        public TelemetryClient(HttpClient httpClient, string? hostUrl, string? accessToken)
        {
            _httpClient = httpClient;
            _accessToken = accessToken;
            _telemetryUrl = !string.IsNullOrEmpty(hostUrl) ? accessToken != null ? $"https://{hostUrl}/telemetry" : $"https://{hostUrl}/telemetry-unauth" : null;
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
                request.Content = new StringContent(JsonSerializer.Serialize(telemetryRequest));
                
                // Set headers
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                if(_accessToken != null)
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
                }

                var response = await _httpClient.SendAsync(request);
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
                request.Content = new StringContent(JsonSerializer.Serialize(telemetryRequest));
                
                // Set headers
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                if(_accessToken != null)
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
                }

                var response = await _httpClient.SendAsync(request);
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

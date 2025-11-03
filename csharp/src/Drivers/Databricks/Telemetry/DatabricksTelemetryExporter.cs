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
using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    internal sealed class DatabricksTelemetryExporter : ITelemetryExporter
    {
        private const string TelemetryEndpoint = "/telemetry-ext";

        private readonly HttpClient _httpClient;
        private readonly string _hostUrl;

        public DatabricksTelemetryExporter(HttpClient httpClient, string hostUrl)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _hostUrl = !string.IsNullOrEmpty(hostUrl) ? hostUrl : throw new ArgumentException("Host URL cannot be null or empty.", nameof(hostUrl));
        }

        public async Task ExportAsync(
            IReadOnlyList<TelemetryMetric> metrics,
            CancellationToken cancellationToken = default)
        {
            if (metrics == null || metrics.Count == 0)
            {
                return;
            }

            try
            {
                await SendMetricsAsync(metrics, cancellationToken);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Telemetry export failed: {ex.Message}");
            }
        }

        private async Task SendMetricsAsync(
            IReadOnlyList<TelemetryMetric> metrics,
            CancellationToken cancellationToken)
        {
            string fullUrl = new UriBuilder(_hostUrl) { Path = TelemetryEndpoint }.ToString();
            string json = SerializeMetrics(metrics);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var request = new HttpRequestMessage(HttpMethod.Post, fullUrl) { Content = content };
            HttpResponseMessage response = await _httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();
        }

        private string SerializeMetrics(IReadOnlyList<TelemetryMetric> metrics)
        {
            var payload = new { metrics = metrics };
            return JsonSerializer.Serialize(payload, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
        }
    }
}

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
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// HTTP handler that captures the x-thriftserver-error-message header from error responses
    /// and includes it in the exception message for better error visibility.
    /// </summary>
    internal class ThriftErrorMessageHandler : DelegatingHandler
    {
        private const string ThriftServerErrorMessageHeader = "x-thriftserver-error-message";

        /// <summary>
        /// Initializes a new instance of the <see cref="ThriftErrorMessageHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        public ThriftErrorMessageHandler(HttpMessageHandler innerHandler)
            : base(innerHandler)
        {
        }

        /// <summary>
        /// Sends an HTTP request and captures Thrift server error messages from response headers.
        /// </summary>
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            HttpResponseMessage response = await base.SendAsync(request, cancellationToken);

            // Check if the response indicates an error
            if (!response.IsSuccessStatusCode)
            {
                // Try to extract the Thrift server error message header
                if (response.Headers.TryGetValues(ThriftServerErrorMessageHeader, out var headerValues))
                {
                    string thriftErrorMessage = string.Join(", ", headerValues);

                    if (!string.IsNullOrWhiteSpace(thriftErrorMessage))
                    {
                        // Create a custom exception that includes both the HTTP status and the Thrift error message
                        string errorMessage = $"Thrift server error: {thriftErrorMessage} (HTTP {(int)response.StatusCode} {response.ReasonPhrase})";

                        // Dispose the response before throwing
                        response.Dispose();

                        throw new HttpRequestException(errorMessage);
                    }
                }
            }

            return response;
        }
    }
}

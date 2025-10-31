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
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// HTTP handler that implements retry behavior for 408, 502, 503, and 504 responses.
    /// Uses Retry-After header if present, otherwise uses exponential backoff.
    /// </summary>
    internal class RetryHttpHandler : DelegatingHandler
    {
        private readonly int _retryTimeoutSeconds;
        private readonly int _initialBackoffSeconds = 1;
        private readonly int _maxBackoffSeconds = 32;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryHttpHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler to delegate to.</param>
        /// <param name="retryTimeoutSeconds">Maximum total time in seconds to retry before failing.</param>
        public RetryHttpHandler(HttpMessageHandler innerHandler, int retryTimeoutSeconds)
            : base(innerHandler)
        {
            _retryTimeoutSeconds = retryTimeoutSeconds;
        }

        /// <summary>
        /// Sends an HTTP request to the inner handler with retry logic for retryable status codes.
        /// </summary>
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Activity? activity = Activity.Current;

            // Clone the request content if it's not null so we can reuse it for retries
            var requestContentClone = request.Content != null
                ? await CloneHttpContentAsync(request.Content)
                : null;

            HttpResponseMessage response;
            string? lastErrorMessage = null;
            DateTime startTime = DateTime.UtcNow;
            int attemptCount = 0;
            int currentBackoffSeconds = _initialBackoffSeconds;
            int totalRetrySeconds = 0;

            do
            {
                // Set the content for each attempt (if needed)
                if (requestContentClone != null && request.Content == null)
                {
                    request.Content = await CloneHttpContentAsync(requestContentClone);
                }

                response = await base.SendAsync(request, cancellationToken);

                // If it's not a retryable status code, return immediately
                if (!IsRetryableStatusCode(response.StatusCode))
                {
                    // Only log retry summary if retries occurred
                    if (attemptCount > 0)
                    {
                        activity?.SetTag("http.retry.total_attempts", attemptCount);
                        activity?.SetTag("http.response.status_code", (int)response.StatusCode);
                    }
                    return response;
                }

                attemptCount++;

                activity?.SetTag("http.retry.attempt", attemptCount);
                activity?.SetTag("http.response.status_code", (int)response.StatusCode);

                // Check if we've exceeded the timeout
                TimeSpan elapsedTime = DateTime.UtcNow - startTime;
                if (_retryTimeoutSeconds > 0 && elapsedTime.TotalSeconds > _retryTimeoutSeconds)
                {
                    // We've exceeded the timeout, so break out of the loop
                    break;
                }

                int waitSeconds;

                // Check for Retry-After header
                if (response.Headers.TryGetValues("Retry-After", out var retryAfterValues))
                {
                    // Parse the Retry-After value
                    string retryAfterValue = string.Join(",", retryAfterValues);
                    if (int.TryParse(retryAfterValue, out int retryAfterSeconds) && retryAfterSeconds > 0)
                    {
                        // Use the Retry-After value
                        waitSeconds = retryAfterSeconds;
                        lastErrorMessage = $"Service temporarily unavailable (HTTP {(int)response.StatusCode}). Using server-specified retry after {waitSeconds} seconds. Attempt {attemptCount}.";
                    }
                    else
                    {
                        // Invalid Retry-After value, use exponential backoff
                        waitSeconds = CalculateBackoffWithJitter(currentBackoffSeconds);
                        lastErrorMessage = $"Service temporarily unavailable (HTTP {(int)response.StatusCode}). Invalid Retry-After header, using exponential backoff of {waitSeconds} seconds. Attempt {attemptCount}.";
                    }
                }
                else
                {
                    // No Retry-After header, use exponential backoff
                    waitSeconds = CalculateBackoffWithJitter(currentBackoffSeconds);
                    lastErrorMessage = $"Service temporarily unavailable (HTTP {(int)response.StatusCode}). Using exponential backoff of {waitSeconds} seconds. Attempt {attemptCount}.";
                }

                // Dispose the response before retrying
                response.Dispose();

                // Reset the request content for the next attempt
                request.Content = null;

                // Update total retry time
                totalRetrySeconds += waitSeconds;
                if (_retryTimeoutSeconds > 0 && totalRetrySeconds > _retryTimeoutSeconds)
                {
                    // We've exceeded the timeout, so break out of the loop
                    break;
                }

                // Wait for the calculated time
                await Task.Delay(TimeSpan.FromSeconds(waitSeconds), cancellationToken);

                // Increase backoff for next attempt (exponential backoff)
                currentBackoffSeconds = Math.Min(currentBackoffSeconds * 2, _maxBackoffSeconds);
            } while (!cancellationToken.IsCancellationRequested);

            activity?.SetTag("http.retry.total_attempts", attemptCount);
            activity?.SetTag("http.response.status_code", (int)response.StatusCode);

            // If we get here, we've either exceeded the timeout or been cancelled
            if (cancellationToken.IsCancellationRequested)
            {
                activity?.SetTag("http.retry.outcome", "cancelled");
                var cancelEx = new OperationCanceledException("Request cancelled during retry wait", cancellationToken);
                activity?.AddException(cancelEx, [
                    new("error.context", "http.retry.cancelled"),
                    new("attempts", attemptCount)
                ]);
            }

            // Timeout exceeded
            activity?.SetTag("http.retry.outcome", "timeout_exceeded");
            var exception = new DatabricksException(lastErrorMessage ?? "Service temporarily unavailable and retry timeout exceeded", AdbcStatusCode.IOError).SetSqlState("08001");
            activity?.AddException(exception, [
                new("error.context", "http.retry.timeout_exceeded"),
                new("attempts", attemptCount),
                new("total_retry_seconds", totalRetrySeconds),
                new("timeout_seconds", _retryTimeoutSeconds)
            ]);
            throw exception;
        }

        /// <summary>
        /// Determines if the status code is one that should be retried.
        /// </summary>
        private bool IsRetryableStatusCode(HttpStatusCode statusCode)
        {
            return statusCode == HttpStatusCode.RequestTimeout ||      // 408
                   statusCode == HttpStatusCode.BadGateway ||          // 502
                   statusCode == HttpStatusCode.ServiceUnavailable ||  // 503
                   statusCode == HttpStatusCode.GatewayTimeout;        // 504
        }

        /// <summary>
        /// Calculates backoff time with jitter to avoid thundering herd problem.
        /// </summary>
        private int CalculateBackoffWithJitter(int baseBackoffSeconds)
        {
            // Add jitter by randomizing between 80-120% of the base backoff time
            Random random = new Random();
            double jitterFactor = 0.8 + (random.NextDouble() * 0.4); // Between 0.8 and 1.2
            return (int)Math.Max(1, baseBackoffSeconds * jitterFactor);
        }

        /// <summary>
        /// Clones an HttpContent object so it can be reused for retries.
        /// Per .NET guidance, we should not reuse the HTTP content across multiple
        /// requests, as it may be disposed.
        /// </summary>
        private static async Task<HttpContent> CloneHttpContentAsync(HttpContent content)
        {
            var ms = new MemoryStream();
            await content.CopyToAsync(ms);
            ms.Position = 0;

            var clone = new StreamContent(ms);
            if (content.Headers != null)
            {
                foreach (var header in content.Headers)
                {
                    clone.Headers.Add(header.Key, header.Value);
                }
            }
            return clone;
        }
    }
}

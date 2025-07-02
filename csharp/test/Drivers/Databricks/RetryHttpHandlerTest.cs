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

using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// Tests for the RetryHttpHandler class.
    /// </summary>
    public class RetryHttpHandlerTest
    {
        /// <summary>
        /// Tests that the RetryHttpHandler properly processes 503 responses with Retry-After headers.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerProcesses503Response()
        {
            // Create a mock handler that returns a 503 response with a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Headers = { { "Retry-After", "1" } },
                    Content = new StringContent("Service Unavailable")
                });

            // Create the RetryHttpHandler with retry enabled and a 5-second timeout
            var retryHandler = new RetryHttpHandler(mockHandler, 5);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Set the mock handler to return a success response after the first retry
            mockHandler.SetResponseAfterRetryCount(1, new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("Success")
            });

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is OK
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
            Assert.Equal(2, mockHandler.RequestCount); // Initial request + 1 retry
        }

        /// <summary>
        /// Tests that the RetryHttpHandler throws an exception when the retry timeout is exceeded.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerThrowsWhenTimeoutExceeded()
        {
            // Create a mock handler that always returns a 503 response with a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Headers = { { "Retry-After", "2" } },
                    Content = new StringContent("Service Unavailable")
                });

            // Create the RetryHttpHandler with retry enabled and a 1-second timeout
            var retryHandler = new RetryHttpHandler(mockHandler, 1);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request and expect an AdbcException
            var exception = await Assert.ThrowsAsync<DatabricksException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception has the correct SQL state in the message
            Assert.Contains("[SQLState: 08001]", exception.Message);
            Assert.Equal(AdbcStatusCode.IOError, exception.Status);

            // Verify we only tried once (since the Retry-After value of 2 exceeds our timeout of 1)
            Assert.Equal(1, mockHandler.RequestCount);
        }

        /// <summary>
        /// Tests that the RetryHttpHandler handles non-503 responses correctly.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerHandlesNon503Response()
        {
            // Create a mock handler that returns a 404 response
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.NotFound)
                {
                    Content = new StringContent("Not Found")
                });

            // Create the RetryHttpHandler with retry enabled
            var retryHandler = new RetryHttpHandler(mockHandler, 5);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is 404
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("Not Found", await response.Content.ReadAsStringAsync());
            Assert.Equal(1, mockHandler.RequestCount); // Only the initial request, no retries
        }

        /// <summary>
        /// Tests that the RetryHttpHandler handles 503 responses without Retry-After headers correctly.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerHandles503WithoutRetryAfterHeader()
        {
            // Create a mock handler that returns a 503 response without a Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = new StringContent("Service Unavailable")
                });

            // Create the RetryHttpHandler with retry enabled
            var retryHandler = new RetryHttpHandler(mockHandler, 5);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is 503
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            Assert.Equal("Service Unavailable", await response.Content.ReadAsStringAsync());
            Assert.Equal(1, mockHandler.RequestCount); // Only the initial request, no retries
        }

        /// <summary>
        /// Tests that the RetryHttpHandler handles invalid Retry-After headers correctly.
        /// </summary>
        [Fact]
        public async Task RetryAfterHandlerHandlesInvalidRetryAfterHeader()
        {
            // Create a mock handler that returns a 503 response with an invalid Retry-After header
            var mockHandler = new MockHttpMessageHandler(
                new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                {
                    Content = new StringContent("Service Unavailable")
                });

            // Add the invalid Retry-After header directly in the test
            var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            {
                Content = new StringContent("Service Unavailable")
            };
            response.Headers.TryAddWithoutValidation("Retry-After", "invalid");
            mockHandler.SetResponseAfterRetryCount(0, response);

            // Create the RetryHttpHandler with retry enabled
            var retryHandler = new RetryHttpHandler(mockHandler, 5);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(retryHandler);

            // Send a request
            response = await httpClient.GetAsync("http://test.com");

            // Verify the response is 503
            Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
            Assert.Equal("Service Unavailable", await response.Content.ReadAsStringAsync());
            Assert.Equal(1, mockHandler.RequestCount); // Only the initial request, no retries
        }

        /// <summary>
        /// Mock HttpMessageHandler for testing the RetryHttpHandler.
        /// </summary>
        private class MockHttpMessageHandler : HttpMessageHandler
        {
            private readonly HttpResponseMessage _defaultResponse;
            private HttpResponseMessage? _responseAfterRetryCount;
            private int _retryCountForResponse;

            public int RequestCount { get; private set; }

            public MockHttpMessageHandler(HttpResponseMessage defaultResponse)
            {
                _defaultResponse = defaultResponse;
            }

            public void SetResponseAfterRetryCount(int retryCount, HttpResponseMessage response)
            {
                _retryCountForResponse = retryCount;
                _responseAfterRetryCount = response;
            }

            protected override Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                RequestCount++;

                if (_responseAfterRetryCount != null && RequestCount > _retryCountForResponse)
                {
                    return Task.FromResult(_responseAfterRetryCount);
                }

                // Create a new response instance to avoid modifying the original
                var response = new HttpResponseMessage
                {
                    StatusCode = _defaultResponse.StatusCode,
                    Content = _defaultResponse.Content
                };

                // Copy headers only if they exist
                if (_defaultResponse.Headers.Contains("Retry-After"))
                {
                    foreach (var value in _defaultResponse.Headers.GetValues("Retry-After"))
                    {
                        response.Headers.Add("Retry-After", value);
                    }
                }

                return Task.FromResult(response);
            }
        }
    }
}

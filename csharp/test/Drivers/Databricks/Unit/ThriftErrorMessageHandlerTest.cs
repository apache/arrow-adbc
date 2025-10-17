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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    /// <summary>
    /// Tests for the ThriftErrorMessageHandler class.
    ///
    /// IMPORTANT: These tests verify Thrift error message extraction in isolation. In production,
    /// ThriftErrorMessageHandler must be positioned OUTSIDE (farther from network) RetryHttpHandler
    /// in the handler chain so that retryable errors (408, 502, 503, 504) are retried before
    /// exceptions are thrown. See DatabricksConnection.CreateHttpHandler() for the correct handler
    /// chain ordering and detailed explanation.
    /// </summary>
    public class ThriftErrorMessageHandlerTest
    {
        /// <summary>
        /// Tests that the handler captures x-thriftserver-error-message header from 401 responses.
        /// </summary>
        [Fact]
        public async Task HandlerCapturesThriftErrorMessageFrom401Response()
        {
            // Create a mock handler that returns a 401 response with x-thriftserver-error-message header
            var mockHandler = new MockHttpMessageHandler(
                HttpStatusCode.Unauthorized,
                "Invalid personal access token");

            // Create the ThriftErrorMessageHandler
            var thriftHandler = new ThriftErrorMessageHandler(mockHandler);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(thriftHandler);

            // Send a request and expect an HttpRequestException
            var exception = await Assert.ThrowsAsync<HttpRequestException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception message includes the Thrift error message
            Assert.Contains("Thrift server error: Invalid personal access token", exception.Message);
            Assert.Contains("HTTP 401", exception.Message);
            Assert.Contains("Unauthorized", exception.Message);
        }

        /// <summary>
        /// Tests that the handler captures x-thriftserver-error-message header from 403 responses.
        /// </summary>
        [Fact]
        public async Task HandlerCapturesThriftErrorMessageFrom403Response()
        {
            // Create a mock handler that returns a 403 response with x-thriftserver-error-message header
            var mockHandler = new MockHttpMessageHandler(
                HttpStatusCode.Forbidden,
                "User does not have permission to access catalog 'main'");

            // Create the ThriftErrorMessageHandler
            var thriftHandler = new ThriftErrorMessageHandler(mockHandler);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(thriftHandler);

            // Send a request and expect an HttpRequestException
            var exception = await Assert.ThrowsAsync<HttpRequestException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception message includes the Thrift error message
            Assert.Contains("Thrift server error: User does not have permission to access catalog 'main'", exception.Message);
            Assert.Contains("HTTP 403", exception.Message);
            Assert.Contains("Forbidden", exception.Message);
        }

        /// <summary>
        /// Tests that the handler passes through success responses unchanged.
        /// </summary>
        [Fact]
        public async Task HandlerPassesThroughSuccessResponses()
        {
            // Create a mock handler that returns a 200 OK response
            var mockHandler = new MockHttpMessageHandler(HttpStatusCode.OK, null);

            // Create the ThriftErrorMessageHandler
            var thriftHandler = new ThriftErrorMessageHandler(mockHandler);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(thriftHandler);

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is successful and passes through unchanged
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("Success", await response.Content.ReadAsStringAsync());
        }

        /// <summary>
        /// Tests that the handler passes through error responses without x-thriftserver-error-message header.
        /// </summary>
        [Fact]
        public async Task HandlerPassesThroughErrorResponsesWithoutThriftHeader()
        {
            // Create a mock handler that returns a 404 response without x-thriftserver-error-message header
            var mockHandler = new MockHttpMessageHandler(HttpStatusCode.NotFound, null);

            // Create the ThriftErrorMessageHandler
            var thriftHandler = new ThriftErrorMessageHandler(mockHandler);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(thriftHandler);

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is returned as-is (not thrown as exception)
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("Not Found", await response.Content.ReadAsStringAsync());
        }

        /// <summary>
        /// Tests that the handler ignores empty x-thriftserver-error-message headers.
        /// </summary>
        [Fact]
        public async Task HandlerIgnoresEmptyThriftErrorMessage()
        {
            // Create a mock handler that returns a 401 response with empty x-thriftserver-error-message header
            var mockHandler = new MockHttpMessageHandler(HttpStatusCode.Unauthorized, "");

            // Create the ThriftErrorMessageHandler
            var thriftHandler = new ThriftErrorMessageHandler(mockHandler);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(thriftHandler);

            // Send a request
            var response = await httpClient.GetAsync("http://test.com");

            // Verify the response is returned as-is (empty header is ignored)
            Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        }

        /// <summary>
        /// Tests that the handler captures Thrift error messages from various HTTP error codes.
        /// </summary>
        [Theory]
        [InlineData(HttpStatusCode.BadRequest, "Invalid request parameter: 'limit' must be positive", "Bad Request")]
        [InlineData(HttpStatusCode.Unauthorized, "Token has expired", "Unauthorized")]
        [InlineData(HttpStatusCode.Forbidden, "Insufficient permissions", "Forbidden")]
        [InlineData(HttpStatusCode.InternalServerError, "Internal server error occurred", "Internal Server Error")]
        [InlineData(HttpStatusCode.ServiceUnavailable, "Service is temporarily unavailable", "Service Unavailable")]
        public async Task HandlerCapturesThriftErrorMessagesFromVariousStatusCodes(
            HttpStatusCode statusCode,
            string thriftErrorMessage,
            string reasonPhrase)
        {
            // Create a mock handler that returns the specified status code with x-thriftserver-error-message header
            var mockHandler = new MockHttpMessageHandler(statusCode, thriftErrorMessage);

            // Create the ThriftErrorMessageHandler
            var thriftHandler = new ThriftErrorMessageHandler(mockHandler);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(thriftHandler);

            // Send a request and expect an HttpRequestException
            var exception = await Assert.ThrowsAsync<HttpRequestException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception message includes the Thrift error message
            Assert.Contains($"Thrift server error: {thriftErrorMessage}", exception.Message);
            Assert.Contains($"HTTP {(int)statusCode}", exception.Message);
            Assert.Contains(reasonPhrase, exception.Message);
        }

        /// <summary>
        /// Tests that the handler handles multiple values in x-thriftserver-error-message header.
        /// </summary>
        [Fact]
        public async Task HandlerHandlesMultipleThriftErrorMessageValues()
        {
            // Create a mock handler that returns a 401 response with multiple x-thriftserver-error-message header values
            var mockHandler = new MockHttpMessageHandlerMultipleHeaders(
                HttpStatusCode.Unauthorized,
                new[] { "Error 1", "Error 2" });

            // Create the ThriftErrorMessageHandler
            var thriftHandler = new ThriftErrorMessageHandler(mockHandler);

            // Create an HttpClient with our handler
            var httpClient = new HttpClient(thriftHandler);

            // Send a request and expect an HttpRequestException
            var exception = await Assert.ThrowsAsync<HttpRequestException>(async () =>
                await httpClient.GetAsync("http://test.com"));

            // Verify the exception message includes both error messages joined
            Assert.Contains("Thrift server error: Error 1, Error 2", exception.Message);
        }

        /// <summary>
        /// Mock HttpMessageHandler for testing the ThriftErrorMessageHandler.
        /// </summary>
        private class MockHttpMessageHandler : HttpMessageHandler
        {
            private readonly HttpStatusCode _statusCode;
            private readonly string? _thriftErrorMessage;

            public MockHttpMessageHandler(HttpStatusCode statusCode, string? thriftErrorMessage)
            {
                _statusCode = statusCode;
                _thriftErrorMessage = thriftErrorMessage;
            }

            protected override Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                var response = new HttpResponseMessage(_statusCode);

                if (_statusCode == HttpStatusCode.OK)
                {
                    response.Content = new StringContent("Success");
                }
                else if (_statusCode == HttpStatusCode.NotFound)
                {
                    response.Content = new StringContent("Not Found");
                }
                else
                {
                    response.Content = new StringContent($"Error: {_statusCode}");
                }

                // Add x-thriftserver-error-message header if provided
                if (!string.IsNullOrEmpty(_thriftErrorMessage))
                {
                    response.Headers.TryAddWithoutValidation("x-thriftserver-error-message", _thriftErrorMessage);
                }
                else if (_thriftErrorMessage == "")
                {
                    // Explicitly add empty header for testing empty header scenario
                    response.Headers.TryAddWithoutValidation("x-thriftserver-error-message", "");
                }

                return Task.FromResult(response);
            }
        }

        /// <summary>
        /// Mock HttpMessageHandler that supports multiple header values for testing.
        /// </summary>
        private class MockHttpMessageHandlerMultipleHeaders : HttpMessageHandler
        {
            private readonly HttpStatusCode _statusCode;
            private readonly string[] _thriftErrorMessages;

            public MockHttpMessageHandlerMultipleHeaders(HttpStatusCode statusCode, string[] thriftErrorMessages)
            {
                _statusCode = statusCode;
                _thriftErrorMessages = thriftErrorMessages;
            }

            protected override Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                var response = new HttpResponseMessage(_statusCode)
                {
                    Content = new StringContent($"Error: {_statusCode}")
                };

                // Add multiple x-thriftserver-error-message header values
                foreach (var errorMessage in _thriftErrorMessages)
                {
                    response.Headers.TryAddWithoutValidation("x-thriftserver-error-message", errorMessage);
                }

                return Task.FromResult(response);
            }
        }
    }
}

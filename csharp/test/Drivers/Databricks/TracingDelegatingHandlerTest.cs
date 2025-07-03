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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using Xunit;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Tests
{
    /// <summary>
    /// Tests for the TracingDelegatingHandler class.
    /// </summary>
    public class TracingDelegatingHandlerTest
    {
        /// <summary>
        /// Mock HTTP message handler for testing.
        /// </summary>
        private class MockHttpMessageHandler : HttpMessageHandler
        {
            public HttpRequestMessage? CapturedRequest { get; private set; }
            public HttpResponseMessage ResponseToReturn { get; set; } = new HttpResponseMessage(HttpStatusCode.OK);

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                CapturedRequest = request;
                return Task.FromResult(ResponseToReturn);
            }
        }

        /// <summary>
        /// Mock activity tracer for testing.
        /// </summary>
        private class MockActivityTracer : IActivityTracer
        {
            public string? TraceParent { get; set; }
            public ActivityTrace Trace => new ActivityTrace("TestSource", "1.0.0", TraceParent);
            public string AssemblyVersion => "1.0.0";
            public string AssemblyName => "TestAssembly";
        }

        [Fact]
        public async Task SendAsync_WithCurrentActivity_AddsTraceParentHeader()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var mockTracer = new MockActivityTracer();
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer);
            var httpClient = new HttpClient(tracingHandler);

            // Create an activity with a listener to ensure it's created
            Activity.DefaultIdFormat = ActivityIdFormat.W3C;
            ActivitySource.AddActivityListener(new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            });

            using var activitySource = new ActivitySource("TestActivitySource");
            using var activity = activitySource.StartActivity("TestActivity", ActivityKind.Client);

            // Skip test if activity creation is not supported in this environment
            if (activity == null)
            {
                // Test with trace parent from tracer instead
                var fallbackTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
                mockTracer.TraceParent = fallbackTraceParent;

                var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
                await httpClient.SendAsync(request);

                Assert.NotNull(mockHandler.CapturedRequest);
                Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
                var traceParentValue = mockHandler.CapturedRequest.Headers.GetValues("traceparent").FirstOrDefault();
                Assert.Equal(fallbackTraceParent, traceParentValue);
                return;
            }

            var expectedTraceParent = activity.Id;

            // Act
            var request2 = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
            await httpClient.SendAsync(request2);

            // Assert
            Assert.NotNull(mockHandler.CapturedRequest);
            Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
            var actualTraceParent = mockHandler.CapturedRequest.Headers.GetValues("traceparent").FirstOrDefault();
            Assert.Equal(expectedTraceParent, actualTraceParent);
        }

        [Fact]
        public async Task SendAsync_WithTracerTraceParent_AddsTraceParentHeader()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var expectedTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
            var mockTracer = new MockActivityTracer { TraceParent = expectedTraceParent };
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer);
            var httpClient = new HttpClient(tracingHandler);

            // Act
            var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
            await httpClient.SendAsync(request);

            // Assert
            Assert.NotNull(mockHandler.CapturedRequest);
            Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
            var traceParentValue = mockHandler.CapturedRequest.Headers.GetValues("traceparent").FirstOrDefault();
            Assert.Equal(expectedTraceParent, traceParentValue);
        }

        [Fact]
        public async Task SendAsync_WithoutTraceContext_DoesNotAddHeader()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var mockTracer = new MockActivityTracer { TraceParent = null };
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer);
            var httpClient = new HttpClient(tracingHandler);

            // Act
            var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
            await httpClient.SendAsync(request);

            // Assert
            Assert.NotNull(mockHandler.CapturedRequest);
            Assert.False(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
        }

        [Fact]
        public async Task SendAsync_CurrentActivityTakesPrecedenceOverTracer()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var tracerTraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
            var mockTracer = new MockActivityTracer { TraceParent = tracerTraceParent };
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer);
            var httpClient = new HttpClient(tracingHandler);

            // Create an activity with a listener to ensure it's created
            Activity.DefaultIdFormat = ActivityIdFormat.W3C;
            ActivitySource.AddActivityListener(new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            });

            using var activitySource = new ActivitySource("TestActivitySource");
            using var activity = activitySource.StartActivity("TestActivity", ActivityKind.Client);

            // Skip test if activity creation is not supported in this environment
            if (activity == null)
            {
                // Can't test precedence without an activity - just verify tracer parent works
                var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
                await httpClient.SendAsync(request);

                Assert.NotNull(mockHandler.CapturedRequest);
                Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
                var traceParentValue = mockHandler.CapturedRequest.Headers.GetValues("traceparent").FirstOrDefault();
                Assert.Equal(tracerTraceParent, traceParentValue);
                return;
            }

            var expectedTraceParent = activity.Id;

            // Act
            var request2 = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
            await httpClient.SendAsync(request2);

            // Assert
            Assert.NotNull(mockHandler.CapturedRequest);
            Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
            var actualTraceParent = mockHandler.CapturedRequest.Headers.GetValues("traceparent").FirstOrDefault();
            Assert.Equal(expectedTraceParent, actualTraceParent);
            Assert.NotEqual(tracerTraceParent, actualTraceParent);
        }

        [Fact]
        public async Task SendAsync_PreservesOtherHeaders()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var mockTracer = new MockActivityTracer { TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" };
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer);
            var httpClient = new HttpClient(tracingHandler);

            // Act
            var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
            request.Headers.Add("Authorization", "Bearer test-token");
            request.Headers.Add("X-Custom-Header", "custom-value");
            await httpClient.SendAsync(request);

            // Assert
            Assert.NotNull(mockHandler.CapturedRequest);
            Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
            Assert.True(mockHandler.CapturedRequest.Headers.Contains("Authorization"));
            Assert.True(mockHandler.CapturedRequest.Headers.Contains("X-Custom-Header"));
            Assert.Equal("Bearer test-token", mockHandler.CapturedRequest.Headers.GetValues("Authorization").FirstOrDefault());
            Assert.Equal("custom-value", mockHandler.CapturedRequest.Headers.GetValues("X-Custom-Header").FirstOrDefault());
        }

        [Fact]
        public async Task SendAsync_WithCustomHeaderName_UsesCustomHeader()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var traceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
            var mockTracer = new MockActivityTracer { TraceParent = traceParent };
            var customHeaderName = "X-Trace-Id";
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer, customHeaderName);
            var httpClient = new HttpClient(tracingHandler);

            // Act
            var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
            await httpClient.SendAsync(request);

            // Assert
            Assert.NotNull(mockHandler.CapturedRequest);
            Assert.False(mockHandler.CapturedRequest.Headers.Contains("traceparent")); // Default header should not be present
            Assert.True(mockHandler.CapturedRequest.Headers.Contains(customHeaderName));
            var headerValue = mockHandler.CapturedRequest.Headers.GetValues(customHeaderName).FirstOrDefault();
            Assert.Equal(traceParent, headerValue);
        }

        [Fact]
        public async Task SendAsync_WithTraceStateEnabled_IncludesTraceState()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var mockTracer = new MockActivityTracer();
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer, "traceparent", includeTraceState: true);
            var httpClient = new HttpClient(tracingHandler);

            // Create an activity with trace state
            Activity.DefaultIdFormat = ActivityIdFormat.W3C;
            ActivitySource.AddActivityListener(new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            });

            using var activitySource = new ActivitySource("TestActivitySource");
            using var activity = activitySource.StartActivity("TestActivity", ActivityKind.Client);

            if (activity != null)
            {
                activity.TraceStateString = "vendor1=value1,vendor2=value2";

                // Act
                var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
                await httpClient.SendAsync(request);

                // Assert
                Assert.NotNull(mockHandler.CapturedRequest);
                Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
                Assert.True(mockHandler.CapturedRequest.Headers.Contains("tracestate"));
                var traceStateValue = mockHandler.CapturedRequest.Headers.GetValues("tracestate").FirstOrDefault();
                Assert.Equal("vendor1=value1,vendor2=value2", traceStateValue);
            }
        }

        [Fact]
        public async Task SendAsync_WithTraceStateDisabled_DoesNotIncludeTraceState()
        {
            // Arrange
            var mockHandler = new MockHttpMessageHandler();
            var mockTracer = new MockActivityTracer();
            var tracingHandler = new TracingDelegatingHandler(mockHandler, mockTracer, "traceparent", includeTraceState: false);
            var httpClient = new HttpClient(tracingHandler);

            // Create an activity with trace state
            Activity.DefaultIdFormat = ActivityIdFormat.W3C;
            ActivitySource.AddActivityListener(new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
            });

            using var activitySource = new ActivitySource("TestActivitySource");
            using var activity = activitySource.StartActivity("TestActivity", ActivityKind.Client);

            if (activity != null)
            {
                activity.TraceStateString = "vendor1=value1,vendor2=value2";

                // Act
                var request = new HttpRequestMessage(HttpMethod.Get, "https://test.databricks.com/api/test");
                await httpClient.SendAsync(request);

                // Assert
                Assert.NotNull(mockHandler.CapturedRequest);
                Assert.True(mockHandler.CapturedRequest.Headers.Contains("traceparent"));
                Assert.False(mockHandler.CapturedRequest.Headers.Contains("tracestate")); // Should not include trace state
            }
        }
    }
}

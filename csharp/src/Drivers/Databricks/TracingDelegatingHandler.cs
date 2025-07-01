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

using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// A delegating handler that adds W3C Trace Context headers to outgoing HTTP requests.
    /// </summary>
    internal class TracingDelegatingHandler : DelegatingHandler
    {
        private readonly IActivityTracer _activityTracer;
        private readonly string _traceParentHeaderName;
        private readonly bool _includeTraceState;

        /// <summary>
        /// Initializes a new instance of the <see cref="TracingDelegatingHandler"/> class.
        /// </summary>
        /// <param name="innerHandler">The inner handler.</param>
        /// <param name="activityTracer">The activity tracer that provides trace context.</param>
        /// <param name="traceParentHeaderName">The name of the trace parent header. Defaults to "traceparent".</param>
        /// <param name="includeTraceState">Whether to include trace state header. Defaults to false.</param>
        public TracingDelegatingHandler(
            HttpMessageHandler innerHandler, 
            IActivityTracer activityTracer,
            string traceParentHeaderName = "traceparent",
            bool includeTraceState = false)
            : base(innerHandler)
        {
            _activityTracer = activityTracer;
            _traceParentHeaderName = traceParentHeaderName;
            _includeTraceState = includeTraceState;
        }

        /// <summary>
        /// Sends an HTTP request with W3C trace context headers.
        /// </summary>
        /// <param name="request">The HTTP request message to send.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // Get the current activity or use the trace parent from the connection
            Activity? currentActivity = Activity.Current;
            string? traceParentValue = null;
            string? traceStateValue = null;

            if (currentActivity != null)
            {
                // Use the current activity's W3C trace parent format
                traceParentValue = currentActivity.Id;
                
                // Get trace state if enabled
                if (_includeTraceState)
                {
                    traceStateValue = currentActivity.TraceStateString;
                }
            }
            else if (!string.IsNullOrEmpty(_activityTracer.TraceParent))
            {
                // Fall back to the trace parent set on the connection
                traceParentValue = _activityTracer.TraceParent;
                
                // Note: We don't have trace state from the connection, only from Activity
            }

            // Add the trace parent header if we have a value
            if (!string.IsNullOrEmpty(traceParentValue))
            {
                request.Headers.TryAddWithoutValidation(_traceParentHeaderName, traceParentValue);
                
                // Add trace state header if we have a value and it's enabled
                if (_includeTraceState && !string.IsNullOrEmpty(traceStateValue))
                {
                    request.Headers.TryAddWithoutValidation("tracestate", traceStateValue);
                }
            }

            return await base.SendAsync(request, cancellationToken);
        }
    }
}
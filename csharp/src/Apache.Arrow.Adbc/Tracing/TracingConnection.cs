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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using OpenTelemetry.Trace;

namespace Apache.Arrow.Adbc.Tracing
{
    public abstract class TracingConnection : AdbcConnection, IActivityTracer
    {
        private TracerProvider? _tracerProvider;
        private bool _isDisposed;
        internal readonly ActivityTrace _trace;

        protected TracingConnection(IReadOnlyDictionary<string, string> properties)
        {
            _tracerProvider = ActivityTrace.InitTracerProvider(out string activitySourceName, out _, TracingAssembly);
            properties.TryGetValue(AdbcOptions.Telemetry.TraceParent, out string? traceParent);
            _trace = new ActivityTrace(activitySourceName, traceParent);
        }

        public abstract Assembly TracingAssembly { get; }

        protected string? TraceParent
        {
            get
            {
                return _trace.TraceParent;
            }
            set
            {
                _trace.TraceParent = value;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    if (_tracerProvider != null)
                    {
                        _tracerProvider.Dispose();
                        _tracerProvider = null;
                    }
                }
                _isDisposed = true;
            }
        }

        // Note: if base class implements this code, remove this override
        public override void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public void TraceActivity(Action<Activity?> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            _trace.TraceActivity(call, activityName, traceParent ?? TraceParent);
        }

        public T TraceActivity<T>(Func<Activity?, T> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return _trace.TraceActivity(call, activityName, traceParent ?? TraceParent);
        }

        public Task TraceActivityAsync(Func<Activity?, Task> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return _trace.TraceActivityAsync(call, activityName, traceParent ?? TraceParent);
        }

        public Task<T> TraceActivityAsync<T>(Func<Activity?, Task<T>> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return _trace.TraceActivityAsync(call, activityName, traceParent ?? TraceParent);
        }
    }
}

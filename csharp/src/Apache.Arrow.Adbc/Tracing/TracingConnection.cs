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
using System.Reflection;
using OpenTelemetry.Trace;

namespace Apache.Arrow.Adbc.Tracing
{
    internal abstract class TracingConnection : AdbcConnection
    {
        private TracerProvider? _tracerProvider;
        private bool _isDisposed;

        protected TracingConnection(IReadOnlyDictionary<string, string> properties, Assembly? executingAssembly = default)
        {
            _tracerProvider = ActivityTrace.InitTracerProvider(out string activitySourceName, out _, executingAssembly);
            properties.TryGetValue(AdbcOptions.Telemetry.TraceParent, out string? traceParent);
            Trace = new ActivityTrace(activitySourceName, traceParent);
        }

        public ActivityTrace Trace { get; }

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
    }
}

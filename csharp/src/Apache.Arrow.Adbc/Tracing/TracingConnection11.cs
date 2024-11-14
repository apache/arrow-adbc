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

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides an <see cref="AdbcConnection11"> which can instrument tracing activity.
    /// </summary>
    public abstract class TracingConnection11 : AdbcConnection11, ITracingObject
    {
        private bool _disposed;

        protected TracingConnection11(IReadOnlyDictionary<string, string>? properties)
        {
            TracingConnectionImpl = new TracingConnectionImpl(TracingBaseName, properties);
        }

        protected TracingConnection11(bool isTracingEnabled, string traceLocation, int traceMaxFileSizeKb, int traceMaxFiles)
        {
            TracingConnectionImpl = new TracingConnectionImpl(TracingBaseName, isTracingEnabled, traceLocation, traceMaxFileSizeKb, traceMaxFiles);
        }

        private TracingConnectionImpl TracingConnectionImpl { get; }

        public ActivitySource? ActivitySource => TracingConnectionImpl.ActivitySource;

        public abstract string TracingBaseName { get; }

        public Activity? StartActivity(string methodName) => TracingConnectionImpl.StartActivity(methodName);

        public static Activity? StartActivity(ActivitySource? activitySource, string baseName, string methodName) =>
            TracingConnectionImpl.StartActivity(activitySource, baseName, methodName);

        public static string ProductVersionDefault => TracingConnectionImpl.ProductVersionDefault;

        public static string GetProductVersion() => TracingConnectionImpl.GetProductVersion();

        public void TraceException(Exception exception, Activity? activity, bool escaped = true) =>
            TracingConnectionImpl.TraceException(exception, activity, escaped);

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    TracingConnectionImpl.Dispose();
                }
                _disposed = true;
            }
        }
    }
}

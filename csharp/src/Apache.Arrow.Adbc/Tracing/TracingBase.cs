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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides a base implementation for a tracing source. If drivers want to enable tracing,
    /// they need to add a trace listener (e.g., <see cref="TracingFileListener"/>).
    /// </summary>
    public class TracingBase : IDisposable
    {
        internal const string ProductVersionDefault = "1.0.0";
        private readonly string _activitySourceName;
        private static readonly string s_assemblyVersion = GetProductVersion();
        private bool _disposedValue;

        protected TracingBase()
        {
            _activitySourceName = GetType().Assembly.GetName().Name!;
            // This is required to be disposed
            ActivitySource = new(_activitySourceName, s_assemblyVersion);
        }

        protected ActivitySource ActivitySource { get; }

        /// <summary>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise.
        /// </summary>
        /// <param name="methodName">The name of the method for the activity</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        protected Activity? StartActivity([CallerMemberName] string methodName = "[unknown-method]")
        {
            StackTrace stackTrace = new();
            MethodBase? callingMethod = stackTrace.GetFrame(1)?.GetMethod();
            string tracingBaseName = callingMethod?.DeclaringType?.DeclaringType?.FullName ?? callingMethod?.DeclaringType?.FullName ?? "[unknown-class]";
            return StartActivity(ActivitySource, tracingBaseName, methodName);
        }

        protected static void TraceException(Exception exception, Activity? activity, bool escaped = true) =>
            WriteTraceException(exception, activity, escaped);

        protected internal static Activity? StartActivity(ActivitySource? activitySource, [CallerMemberName] string methodName = "[unknown-method]")
        {
            MethodBase? callingMethod = (new StackTrace()).GetFrame(1)?.GetMethod();
            string tracingBaseName = callingMethod?.DeclaringType?.DeclaringType?.FullName ?? callingMethod?.DeclaringType?.FullName ?? "[unknown-class]";
            return StartActivity(activitySource, tracingBaseName, methodName);
        }

        protected static string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? ProductVersionDefault;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    ActivitySource?.Dispose();
                }
                _disposedValue = true;
            }
        }

        public virtual void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        private static void WriteTraceException(Exception exception, Activity? activity, bool escaped = true)
        {
            // https://opentelemetry.io/docs/specs/otel/trace/exceptions/
            activity?.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection(
                [
                    // TODO: Determine if "exception.escaped" is being set correctly.
                    // https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-spans/
                    new("exception.escaped", escaped),
                            new("exception.message", exception.Message),
                            new("exception.stacktrace", exception.StackTrace),
                            new("exception.type", exception.GetType().Name),
                        ])));
        }

        private static Activity? StartActivity(ActivitySource? activitySource, string tracingBaseName, string methodName)
        {
            return activitySource?.StartActivity(tracingBaseName + "." + methodName );
        }
    }
}

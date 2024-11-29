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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides a base implementation for a tracing source. If drivers want to enable tracing,
    /// they need to add a trace listener (e.g., <see cref="FileExporter"/>).
    /// </summary>
    public class TracingBase : IDisposable
    {
        internal const string ProductVersionDefault = "1.0.0";
        private static readonly string s_assemblyVersion = GetProductVersion();
        private bool _disposedValue;
        private string? _traceParent = null;

        /// <summary>
        /// Constructs a new <see cref="TracingBase"/> object. If <paramref name="activitySoureceName"/> is set, it provides the
        /// activity source name, otherwise the current assembly name is used as the acctivity source name.
        /// </summary>
        /// <param name="activitySoureceName"></param>
        protected TracingBase(string? activitySoureceName = default)
        {
            activitySoureceName ??= GetType().Assembly.GetName().Name!;
            if (string.IsNullOrWhiteSpace(activitySoureceName))
            {
                throw new ArgumentNullException(nameof(activitySoureceName));
            }

            // This is required to be disposed
            ActivitySource = new(activitySoureceName, s_assemblyVersion);
        }

        /// <summary>
        /// Gets the <see cref="ActivitySource"/>.
        /// </summary>
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
            return StartActivity(ActivitySource, tracingBaseName, methodName, ParentContext);
        }

        /// <summary>
        /// Writes the exception to the trace by adding an exception event to the current activity (span).
        /// </summary>
        /// <param name="exception">The exception to trace.</param>
        /// <param name="activity">The current activity where the exception is caught.</param>
        /// <param name="escaped">
        /// An indicator that should be set to true if the exception event is recorded
        /// at a point where it is known that the exception is escaping the scope of the span/activity.
        /// For example, <c>escaped</c> should be <c>true</c> if the exception is caught and re-thrown.
        /// However, <c>escaped</c> should be set to <c>false</c> if execution continues in the current scope.
        /// </param>
        protected static void TraceException(Exception exception, Activity? activity, bool escaped = true) =>
            WriteTraceException(exception, activity, escaped);

        /// <summary>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise.
        /// </summary>
        /// <param name="activitySource">The <see cref="ActivitySource"/> from which to start the activity.</param>
        /// <param name="methodName">The name of the method for the activity</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        protected internal static Activity? StartActivity(ActivitySource? activitySource, [CallerMemberName] string methodName = "[unknown-method]", ActivityContext? parentContext = default)
        {
            MethodBase? callingMethod = (new StackTrace()).GetFrame(1)?.GetMethod();
            string tracingBaseName = callingMethod?.DeclaringType?.DeclaringType?.FullName
                ?? callingMethod?.DeclaringType?.FullName
                ?? "[unknown-class]";
            return StartActivity(activitySource, tracingBaseName, methodName, parentContext);
        }

        /// <summary>
        /// Gets the parent context.
        /// </summary>
        /// <remarks>
        /// To ensure the current parent context is retrieved only once,
        /// the current value is returned and then the value is reset to null.
        /// </remarks>
        protected internal virtual ActivityContext? ParentContext
        {
            get
            {
                string? traceParent = Interlocked.Exchange(ref _traceParent, null);
                if (ActivityContext.TryParse(traceParent, null, isRemote: true, out ActivityContext parentContext))
                {
                    return parentContext;
                }
                return null;
            }
        }

        /// <summary>
        /// Sets the Trace Parent. See <see href="https://www.w3.org/TR/trace-context/#traceparent-header"/>
        /// </summary>
        /// <param name="traceParent"></param>
        protected internal virtual void SetTraceParent(string? traceParent)
        {
            Interlocked.Exchange(ref _traceParent, traceParent);
        }

        /// <summary>
        /// Gets the product version from the file version of the current assembly.
        /// </summary>
        /// <returns></returns>
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
                    ActivitySource.Dispose();
                }
                _disposedValue = true;
            }
        }

        /// <inheritdoc />
        public virtual void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        private static void WriteTraceException(Exception exception, Activity? activity, bool escaped = true)
        {
            activity?.SetStatus(ActivityStatusCode.Error);
            // https://opentelemetry.io/docs/specs/otel/trace/exceptions/
            activity?.AddEvent(new ActivityEvent("exception", tags: new(
                [
                    // https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-spans/
                    new("exception.escaped", escaped),
                    new("exception.message", exception.Message),
                    new("exception.stacktrace", exception.StackTrace),
                    new("exception.type", exception.GetType().Name),
                ])));
        }

        private static Activity? StartActivity(ActivitySource? activitySource, string tracingBaseName, string methodName, ActivityContext? parentContext)
        {
            return parentContext == null
                ? (activitySource?.StartActivity(tracingBaseName + "." + methodName, ActivityKind.Client))
                : (activitySource?.StartActivity(tracingBaseName + "." + methodName, ActivityKind.Client, (ActivityContext)parentContext));
        }
    }
}

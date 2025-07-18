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
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides the implementation for a tracing source. If drivers want to enable tracing,
    /// they need to add a trace listener (e.g., <see cref="FileExporter.FileExporter"/>).
    /// </summary>
    public sealed class ActivityTrace : IDisposable
    {
        private bool _disposed;

        /// <summary>
        /// Constructs a new <see cref="ActivityTrace"/> object. If <paramref name="activitySourceName"/> is set, it provides the
        /// activity source name, otherwise the current assembly name is used as the activity source name.
        /// </summary>
        /// <param name="activitySourceName"></param>
        public ActivityTrace(string? activitySourceName = default, string? activitySourceVersion = default, string? traceParent = default)
        {
            activitySourceName ??= GetType().Assembly.GetName().Name!;
            // It's okay to have a null version.
            activitySourceVersion ??= FileVersionInfo.GetVersionInfo(GetType().Assembly.Location).ProductVersion;
            if (string.IsNullOrWhiteSpace(activitySourceName))
            {
                throw new ArgumentNullException(nameof(activitySourceName));
            }

            TraceParent = traceParent;
            // This is required to be disposed
            ActivitySource = new(activitySourceName, activitySourceVersion);
        }

        /// <summary>
        /// Gets the <see cref="System.Diagnostics.ActivitySource"/>.
        /// </summary>
        public ActivitySource ActivitySource { get; }

        /// <summary>
        /// Gets the name of the <see cref="System.Diagnostics.ActivitySource"/>
        /// </summary>
        public string ActivitySourceName => ActivitySource.Name;

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="activityName">The name of the method for the activity.</param>
        /// <param name="traceParent">Optional trace parent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="exceptionIsPii">Indicator for whether a thrown exception could contain PII.</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/>. If an exception is thrown by the delegate, the Activity
        /// status is set to <see cref="ActivityStatusCode.Error"/> and an Activity <see cref="ActivityEvent"/> is added to the activity
        /// and finally the exception is rethrown.
        /// </remarks>
        public void TraceActivity(Action<ActivityWithPii?> call, [CallerMemberName] string? activityName = default, string? traceParent = default, bool exceptionIsPii = true)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ActivityTrace));
            using ActivityWithPii? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
            try
            {
                call(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                TraceException(ex, activity, exceptionIsPii);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="activityName">The name of the method for the activity.</param>
        /// <param name="traceParent">Optional trace parent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="exceptionIsPii">Indicator for whether a thrown exception could contain PII.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public T TraceActivity<T>(Func<ActivityWithPii?, T> call, [CallerMemberName] string? activityName = default, string? traceParent = default, bool exceptionIsPii = true)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ActivityTrace));
            using ActivityWithPii? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
            try
            {
                T? result = call(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
                return result;
            }
            catch (Exception ex)
            {
                TraceException(ex, activity, exceptionIsPii);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="activityName">The name of the method for the activity.</param>
        /// <param name="traceParent">Optional trace parent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="exceptionIsPii">Indicator for whether a thrown exception could contain PII.</param>
        /// <returns></returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public async Task TraceActivityAsync(Func<ActivityWithPii?, Task> call, [CallerMemberName] string? activityName = default, string? traceParent = default, bool exceptionIsPii = true)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ActivityTrace));
            using ActivityWithPii? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
            try
            {
                await call(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                TraceException(ex, activity, exceptionIsPii);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="activityName">The name of the method for the activity.</param>
        /// <param name="traceParent">Optional trace parent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="exceptionIsPii">Indicator for whether a thrown exception could contain PII.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public async Task<T> TraceActivityAsync<T>(Func<ActivityWithPii?, Task<T>> call, [CallerMemberName] string? activityName = default, string? traceParent = default, bool exceptionIsPii = true)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ActivityTrace));
            using ActivityWithPii? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
            try
            {
                T? result = await call(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
                return result;
            }
            catch (Exception ex)
            {
                TraceException(ex, activity, exceptionIsPii);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="activitySource">The <see cref="ActivitySource"/> to start the <see cref="Activity"/> on.</param>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="activityName">The name of the method for the activity.</param>
        /// <param name="traceParent">Optional trace parent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="exceptionIsPii">Indicator for whether a thrown exception could contain PII.</param>
        /// <returns></returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public static async Task TraceActivityAsync(ActivitySource activitySource, Func<ActivityWithPii?, Task> call, [CallerMemberName] string? activityName = default, string? traceParent = default, bool exceptionIsPii = true)
        {
            using ActivityWithPii? activity = StartActivityInternal(activityName, activitySource, traceParent);
            try
            {
                await call(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                TraceException(ex, activity, exceptionIsPii);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="activitySource">The <see cref="ActivitySource"/> to start the <see cref="Activity"/> on.</param>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="activityName">The name of the method for the activity.</param>
        /// <param name="traceParent">Optional trace parent id for the associated <see cref="ActivityContext"/>.</param>
        /// <param name="exceptionIsPii">Indicator for whether a thrown exception could contain PII.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Event <see cref="ActivityEvent"/> is added to the activity and finally the exception is rethrown.
        /// </remarks>
        public static async Task<T> TraceActivityAsync<T>(ActivitySource activitySource, Func<ActivityWithPii?, Task<T>> call, [CallerMemberName] string? activityName = default, string? traceParent = default, bool exceptionIsPii = true)
        {
            using ActivityWithPii? activity = StartActivityInternal(activityName, activitySource, traceParent);
            try
            {
                T? result = await call(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
                return result;
            }
            catch (Exception ex)
            {
                TraceException(ex, activity, exceptionIsPii);
                throw;
            }
        }

        /// <summary>
        /// Gets or sets the trace parent context.
        /// </summary>
        public string? TraceParent { get; set; }

        /// <summary>
        /// Writes the exception to the trace by adding an exception event to the current activity (span).
        /// </summary>
        /// <param name="exception">The exception to trace.</param>
        /// <param name="activity">The current activity where the exception is caught.</param>
        /// <param name="isPii">Indicator of whether the exception message could contain PII.</param>
        private static void TraceException(Exception exception, ActivityWithPii? activity, bool isPii)
        {
            activity?.AddException(exception, isPii);
            activity?.SetStatus(ActivityStatusCode.Error);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed) { return; }

            ActivitySource.Dispose();
            _disposed = true;
        }

        private static ActivityWithPii? StartActivityInternal(string? activityName, ActivitySource activitySource, string? traceParent = default)
        {
            string fullActivityName = GetActivityName(activityName);
            return StartActivity(activitySource, fullActivityName, traceParent);
        }

        private static string GetActivityName(string? activityName)
        {
            if (string.IsNullOrWhiteSpace(activityName))
            {
                activityName = "[unknown-member]";
            }
            return activityName!;
        }

        /// <summary>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise.
        /// </summary>
        /// <param name="activitySource">The <see cref="ActivitySource"/> from which to start the activity.</param>
        /// <param name="activityName">The name of the method for the activity</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        private static ActivityWithPii? StartActivity(ActivitySource activitySource, string activityName, string? traceParent = default)
        {
            Activity? activity = traceParent != null && ActivityContext.TryParse(traceParent, null, isRemote: true, out ActivityContext parentContext)
                ? (activitySource.StartActivity(activityName, ActivityKind.Client, parentContext))
                : (activitySource.StartActivity(activityName, ActivityKind.Client));
            return activity != null ? new ActivityWithPii(activity) : null;
        }
    }
}

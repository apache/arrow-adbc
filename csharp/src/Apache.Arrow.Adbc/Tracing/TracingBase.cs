﻿/*
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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

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
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/>. If an exception is thrown by the delegate, the Activity
        /// status is set to <see cref="ActivityStatusCode.Error"/> and an Activity <see cref="ActivityEvent"/> is added to the actitity
        /// and finally the exception is rethrown.
        /// </remarks>
        protected void TraceActivity(Action<Activity?> call, [CallerMemberName] string? memberName = default)
        {
            using Activity? activity = StartActivityInternal(memberName, ActivitySource, TraceParent);
            try
            {
                call.Invoke(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                TraceException(ex, activity);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Activity <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        protected T TraceActivity<T>(Func<Activity?, T> call, [CallerMemberName] string? memberName = default)
        {
            using Activity? activity = StartActivityInternal(memberName, ActivitySource, TraceParent);
            try
            {
                T? result = call.Invoke(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
                return result;
            }
            catch (Exception ex)
            {
                TraceException(ex, activity);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns></returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Activity <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        protected async Task TraceActivityAsync(Func<Activity?, Task> call, [CallerMemberName] string? memberName = default)
        {
            using Activity? activity = StartActivityInternal(memberName, ActivitySource, TraceParent);
            try
            {
                await call.Invoke(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                TraceException(ex, activity);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Activity <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        protected async Task<T> TraceActivityAsync<T>(Func<Activity?, Task<T>> call, [CallerMemberName] string? memberName = default)
        {
            using Activity? activity = StartActivityInternal(memberName, ActivitySource, TraceParent);
            try
            {
                T? result = await call.Invoke(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
                return result;
            }
            catch (Exception ex)
            {
                TraceException(ex, activity);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <param name="activitySource">The <see cref="ActivitySource"/> to start the <see cref="Activity"/> on.</param>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns></returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Activity <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        protected static async Task TraceActivityAsync(ActivitySource? activitySource, Func<Activity?, Task> call, [CallerMemberName] string? memberName = default)
        {
            using Activity? activity = StartActivityInternal(memberName, activitySource);
            try
            {
                await call.Invoke(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                TraceException(ex, activity);
                throw;
            }
        }

        /// <summary>
        /// Invokes the delegate within the context of a new started <see cref="Activity"/>.
        /// </summary>
        /// <typeparam name="T">The return type for the delegate.</typeparam>
        /// <param name="activitySource">The <see cref="ActivitySource"/> to start the <see cref="Activity"/> on.</param>
        /// <param name="call">The delegate to call within the context of a newly started <see cref="Activity"/></param>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>The result of the call to the delegate.</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/> and the result is returned.
        /// If an exception is thrown by the delegate, the Activity status is set to <see cref="ActivityStatusCode.Error"/>
        /// and an Activity <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        protected static async Task<T> TraceActivityAsync<T>(ActivitySource? activitySource, Func<Activity?, Task<T>> call, [CallerMemberName] string? memberName = default)
        {
            using Activity? activity = StartActivityInternal(memberName, activitySource);
            try
            {
                T? result = await call.Invoke(activity);
                if (activity?.Status == ActivityStatusCode.Unset) activity?.SetStatus(ActivityStatusCode.Ok);
                return result;
            }
            catch (Exception ex)
            {
                TraceException(ex, activity);
                throw;
            }
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
        /// Starts an <see cref="Activity"/> on the <see cref="TracingBase.ActivitySource"/> if there is
        /// and active listener on the source.
        /// </summary>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>If there is an active listener on the source, an <see cref="Activity"/> is returned, null otherwise.</returns>
        protected Activity? StartActivity([CallerMemberName] string? memberName = default)
        {
            return StartActivityInternal(memberName, ActivitySource, TraceParent);
        }

        /// <summary>
        /// Gets or sets the trace parent context.
        /// </summary>
        protected internal virtual string? TraceParent { get; set; }

        /// <summary>
        /// Gets the product version from the file version of the current assembly.
        /// </summary>
        /// <returns></returns>
        protected static string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? ProductVersionDefault;
        }

        /// <summary>
        /// Disposes managed and unmanaged objects. If overridden, ensure to call this base method.
        /// </summary>
        /// <param name="disposing">An indicator of whether this method is being called from the <c>Dispose</c> method.</param>
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

        private static Activity? StartActivityInternal(string? memberName, ActivitySource? activitySource, string? traceParent = default)
        {
            string activityName = GetActivityName(memberName);
            return StartActivity(activitySource, activityName, traceParent);
        }

        private static string GetActivityName(string? memberName)
        {
            memberName ??= "[unknown-member]";
            StackTrace stackTrace = new();
            StackFrame? frame = stackTrace.GetFrames().FirstOrDefault(f => f.GetMethod()?.Name == memberName);
            string tracingBaseName = frame?.GetMethod()?.DeclaringType?.FullName ?? "[unknown-type]";
            string activityName = tracingBaseName + "." + memberName;
            return activityName;
        }

        /// <summary>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise.
        /// </summary>
        /// <param name="activitySource">The <see cref="ActivitySource"/> from which to start the activity.</param>
        /// <param name="activityName">The name of the method for the activity</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        private static Activity? StartActivity(ActivitySource? activitySource, string activityName, string? traceParent = default)
        {
            return traceParent == null
                ? (activitySource?.StartActivity(activityName, ActivityKind.Client))
                : (activitySource?.StartActivity(activityName, ActivityKind.Client, traceParent));
        }
    }
}
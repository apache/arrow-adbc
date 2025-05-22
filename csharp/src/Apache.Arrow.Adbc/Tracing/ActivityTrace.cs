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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing.FileExporter;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides a base implementation for a tracing source. If drivers want to enable tracing,
    /// they need to add a trace listener (e.g., <see cref="FileExporter"/>).
    /// </summary>
    public class ActivityTrace
    {
        private const string ProductVersionDefault = "1.0.0";
        private static readonly string s_assemblyVersion = GetProductVersion();
        private bool _disposedValue;
        private const string DefaultSourceName = "apache.arrow.adbc";
        private const string DefaultSourceVersion = "0.0.0.0";
        private const string TracesExporterEnvironment = AdbcOptions.Telemetry.Traces.Exporter.Environment;

        /// <summary>
        /// Constructs a new <see cref="ActivityTrace"/> object. If <paramref name="activitySourceName"/> is set, it provides the
        /// activity source name, otherwise the current assembly name is used as the activity source name.
        /// </summary>
        /// <param name="activitySourceName"></param>
        public ActivityTrace(string? activitySourceName = default, string? traceParent = default)
        {
            activitySourceName ??= GetType().Assembly.GetName().Name!;
            if (string.IsNullOrWhiteSpace(activitySourceName))
            {
                throw new ArgumentNullException(nameof(activitySourceName));
            }

            // This is required to be disposed
            ActivitySource = new(activitySourceName, s_assemblyVersion);
            TraceParent = traceParent;
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
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        /// <remarks>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener for the ActivitySource.
        /// Passes the Activity to the delegate and invokes the delegate. If there are no exceptions thrown by the delegate the
        /// Activity status is set to <see cref="ActivityStatusCode.Ok"/>. If an exception is thrown by the delegate, the Activity
        /// status is set to <see cref="ActivityStatusCode.Error"/> and an Activity <see cref="ActivityEvent"/> is added to the actitity
        /// and finally the exception is rethrown.
        /// </remarks>
        public void TraceActivity(Action<Activity?> call, [CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            using Activity? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
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
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public T TraceActivity<T>(Func<Activity?, T> call, [CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            using Activity? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
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
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public async Task TraceActivityAsync(Func<Activity?, Task> call, [CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            using Activity? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
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
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public async Task<T> TraceActivityAsync<T>(Func<Activity?, Task<T>> call, [CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            using Activity? activity = StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
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
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public static async Task TraceActivityAsync(ActivitySource activitySource, Func<Activity?, Task> call, [CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            using Activity? activity = StartActivityInternal(activityName, activitySource, traceParent);
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
        /// and an Event <see cref="ActivityEvent"/> is added to the actitity and finally the exception is rethrown.
        /// </remarks>
        public static async Task<T> TraceActivityAsync<T>(ActivitySource activitySource, Func<Activity?, Task<T>> call, [CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            using Activity? activity = StartActivityInternal(activityName, activitySource, traceParent);
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
        public static void TraceException(Exception exception, Activity? activity) =>
            WriteTraceException(exception, activity);

        /// <summary>
        /// Starts an <see cref="Activity"/> on the <see cref="ActivityTrace.ActivitySource"/> if there is
        /// and active listener on the source.
        /// </summary>
        /// <param name="methodName">The name of the method for the activity.</param>
        /// <returns>If there is an active listener on the source, an <see cref="Activity"/> is returned, null otherwise.</returns>
        public Activity? StartActivity([CallerMemberName] string? activityName = default, string? traceParent = default)
        {
            return StartActivityInternal(activityName, ActivitySource, traceParent ?? TraceParent);
        }

        /// <summary>
        /// Gets or sets the trace parent context.
        /// </summary>
        public string? TraceParent { get; set; }

        /// <summary>
        /// Gets the product version from the file version of the current assembly.
        /// </summary>
        /// <returns></returns>
        private static string GetProductVersion()
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

        public static TracerProvider? InitTracerProvider(out string activitySourceName, out string activitySourceVersion, Assembly? executingAssembly = default)
        {
            AssemblyName s_assemblyName = executingAssembly?.GetName() ?? Assembly.GetExecutingAssembly().GetName();
            string sourceName = s_assemblyName.Name ?? DefaultSourceName;
            string sourceVersion = s_assemblyName.Version?.ToString() ?? DefaultSourceVersion;
            activitySourceName = sourceName;
            activitySourceVersion = sourceVersion;

            string? tracesExporter = Environment.GetEnvironmentVariable(TracesExporterEnvironment);
            return tracesExporter switch
            {
                null or "" => null,// Do not create a listener/exporter
                AdbcOptions.Telemetry.Traces.Exporter.Otlp => Sdk.CreateTracerProviderBuilder()
                    .AddSource(sourceName)
                    .ConfigureResource(resource =>
                        resource.AddService(
                            serviceName: sourceName,
                            serviceVersion: sourceVersion))
                    .AddOtlpExporter()
                    .Build(),
                AdbcOptions.Telemetry.Traces.Exporter.Console => Sdk.CreateTracerProviderBuilder()
                    .AddSource(sourceName)
                    .ConfigureResource(resource =>
                        resource.AddService(
                            serviceName: sourceName,
                            serviceVersion: sourceVersion))
                    .AddConsoleExporter()
                    .Build(),
                AdbcOptions.Telemetry.Traces.Exporter.AdbcFile => Sdk.CreateTracerProviderBuilder()
                    .AddSource(sourceName)
                    .ConfigureResource(resource =>
                        resource.AddService(
                            serviceName: sourceName,
                            serviceVersion: sourceVersion))
                    .AddAdbcFileExporter()
                    .Build(),
                _ => throw new AdbcException(
                        $"Unsupported {TracesExporterEnvironment} option: '{tracesExporter}'",
                        AdbcStatusCode.InvalidArgument),
            };
        }

        private static void WriteTraceException(Exception exception, Activity? activity)
        {
            activity?.AddException(exception);
            activity?.SetStatus(ActivityStatusCode.Error);
        }

        private static Activity? StartActivityInternal(string? activityName, ActivitySource activitySource, string? traceParent = default)
        {
            string fullActivityName = GetActivityName(activityName);
            return StartActivity(activitySource, fullActivityName, traceParent);
        }

        private static string GetActivityName(string? activityName)
        {
            string tracingBaseName = string.Empty;
            if (!string.IsNullOrWhiteSpace(activityName))
            {
                StackTrace stackTrace = new();
                StackFrame? frame = stackTrace.GetFrames().FirstOrDefault(f => f.GetMethod()?.Name == activityName);
                tracingBaseName = frame?.GetMethod()?.DeclaringType?.FullName ?? string.Empty;
                if (tracingBaseName != string.Empty)
                {
                    tracingBaseName += ".";
                }
            }
            else
            {
                activityName = "[unknown-member]";
            }
            string fullActivityName = tracingBaseName + activityName;
            return fullActivityName;
        }

        /// <summary>
        /// Creates and starts a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise.
        /// </summary>
        /// <param name="activitySource">The <see cref="ActivitySource"/> from which to start the activity.</param>
        /// <param name="activityName">The name of the method for the activity</param>
        /// <returns>Returns a new <see cref="Activity"/> object if there is any listener to the Activity, returns null otherwise</returns>
        private static Activity? StartActivity(ActivitySource activitySource, string activityName, string? traceParent = default)
        {
            return traceParent != null && ActivityContext.TryParse(traceParent, null, isRemote: true, out ActivityContext parentContext)
                ? (activitySource.StartActivity(activityName, ActivityKind.Client, parentContext))
                : (activitySource.StartActivity(activityName, ActivityKind.Client));
        }
    }
}

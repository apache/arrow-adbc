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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides an <see cref="AdbcConnection"> which can instrument tracing activity.
    /// </summary>
    internal sealed class TracingConnectionImpl : ITracingObject, IDisposable
    {
        private bool _disposed = false;
        internal const string ProductVersionDefault = "1.0.0";
        private static readonly string s_activitySourceName = Assembly.GetExecutingAssembly().GetName().Name!;
        private static readonly string s_assemblyVersion = GetProductVersion();
        private static readonly ConcurrentDictionary<string, ActivityListener> s_listeners = new();
        private readonly ConcurrentQueue<Activity> _activityQueue = new();
        private readonly IReadOnlyDictionary<string, string>? _options;
        private DirectoryInfo? _traceDirectory;
        private readonly string _baseName;

        internal TracingConnectionImpl(string baseName, IReadOnlyDictionary<string, string>? options = default)
        {
            _baseName = baseName;
            _options = options ?? new Dictionary<string, string>();
            EnsureTracing();
        }

        internal TracingConnectionImpl(string baseName, bool isTracingEnabled, string traceLocation, int traceMaxFileSizeKb, int traceMaxFiles)
        {
            _baseName = baseName;
            var options = new Dictionary<string, string>();
            options[TracingOptions.Connection.Trace] = isTracingEnabled.ToString();
            options[TracingOptions.Connection.TraceLocation] = traceLocation;
            options[TracingOptions.Connection.TraceFileMaxSizeKb] = traceMaxFileSizeKb.ToString();
            options[TracingOptions.Connection.TraceFileMaxFiles] = traceMaxFiles.ToString();
            _options = options;
            EnsureTracing();
        }

        public ActivitySource? ActivitySource { get; private set; }

        public string TracingBaseName => _baseName;

        internal static string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? ProductVersionDefault;
        }

        private void EnsureTracing()
        {
            if (true || _options.TryGetValue(TracingOptions.Connection.Trace, out string? traceOption) && bool.TryParse(traceOption, out bool traceEnabled))
            {

                // TODO: Handle exceptions
                if (_options?.TryGetValue(TracingOptions.Connection.TraceLocation, out string? traceLocation) != true || !Directory.Exists(traceLocation))
                {
                    string? traceLocationDefault = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
                    _traceDirectory = new DirectoryInfo(traceLocationDefault);
                }
                else
                {
                    // TODO: If not exist, try to create
                    _traceDirectory = new DirectoryInfo(traceLocation);
                }
                // TODO: Check if folder is writable


                // TODO: Determine the best handling of listener lifetimes.
                // Key of listeners collection should be ouput file location
                ActivityListener listener = s_listeners.GetOrAdd(s_activitySourceName + "." + _traceDirectory.FullName, (_) => new()
                {
                    ShouldListenTo = (source) => source.Name == s_activitySourceName,
                    Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                    ActivityStarted = OnActivityStarted,
                    ActivityStopped = OnActivityStopped,
                    SampleUsingParentId = (ref ActivityCreationOptions<string> options) => ActivitySamplingResult.AllDataAndRecorded,
                });
                // This is a singleton add, if the lister is the same.
                ActivitySource.AddActivityListener(listener);
                // THis is an new instance and needs to be disposed later
                ActivitySource = new(s_activitySourceName, s_assemblyVersion);
            }
        }

        private void OnActivityStarted(Activity activity)
        {
            _activityQueue.Enqueue(activity);
            // Intentionally avoid await.
            DequeueAndWrite("started")
                .ContinueWith(t => Console.WriteLine(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
        }

        private void OnActivityStopped(Activity activity)
        {
            _activityQueue.Enqueue(activity);
            // Intentionally avoid await.
            DequeueAndWrite("stopped")
                .ContinueWith(t => Console.WriteLine(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
        }

        // TODO: Encapsulate this separately
        private Task DequeueAndWrite(string state)
        {
            if (_activityQueue.TryDequeue(out Activity? activity))
            {
                if (activity != null)
                {
                    try
                    {
                        string json = JsonSerializer.Serialize(new { State = state, Activity = activity });
                        Console.WriteLine(json);
                    }
                    catch (NotSupportedException ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }

            return Task.CompletedTask;
        }

        public Activity? StartActivity(string methodName)
        {
            return StartActivity(ActivitySource, _baseName, methodName);
        }

        internal static Activity? StartActivity(ActivitySource? activitySource, string baseName, string methodName) =>
            activitySource?.StartActivity(baseName + "." + methodName);

        public void TraceException(Exception exception, Activity? activity, bool escaped = true) =>
            TracingConnectionImpl.WriteTraceException(exception, activity, escaped);

        internal static void WriteTraceException(Exception exception, Activity? activity, bool escaped = true)
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

        internal void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    ActivitySource?.Dispose();
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}

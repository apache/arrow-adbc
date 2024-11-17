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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
{
    public class TracingFileListener : ITracingListener
    {
        private const int MaxFileCountDefault = 999;

        private bool _disposedValue;
        private readonly string _activitySourceName;
        private readonly string _traceFolderLocation;
        private static readonly ConcurrentDictionary<string, ActivityListener> s_listeners = new();
        private static readonly ConcurrentDictionary<string, int> s_listenerCounts = new();
        private readonly ConcurrentQueue<Activity> _activityQueue = new();
        private readonly TracingFile _tracingFile;
        private static readonly string s_tracingLocationDefault = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Apache.Arrow.Adbc", "Tracing");
        private static readonly object s_lock = new();
        private static CancellationTokenSource? s_cancellationTokenSource = null;
        private static Task? s_cleanupTask = null;

        public TracingFileListener(string? activitySourceName = default, string? traceFolderLocation = default)
        {
            _activitySourceName = activitySourceName ?? GetType().Assembly.GetName().Name!;
            if (traceFolderLocation == null)
            {
                string? traceLocationDefault = s_tracingLocationDefault;
                DirectoryInfo traceDirectory = new(traceLocationDefault);
                _traceFolderLocation = traceDirectory.FullName;
            }
            else
            {
                _traceFolderLocation = traceFolderLocation;
            }
            if (!Directory.Exists(_traceFolderLocation))
            {
                // TODO: Handle exceptions
                Directory.CreateDirectory(_traceFolderLocation);
            }

            _tracingFile = new TracingFile(_activitySourceName, _traceFolderLocation);
            lock (s_lock)
            {
                // TODO: Make this instanced by source and tracing folder! Not single instance
                if (s_cancellationTokenSource == null)
                {
                    s_cancellationTokenSource = new CancellationTokenSource();
                    s_cleanupTask = CleanupTraceDirectory(new DirectoryInfo(_traceFolderLocation), _activitySourceName, s_cancellationTokenSource.Token);
                }
                s_listenerCounts.AddOrUpdate(ListenerId, 1, (k, v) => v + 1);
                ActivityListener = s_listeners.GetOrAdd(ListenerId, (_) => NewActivityListener());
                ActivitySource.AddActivityListener(ActivityListener);
            }
        }

        private string ListenerId => $"{_activitySourceName}{_traceFolderLocation}";

        public ActivityListener ActivityListener { get; private set; }

        private ActivityListener NewActivityListener()
        {
            return new ActivityListener()
            {
                ShouldListenTo = (source) => source.Name == _activitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = OnActivityStarted,
                ActivityStopped = OnActivityStopped,
                SampleUsingParentId = (ref ActivityCreationOptions<string> options) => ActivitySamplingResult.AllDataAndRecorded,
            };
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

        private async Task DequeueAndWrite(string state, CancellationToken cancellationToken = default)
        {
            if (_activityQueue.TryDequeue(out Activity? activity) && activity != null)
            {
                try
                {
                    string json = JsonSerializer.Serialize(new { State = state, Activity = activity });
                    await _tracingFile.WriteLine(json, cancellationToken);
                }
                catch (NotSupportedException ex)
                {
                    // TODO: Handle excption thrown by JsonSerializer
                    Console.WriteLine(ex.Message);
                }
            }

            return;
        }

        private static async Task CleanupTraceDirectory(DirectoryInfo traceDirectory, string fileBaseName, CancellationToken cancellationToken)
        {
            const int delayTimeSeconds = 5;
            string searchPattern = fileBaseName + "-trace-*.log";
            while (!cancellationToken.IsCancellationRequested)
            {
                if (traceDirectory.Exists)
                {
                    FileInfo[] tracingFiles = [.. TracingFile.GetTracingFiles(traceDirectory, searchPattern)];
                    if (tracingFiles != null && tracingFiles.Length > MaxFileCountDefault)
                    {
                        int filesToRemove = tracingFiles.Length - MaxFileCountDefault;
                        for (int i = tracingFiles.Length - 1; i > MaxFileCountDefault; i--)
                        {
                            FileInfo? file = tracingFiles.ElementAtOrDefault(i);
                            await TracingFile.ActionWithRetry<IOException>(() => file?.Delete(), cancellationToken: cancellationToken);
                        }
                    }
                }
                else
                {
                    // The folder may have temporarily been removed. Let's keep monitoring in case it returns.
                }
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(delayTimeSeconds), cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    // Need to catch this exception or it will be propogated.
                    break;
                }
            }
            return;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    lock (s_lock)
                    {
                        if (s_listenerCounts.TryGetValue(ListenerId, out int count))
                        {
                            if (count > 1)
                            {
                                s_listenerCounts.TryUpdate(ListenerId, count - 1, count);
                            }
                            else
                            {
                                s_listenerCounts.TryRemove(ListenerId, out _);
                                s_listeners.TryRemove(ListenerId, out ActivityListener? listener);
                                listener?.Dispose();
                                s_cancellationTokenSource?.Cancel();
                                s_cancellationTokenSource?.Dispose();
                                s_cancellationTokenSource = null;
                                s_cleanupTask?.Wait();
                                s_cleanupTask?.Dispose();
                                s_cleanupTask = null;
                            }
                        }
                    }
                }
                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

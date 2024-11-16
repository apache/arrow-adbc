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
using System.Text.Json;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
{
    public class TracingFileListener : ITracingListener
    {
        private bool _disposedValue;
        private readonly string _activitySourceName;
        private readonly string _traceFolderLocation;
        private static readonly ConcurrentDictionary<string, ActivityListener> s_listeners = new();
        private static readonly ConcurrentDictionary<string, int> s_listenerCounts = new();
        private readonly ConcurrentQueue<Activity> _activityQueue = new();
        private readonly TracingFile _tracingFile;
        private readonly object _traceLock = new object();

        public TracingFileListener(string activitySourceName, string traceFolderLocation)
        {
            _activitySourceName = activitySourceName;
            _traceFolderLocation = traceFolderLocation;
            _tracingFile = new TracingFile(activitySourceName, traceFolderLocation);
            lock (_traceLock)
            {
                s_listenerCounts.AddOrUpdate(ListenerId, 1, (k, v) => v + 1);
                ActivityListener = s_listeners.GetOrAdd(ListenerId, (_) => NewActivityListener());
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

        private async Task DequeueAndWrite(string state)
        {
            if (_activityQueue.TryDequeue(out Activity? activity))
            {
                if (activity != null)
                {
                    try
                    {
                        string json = JsonSerializer.Serialize(new { State = state, Activity = activity });
                        await _tracingFile.WriteLine(json);
                    }
                    catch (NotSupportedException ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
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
                    lock (_traceLock)
                    {
                        if (s_listenerCounts.TryGetValue(ListenerId, out int count))
                        {
                            if (count > 1)
                            {
                                s_listenerCounts.TryUpdate(ListenerId, count - 1, count);
                            }
                            else
                            {
                                s_listenerCounts.TryRemove(ListenerId, out count);
                                s_listeners.TryRemove(ListenerId, out ActivityListener? listener);
                                listener?.Dispose();
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

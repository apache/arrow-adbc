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
using System.Reflection;
using System.Text.Json;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    public class SparkDriver : AdbcDriver
    {
        private bool _disposed;
        private readonly ActivitySource _activitySource;
        private static ActivityListener _listener => new()
        {
            ShouldListenTo = (source) => source.Name == ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = OnActivityStarted,
            ActivityStopped = OnActivityStopped,
        };

        static SparkDriver()
        {
            ActivitySource.AddActivityListener(_listener);
        }

        public SparkDriver()
        {
            _activitySource = new(ActivitySourceName, AssemblyVersion);
        }

        public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
        {
            using Activity? activity = StartActivity(nameof(Open));
            if (activity != null)
            {
                ActivityEvent activityEvent = new("opening database", tags: new ActivityTagsCollection([new("parameter-keys", parameters.Keys)]));
                activity?.AddEvent(activityEvent);
            }
            return new SparkDatabase(parameters, _activitySource);
        }

        private static void OnActivityStarted(Activity activity)
        {
            try
            {
                string json = JsonSerializer.Serialize(new { State = "started", Activity = activity });
                Console.WriteLine(json);
            }
            catch (NotSupportedException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static void OnActivityStopped(Activity activity)
        {
            try
            {
                string json = JsonSerializer.Serialize(new { State = "stopped", Activity = activity });
                Console.WriteLine(json);
            }
            catch (NotSupportedException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static string AssemblyVersion => FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location).ProductVersion ?? "1.0.0";

        private static string ActivitySourceName => typeof(SparkDriver).Assembly.GetName().Name!;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    //_listener?.Dispose();
                    _activitySource?.Dispose();
                }
                _disposed = true;
            }
        }

        public override void Dispose()
        {
            Dispose(true);
            base.Dispose();
            GC.SuppressFinalize(this);
        }

        private Activity? StartActivity(string methodName) => _activitySource?.StartActivity(typeof(SparkDriver).FullName + "." + methodName);
    }
}

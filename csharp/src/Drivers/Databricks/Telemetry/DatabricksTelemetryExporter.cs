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

using System.Diagnostics;
using Apache.Arrow.Adbc.Tracing;
using System.IO;
using System;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class DatabricksTelemetryExporter : IDisposable
    {
        // Source name, should be "Apache.Arrow.Adbc.Drivers.Databricks"
        //private readonly string _sourceName = GetType().Assembly.GetName().Name!;
        private readonly string _logFilePath = Path.Combine(Path.GetTempPath(), $"databricks/{DateTime.Now:yyyyMMdd-HHmmss}.log");

        private ActivityListener _activityListener { get; }
        private StreamWriter _logWriter;

        public DatabricksTelemetryExporter(String sourceName)
        {
            this._activityListener = new ActivityListener
            {
                //ShouldListenTo = (activitySource) => activitySource.Name == sourceName,
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStarted = OnActivityStarted,
                ActivityStopped = OnActivityStopped,
            };
            ActivitySource.AddActivityListener(_activityListener);
            _logWriter = new StreamWriter(_logFilePath);
        }

        public void log(string message)
        {
            _logWriter.WriteLine(message);
            _logWriter.WriteLine("hello");
            _logWriter.Flush();
        }

        private void OnActivityStarted(Activity activity)
        {
            _logWriter.WriteLine(activity.OperationName);
            _logWriter.WriteLine(activity.Tags);
            _logWriter.Flush();
        }

        private void OnActivityStopped(Activity activity)
        {
            _logWriter.WriteLine(activity.OperationName);
            _logWriter.WriteLine(activity.Duration);
            _logWriter.Flush();
        }

        private void SendToDatabricks()
        {
            
        }


        public void Dispose()
        {   
            this._activityListener.Dispose();
            _logWriter.Close();
        }
    }
}
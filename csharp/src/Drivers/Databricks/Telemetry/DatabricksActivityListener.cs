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
using System.IO;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;


namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    internal class DatabricksActivityListener : IDisposable
    {
        private readonly ActivityListener _activityListener;
        private TelemetryHelper? _telemetryHelper;

        public DatabricksActivityListener(TelemetryHelper? telemetryHelper, string sourceName)
        {
            this._telemetryHelper = telemetryHelper;
            this._activityListener = new ActivityListener
            {
                ShouldListenTo = (activitySource) => activitySource.Name == sourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = OnActivityStopped,
            };
            ActivitySource.AddActivityListener(_activityListener);
        }

        private void OnActivityStopped(Activity activity)
        {       
            if(_telemetryHelper == null)
            {
                return;
            }

            if(activity.OperationName.EndsWith("ExecuteStatementAsync"))
            {
                var sqlExecutionEvent = new SqlExecutionEvent();
                var operationDetail = new OperationDetail();
                operationDetail.OperationType = Util.StringToOperationType("EXECUTE_STATEMENT_ASYNC");
                sqlExecutionEvent.OperationDetail = operationDetail;
                _telemetryHelper.AddSqlExecutionEvent(sqlExecutionEvent, Convert.ToInt64(activity.Duration.TotalMilliseconds));
            }
        }

        public void Dispose()
        {   
            this._activityListener.Dispose();
        }
    }
}

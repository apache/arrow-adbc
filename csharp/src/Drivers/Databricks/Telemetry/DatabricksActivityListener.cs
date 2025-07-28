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
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;
using System;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class DatabricksActivityListener : IDisposable
    {

        private ActivityListener _activityListener;

        public DatabricksActivityListener()
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
        }

        private void OnActivityStarted(Activity activity)
        {
        }

        private void OnActivityStopped(Activity activity)
        {       
            if(activity.OperationName == "ExecuteStatementAsync")
            {
                var sqlExecutionEvent = new SqlExecutionEvent();
                var operationDetail = new OperationDetail();
                operationDetail.OperationType = Util.StringToOperationType("EXECUTE_STATEMENT_ASYNC");
                sqlExecutionEvent.OperationDetail = operationDetail;
                TelemetryHelper.AddSqlExecutionEvent(sqlExecutionEvent);
            }
            // iterate over the tags and create a telemetry event
            foreach (var tag in activity.Tags)
            {
                // example tag and handling
                if(tag.Key == "sql.query")
                {
                    var sqlExecutionEvent = new SqlExecutionEvent();
                    var operationDetail = new OperationDetail();
                    operationDetail.OperationType = Util.StringToOperationType(tag.Value);
                    sqlExecutionEvent.StatementType = StatementType.QUERY;
                    sqlExecutionEvent.OperationDetail = operationDetail;
                    TelemetryHelper.AddSqlExecutionEvent(sqlExecutionEvent);
                }
            }
        }

        public void Dispose()
        {   
            this._activityListener.Dispose();
        }
    }
}
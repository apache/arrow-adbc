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

namespace Apache.Arrow.Adbc.Tracing
{
    /// <summary>
    /// Provides an implementatio of <see cref="AdbcStatement"/> which can instrument tracing activity.
    /// </summary>
    /// <param name="activitySource">The <see cref="System.Diagnostics.ActivitySource"/> to add trace activity on.</param>
    public abstract class TracingStatement(ActivitySource? activitySource) : AdbcStatement, ITracingObject
    {
        public ActivitySource? ActivitySource => activitySource;

        public abstract string TracingBaseName { get; }

        public Activity? StartActivity(string methodName) => TracingConnection.StartActivity(ActivitySource, TracingBaseName, methodName);

        public  void TraceException(Exception exception, Activity? activity, bool escaped = true) =>
            TracingConnectionImpl.WriteTraceException(exception, activity, escaped);

    }
}

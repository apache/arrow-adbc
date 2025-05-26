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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
{
    public abstract class TracingStatement : AdbcStatement, IActivityTracer
    {
        internal ActivityTrace _trace;

        public TracingStatement(TracingConnection connection) {
            _trace = connection._trace;
        }

        internal string? TraceParent { get; set; }

        public void TraceActivity(Action<Activity?> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            _trace.TraceActivity(call, activityName, traceParent ?? TraceParent);
        }

        public T TraceActivity<T>(Func<Activity?, T> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return TraceActivity(call, activityName, traceParent ?? TraceParent);
        }

        public Task TraceActivityAsync(Func<Activity?, Task> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return _trace.TraceActivityAsync(call, activityName, traceParent ?? TraceParent);
        }

        public Task<T> TraceActivityAsync<T>(Func<Activity?, Task<T>> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return _trace.TraceActivityAsync(call, activityName, traceParent ?? TraceParent);
        }
    }
}

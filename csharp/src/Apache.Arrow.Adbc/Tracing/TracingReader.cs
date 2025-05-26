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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Tracing
{
    public abstract class TracingReader : IArrowArrayStream, IActivityTracer
    {
        private bool _isDisposed;
        private readonly TracingStatement _statement;

        protected TracingReader(TracingStatement statement)
        {
            _statement = statement;
        }

        public abstract Schema Schema { get; }

        public abstract ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default);

        private ActivityTrace Trace => _statement._trace;

        private string? TraceParent => _statement.TraceParent;

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                _isDisposed = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~TracingReader()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public void TraceActivity(Action<Activity?> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            TraceActivity(call, activityName, traceParent ?? TraceParent);
        }

        public T TraceActivity<T>(Func<Activity?, T> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return Trace.TraceActivity(call, activityName, traceParent ?? TraceParent);
        }

        public Task TraceActivityAsync(Func<Activity?, Task> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return Trace.TraceActivityAsync(call, activityName, traceParent ?? TraceParent);
        }

        public Task<T> TraceActivityAsync<T>(Func<Activity?, Task<T>> call, [CallerMemberName] string? activityName = null, string? traceParent = null)
        {
            return Trace.TraceActivityAsync(call, activityName, traceParent ?? TraceParent);
        }
    }
}

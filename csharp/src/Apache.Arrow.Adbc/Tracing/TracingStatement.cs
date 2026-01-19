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

namespace Apache.Arrow.Adbc.Tracing
{
    public abstract class TracingStatement : AdbcStatement, ITracingStatement
    {
        private readonly ActivityTrace _trace;
        private string? _statementTraceParent;

        public TracingStatement(TracingConnection connection)
        {
            _trace = ((IActivityTracer)connection).Trace;
        }

        ActivityTrace IActivityTracer.Trace => _trace;

        string? IActivityTracer.TraceParent => _statementTraceParent ?? _trace.TraceParent;

        protected void SetTraceParent(string? traceParent)
        {
            _statementTraceParent = traceParent;
        }

        public abstract string AssemblyVersion { get; }

        public abstract string AssemblyName { get; }

        protected virtual void Dispose(bool disposing)
        {
        }

        public override void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

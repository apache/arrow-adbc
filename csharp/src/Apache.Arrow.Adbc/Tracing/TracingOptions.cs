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

namespace Apache.Arrow.Adbc.Tracing
{
    public static class TracingOptions
    {
        public const string Enabled = "true";
        public const string Disabled = "false";

        public static class Connection
        {
            /// <summary>
            /// The name of the canonical option for enabling tracing into a file. Default is <c>false</c>.
            /// </summary>
            public const string Trace = "adbc.tracing.connection.trace";

            /// <summary>
            /// The name of the canonical option for indicating the folder location to write tracing files. Default is the current user's home folder (<c>~</c>).
            /// </summary>
            public const string TraceLocation = "adbc.tracing.connection.trace_location";

            /// <summary>
            /// The name of the canonical option for indicating the maximum size of a single tracing file. Default is 1024.
            /// </summary>
            public const string TraceFileMaxSizeKb = "adbc.tracing.connection.trace_max_size_kb";

            /// <summary>
            /// The name of the canonical option for indicating the maximum number of trace files to keep before roll-over. Default is 999.
            /// </summary>
            public const string TraceFileMaxFiles = "adbc.tracing.connection.trace_max_files";
        }

        public static class Statement
        {
            /// <summary>
            /// The name of the canonical option for the hex-string for the 16-byte trace id for the parent context. Must be combined with the <see cref="ParentId"/> option.
            /// See also <see href="https://www.w3.org/TR/trace-context/#trace-id">Trace Context</see>
            /// </summary>
            public const string TraceId = "adbc.tracing.statement.trace_id";

            /// <summary>
            /// The name of the canonical option for the hex-string for the 8-byte parent id for the parent context. Must be combined with the <see cref="TraceId"/> option.
            /// See also <see href="https://www.w3.org/TR/trace-context/#parent-id">Trace Context</see>
            /// </summary>
            public const string ParentId = "adbc.tracing.statement.parent_id";
        }
    }
}

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

namespace Apache.Arrow.Adbc.Telemetry.Traces.Exporters.FileExporter
{
    /// <summary>
    /// The options an <see cref="FileExporter"/> uses to trace active to files.
    /// </summary>
    public class FileExporterOptions
    {
        /// <summary>
        /// Gets or sets the base file name (typically the tracing source name).
        /// Trace files will be created with the following name template: {fileBaseName}-trace-{dateTime}.log
        /// </summary>
        public string FileBaseName { get; set; } = FileExporter.ApacheArrowAdbcNamespace;

        /// <summary>
        /// The full or partial path to a folder where the trace files will be written.
        /// If the folder doesn not exist, it will be created.
        /// </summary>
        public string? TraceLocation { get; set; } = FileExporter.TracingLocationDefault;

        /// <summary>
        /// The maximum size of each trace file (in KB). If a trace file exceeds this limit, a new trace file is created.
        /// </summary>
        public long MaxTraceFileSizeKb { get; set; } = FileExporter.MaxFileSizeKbDefault;

        /// <summary>
        /// The maximum number of trace files in the tracing folder. If the number of files exceeds this maximum, older files will be removed.
        /// </summary>
        public int MaxTraceFiles { get; set; } = FileExporter.MaxTraceFilesDefault;
    }
}

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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Exporters.FileExporter
{
    public static class FileExporterExtensions
    {
        /// <summary>
        /// Adds an ADBC file exporter to listen for and write trace entries into files.
        /// </summary>
        /// <param name="builder">
        /// The <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> to add to.
        /// Ensure to call <see cref="OpenTelemetry.Trace.TracerProviderBuilder.AddSource"/> before calling this method.
        /// </param>
        /// <returns>The previous <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> with the exporter added.</returns>
        /// <remarks>
        /// Note that only one global instance of the exporter is added.
        /// If there is already an existing exporter for the same source and location, no new one is added.
        /// </remarks>
        public static OpenTelemetry.Trace.TracerProviderBuilder AddAdbcFileExporter(this OpenTelemetry.Trace.TracerProviderBuilder builder)
            => builder.AddAdbcFileExporter(null, null);

        /// <summary>
        /// Adds an ADBC file exporter to listen for and write trace entries into files.
        /// </summary>
        /// <param name="builder">
        /// The <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> to add to.
        /// Ensure to call <see cref="OpenTelemetry.Trace.TracerProviderBuilder.AddSource"/> before calling this method.
        /// </param>
        /// <param name="configure">The configuratio action to set the <see cref="FileExporterOptions"/> with.</param>
        /// <returns>The previous <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> with the exporter added.</returns>
        /// <remarks>
        /// Note that only one global instance of the exporter is added.
        /// If there is already an existing exporter for the same source and location, no new one is added.
        /// </remarks>
        public static OpenTelemetry.Trace.TracerProviderBuilder AddAdbcFileExporter(this OpenTelemetry.Trace.TracerProviderBuilder builder, Action<FileExporterOptions>? configure)
           => builder.AddAdbcFileExporter(null, configure);

        /// <summary>
        /// Adds an ADBC file exporter to listen for and write trace entries into files.
        /// </summary>
        /// <param name="builder">
        /// The <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> to add to.
        /// Ensure to call <see cref="OpenTelemetry.Trace.TracerProviderBuilder.AddSource"/> before calling this method.
        /// </param>
        /// <param name="name">The name of this configuration.</param>
        /// <param name="configure">The configuratio action to set the <see cref="FileExporterOptions"/> with.</param>
        /// <returns>The previous <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> with the exporter added.</returns>
        /// <remarks>
        /// Note that only one global instance of the exporter is added.
        /// If there is already an existing exporter for the same source and location, no new one is added.
        /// </remarks>
        public static OpenTelemetry.Trace.TracerProviderBuilder AddAdbcFileExporter(
            this OpenTelemetry.Trace.TracerProviderBuilder builder,
            string? name,
            Action<FileExporterOptions>? configure)
        {
            name ??= Options.DefaultName;

            if (configure != null)
            {
                builder.ConfigureServices(services => services.Configure(name, configure));
            }

            FileExporterOptions options = new();
            configure?.Invoke(options);
            if (FileExporter.TryCreate(options, out FileExporter? fileExporter))
            {
                // Only add a new processor if there isn't already one listening for the source/location.
                return builder.AddProcessor(_ => new BatchActivityExportProcessor(fileExporter!));
            }
            return builder;
        }

        /// <summary>
        /// Adds an ADBC file exporter to listen for and write trace entries into files.
        /// </summary>
        /// <param name="builder">
        /// The <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> to add to.
        /// Ensure to call <see cref="OpenTelemetry.Trace.TracerProviderBuilder.AddSource"/> before calling this method.
        /// </param>
        /// <param name="fileBaseName">
        /// The base file name (typically the tracing source name).
        /// Trace files will be created with the following name template: {fileBaseName}-trace-{dateTime}.log
        /// </param>
        /// <param name="traceLocation">
        /// The full or partial path to a folder where the trace files will be written.
        /// If the folder doesn not exist, it will be created.
        /// </param>
        /// <param name="maxTraceFileSizeKb">The maximum size of each trace file (in KB). If a trace file exceeds this limit, a new trace file is created.</param>
        /// <param name="maxTraceFiles">The maximum number of trace files in the tracing folder. If the number of files exceeds this maximum, older files will be removed.</param>
        /// <returns>The previous <see cref="OpenTelemetry.Trace.TracerProviderBuilder"/> with the exporter added.</returns>
        /// <remarks>
        /// Note that only one global instance of the exporter is added.
        /// If there is already an existing exporter for the same source and location, no new one is added.
        /// </remarks>
        public static OpenTelemetry.Trace.TracerProviderBuilder AddAdbcFileExporter(
            this OpenTelemetry.Trace.TracerProviderBuilder builder,
            string fileBaseName,
            string? traceLocation = default,
            long? maxTraceFileSizeKb = default,
            int? maxTraceFiles = default)
        {
            maxTraceFileSizeKb ??= FileExporter.MaxFileSizeKbDefault;
            maxTraceFiles ??= FileExporter.MaxTraceFilesDefault;
            traceLocation ??= FileExporter.TracingLocationDefault;
            FileExporter.ValidateParameters(fileBaseName, traceLocation, maxTraceFileSizeKb.Value, maxTraceFiles.Value);

            if (FileExporter.TryCreate(fileBaseName, traceLocation, maxTraceFileSizeKb.Value, maxTraceFiles.Value, out FileExporter? fileExporter))
            {
                // Only add a new processor if there isn't already one listening for the source/location.
                return builder.AddProcessor(_ => new SimpleActivityExportProcessor(fileExporter!));
            }
            return builder;
        }
    }
}

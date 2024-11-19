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
using OpenTelemetry;
using OpenTelemetry.Trace;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Apache.Arrow.Adbc.Tracing
{
    public static class FileExporterExtensions
    {
        public static TracerProviderBuilder AddFileExporter(this TracerProviderBuilder builder,Action<FileExporterOptions>? configure = default)
           => builder.AddFileExporter(null, configure);

        public static TracerProviderBuilder AddFileExporter(
            this TracerProviderBuilder builder,
            string? name = default,
            Action<FileExporterOptions>? configure = default)
        {
            name ??= Options.DefaultName;

            if (configure != null)
            {
                builder.ConfigureServices(services => services.Configure(name, configure));
            }

            FileExporterOptions options = new();
            configure?.Invoke(options);
            if (FileExporter.TryCreate(out FileExporter? fileExporter, options))
            {
                return builder.AddProcessor(_ => new SimpleActivityExportProcessor(fileExporter!));
            }
            return builder;
        }

        public static TracerProviderBuilder AddFileExporter(
            this TracerProviderBuilder builder,
            string fileBaseName,
            string? traceFolderLocation = default,
            long maxTraceFileSizeKb = FileExporter.MaxFileSizeKbDefault,
            int maxTraceFiles = FileExporter.MaxTraceFilesDefault)
        {
            if (FileExporter.TryCreate(out FileExporter? fileExporter, fileBaseName, traceFolderLocation, maxTraceFileSizeKb, maxTraceFiles))
            {
                return builder.AddProcessor(_ => new SimpleActivityExportProcessor(fileExporter!));
            }
            return builder;
        }
    }
}

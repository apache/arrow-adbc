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
using System.Reflection;
using Apache.Arrow.Adbc.Tracing.FileExporter;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Apache.Arrow.Adbc.Tracing
{
    public abstract class TracingConnection : AdbcConnection, IActivityTracer
    {
        private const string SourceNameDefault = "apache.arrow.adbc";
        private readonly ActivityTrace _trace;
        private readonly TracerProvider? _tracerProvider;
        private bool _isDisposed;

        protected TracingConnection(IReadOnlyDictionary<string, string> properties)
        {
            properties.TryGetValue(AdbcOptions.Telemetry.TraceParent, out string? traceParent);
            properties.TryGetValue(AdbcOptions.Telemetry.Exporter, out string? tracesExporter);
            _tracerProvider = InitTracerProvider(
                GetType().Assembly,
                tracesExporter,
                out string activitySourceName,
                out _);
            _trace = new ActivityTrace(this.GetAssemblyName(), this.GetAssemblyVersion(), traceParent);
        }

        string? IActivityTracer.TraceParent => _trace.TraceParent;

        ActivityTrace IActivityTracer.Trace => _trace;

        protected void SetTraceParent(string? traceParent)
        {
            _trace.TraceParent = traceParent;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed && disposing)
            {
                if (_tracerProvider != null)
                {
                    _tracerProvider.Dispose();
                }
                _isDisposed = true;
            }
        }

        // Note: if base class implements this code, remove this override
        public override void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        private static TracerProvider? InitTracerProvider(Assembly executingAssembly, string? tracesExporter, out string activitySourceName, out string activitySourceVersion)
        {
            AssemblyName assemblyName = executingAssembly.GetName();
            string sourceName = (assemblyName.Name ?? SourceNameDefault).ToLowerInvariant();
            string sourceVersion = (assemblyName.Version?.ToString() ?? ActivityTrace.ProductVersionDefault).ToLowerInvariant();
            activitySourceName = sourceName;
            activitySourceVersion = sourceVersion;

            tracesExporter ??= Environment.GetEnvironmentVariable(AdbcOptions.Telemetry.Environment.Exporter);
            tracesExporter = tracesExporter?.ToLowerInvariant();
            return tracesExporter switch
            {
                null or "" or AdbcOptions.Telemetry.TraceExporters.None =>
                    null, // Do not create a listener/exporter
                AdbcOptions.Telemetry.TraceExporters.Otlp =>
                    Sdk.CreateTracerProviderBuilder()
                        .AddSource(sourceName)
                        .ConfigureResource(resource =>
                            resource.AddService(
                                serviceName: sourceName,
                                serviceVersion: sourceVersion))
                        .AddOtlpExporter()
                        .Build(),
                AdbcOptions.Telemetry.TraceExporters.Console =>
                    Sdk.CreateTracerProviderBuilder()
                        .AddSource(sourceName)
                        .ConfigureResource(resource =>
                            resource.AddService(
                                serviceName: sourceName,
                                serviceVersion: sourceVersion))
                        .AddConsoleExporter()
                        .Build(),
                AdbcOptions.Telemetry.TraceExporters.AdbcFile =>
                    Sdk.CreateTracerProviderBuilder()
                        .AddSource(sourceName)
                        .ConfigureResource(resource =>
                            resource.AddService(
                                serviceName: sourceName,
                                serviceVersion: sourceVersion))
                        .AddAdbcFileExporter(sourceName)
                        .Build(),
                _ => throw new AdbcException(
                        $"Unsupported {AdbcOptions.Telemetry.Environment.Exporter} option: '{tracesExporter}'",
                        AdbcStatusCode.InvalidArgument),
            };
        }
    }
}

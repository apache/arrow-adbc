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
using Apache.Arrow.Adbc.Telemetry.Traces.Exporters.FileExporter;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Exporters
{
    public class ExportersBuilder
    {
        private static readonly IReadOnlyDictionary<string, Func<string, string?, TracerProvider?>> s_tracerProviderFactoriesDefault;

        private readonly string _sourceName;
        private readonly string? _sourceVersion;
        private readonly IReadOnlyDictionary<string, Func<string, string?, TracerProvider?>> _tracerProviderFactories;

        static ExportersBuilder()
        {
            var defaultProviders = new Dictionary<string, Func<string, string?, TracerProvider?>>
            {
                [ExportersOptions.Exporters.None] = NewNoopTracerProvider,
                [ExportersOptions.Exporters.Otlp] = NewOtlpTracerProvider,
                [ExportersOptions.Exporters.Console] = NewConsoleTracerProvider,
                [ExportersOptions.Exporters.AdbcFile] = NewAdbcFileTracerProvider,
            };
            s_tracerProviderFactoriesDefault = defaultProviders;
        }

        private ExportersBuilder(Builder builder)
        {
            _sourceName = builder.SourceName;
            _sourceVersion = builder.SourceVersion;
            _tracerProviderFactories = builder.TracerProviderFactories;
        }

        /// <summary>
        /// Build an <see cref="ExportersBuilder"/> from different possible Exporters. Use the Add* functions to add
        /// one or more <see cref="TracerProvider"/> factories.
        /// </summary>
        /// <param name="sourceName">The name of the source that the exporter will filter on.</param>
        /// <param name="sourceVersion">The (optional) version of the source that the exporter will filter on.</param>
        /// <returns>A <see cref="Builder"/> object.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static Builder Build(string sourceName, string? sourceVersion = default, bool addDefaultExporters = false)
        {
            if (string.IsNullOrWhiteSpace(sourceName))
            {
                throw new ArgumentNullException(nameof(sourceName));
            }
            return new Builder(sourceName, sourceVersion, addDefaultExporters);
        }

        /// <summary>
        /// <para>
        /// Attempts to activate an exporter based on the dictionary of <see cref="TracerProvider"/> factories
        /// added to the <see cref="Builder"/>. If the value of exporterOption is not null and not empty, it will be
        /// used as the key to the dictionary. If exporterOption is null or empty, then the exporter option will
        /// check the environment variable identified by environmentName (default <see cref="ExportersOptions.Environment.Exporter"/>).
        /// If both the exporterOption and the environment variable value are null or empty, then this function will return null
        /// and no exporeter will be activated.
        /// </para>
        /// <para>
        /// If the exporterOption or the value of the environment variable are not null and not empty, then
        /// if a matching factory delegate is found, it is called to activate the exporter. If no exception is thrown,
        /// the result of the factory method returns the result which may be null. If a matching function is found, the expoertName is set.
        /// If the factory delegate throws an exception it is not caught.
        /// </para>
        /// </summary>
        /// <param name="exporterOption">The value (name) of the exporter option, typically passed as option <see cref="ExportersOptions.Exporter"/>.</param>
        /// <param name="exporterName">The actual exporter name when successfully activated.</param>
        /// <param name="environmentName">The (optional) name of the environment variable to test for the exporter name. Default: <see cref="ExportersOptions.Environment.Exporter"/></param>
        /// <exception cref="AdbcException" />
        /// <returns>The a non-null <see cref="TracerProvider"/> when successfully activated. Note: this object must be explicitly disposed when no longer necessary.</returns>
        public TracerProvider? Activate(
            string? exporterOption,
            out string? exporterName,
            string environmentName = ExportersOptions.Environment.Exporter)
        {
            TracerProvider? tracerProvider = null;
            exporterName = null;

            if (string.IsNullOrWhiteSpace(exporterOption))
            {
                // Fall back to check the environment variable
                exporterOption = Environment.GetEnvironmentVariable(environmentName);
            }
            if (string.IsNullOrWhiteSpace(exporterOption))
            {
                // Neither option or environment variable is set - no tracer provider will be activated.
                return null;
            }

            if (!_tracerProviderFactories.TryGetValue(exporterOption!, out Func<string, string?, TracerProvider?>? factory))
            {
                // Requested option has not been added via the builder
                throw AdbcException.NotImplemented($"Exporter option '{exporterOption}' is not implemented.");
            }

            tracerProvider = factory.Invoke(_sourceName, _sourceVersion);
            if (tracerProvider != null)
            {
                exporterName = exporterOption;
            }
            return tracerProvider;
        }

        public static TracerProvider NewAdbcFileTracerProvider(string sourceName, string? sourceVersion) =>
            Sdk.CreateTracerProviderBuilder()
                .AddSource(sourceName)
                .ConfigureResource(resource =>
                    resource.AddService(
                        serviceName: sourceName,
                        serviceVersion: sourceVersion))
                .AddAdbcFileExporter(sourceName)
                .Build();

        public static TracerProvider NewConsoleTracerProvider(string sourceName, string? sourceVersion) =>
            Sdk.CreateTracerProviderBuilder()
                .AddSource(sourceName)
                .ConfigureResource(resource =>
                    resource.AddService(
                        serviceName: sourceName,
                        serviceVersion: sourceVersion))
                .AddConsoleExporter()
                .Build();

        public static TracerProvider NewOtlpTracerProvider(string sourceName, string? sourceVersion) =>
            Sdk.CreateTracerProviderBuilder()
                .AddSource(sourceName)
                .ConfigureResource(resource =>
                    resource.AddService(
                        serviceName: sourceName,
                        serviceVersion: sourceVersion))
                .AddOtlpExporter()
                .Build();

        public static TracerProvider? NewNoopTracerProvider(string sourceName, string? sourceVersion) =>
            null;

        public class Builder
        {
            private readonly string _sourceName;
            private readonly string? _sourceVersion;
            private readonly Dictionary<string, Func<string, string?, TracerProvider?>> _tracerProviderFactories;

            internal string SourceName => _sourceName;

            internal string? SourceVersion => _sourceVersion;

            internal IReadOnlyDictionary<string, Func<string, string?, TracerProvider?>> TracerProviderFactories => _tracerProviderFactories;

            public Builder(string sourceName, string? sourceVersion, bool addDefaultExporters = false)
            {
                _sourceName = sourceName;
                _sourceVersion = sourceVersion;
                _tracerProviderFactories = [];
                if (addDefaultExporters)
                {
                    AddDefaultExporters();
                }
            }

            public ExportersBuilder Build()
            {
                if (_tracerProviderFactories.Count == 0)
                {
                    throw new InvalidOperationException("No options provided. Please add one or more exporter.");
                }
                return new ExportersBuilder(this);
            }

            public Builder AddExporter(string exporterName, Func<string, string?, TracerProvider?> tracerProviderFactory)
            {
                if (string.IsNullOrWhiteSpace(exporterName))
                {
                    throw new ArgumentNullException(nameof(exporterName));
                }

                _tracerProviderFactories.Add(exporterName, tracerProviderFactory);
                return this;
            }

            private Builder AddDefaultExporters()
            {
                foreach (KeyValuePair<string, Func<string, string?, TracerProvider?>> item in s_tracerProviderFactoriesDefault)
                {
                    _tracerProviderFactories[item.Key] = item.Value;
                }
                return this;
            }
        }
    }
}

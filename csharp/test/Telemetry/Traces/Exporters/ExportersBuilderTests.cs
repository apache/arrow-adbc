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
using Apache.Arrow.Adbc.Telemetry.Traces.Exporters;
using OpenTelemetry.Trace;

namespace Apache.Arrow.Adbc.Tests.Telemetry.Traces.Exporters
{
    public class ExportersBuilderTests
    {
        [Theory]
        [ClassData(typeof(ActivatorOptionData))]
        public void CanActivateUsingOption(string sourceName, string? exporterOption, bool willActivate, Type? exceptionType = default)
        {
            string environmentName = NewName();
            Environment.SetEnvironmentVariable(environmentName, null);
            if (exceptionType == null)
            {
                using TracerProvider? tracerProvider = ExportersBuilder.Build(sourceName, addDefaultExporters: true)
                    .Build()
                    .Activate(exporterOption, out string? exporterName, environmentName);
                if (willActivate)
                {
                    Assert.NotNull(exporterName);
                    Assert.NotNull(tracerProvider);
                }
                else
                {
                    Assert.Null(tracerProvider);
                }
            }
            else
            {
                Assert.Throws(exceptionType, () => ExportersBuilder.Build(sourceName, addDefaultExporters: true)
                    .Build()
                    .Activate(exporterOption, out string? exporterName, environmentName));
            }
        }

        [Fact]
        public void CanDetectMissingExporters()
        {
            Assert.Throws<InvalidOperationException>(() => ExportersBuilder.Build("sourceName")
                .Build()
                .Activate(ExportersOptions.Exporters.Console, out string? exporterName));
        }

        [Theory]
        [ClassData(typeof(ActivatorOptionData))]
        public void CanActivateUsingEnvironment(string sourceName, string? exporterOption, bool activateExpected, Type? exceptionType = default)
        {
            string environmentName = NewName();
            Environment.SetEnvironmentVariable(environmentName, exporterOption);
            if (exceptionType == null)
            {
                using TracerProvider? tracerProvider = ExportersBuilder.Build(sourceName, addDefaultExporters: true)
                    .Build()
                    .Activate(null, out string? exporterName, environmentName);
                if (activateExpected)
                {
                    Assert.NotNull(exporterName);
                    Assert.NotNull(tracerProvider);
                }
                else
                {
                    Assert.Null(tracerProvider);
                }
            }
            else
            {
                Assert.Throws(exceptionType, () => ExportersBuilder.Build(sourceName, addDefaultExporters: true)
                    .Build()
                    .Activate(null, out string? exporterName, environmentName));
            }
        }

        [Fact]
        public void CanAddExporters()
        {
            // Test single
            string exporterOption = NewName();
            using (TracerProvider? tracerProvider = ExportersBuilder.Build("sourceName")
                .AddExporter(exporterOption, ExportersBuilder.NewConsoleTracerProvider)
                .Build()
                .Activate(exporterOption, out string? exporterName))
            {
                Assert.NotNull(tracerProvider);
                Assert.NotNull(exporterName);
                Assert.Equal(exporterOption, exporterName);
            }

            // Test with other default exporters
            exporterOption = NewName();
            using (TracerProvider? tracerProvider = ExportersBuilder.Build("sourceName", addDefaultExporters: true)
                .AddExporter(exporterOption, ExportersBuilder.NewConsoleTracerProvider)
                .Build()
                .Activate(exporterOption, out string? exporterName))
            {
                Assert.NotNull(tracerProvider);
                Assert.NotNull(exporterName);
                Assert.Equal(exporterOption, exporterName);
            }
        }

        public class ActivatorOptionData : TheoryData<string?, string?, bool, Type?>
        {
            public ActivatorOptionData()
            {
                Add("a", null, false, null);
                Add("a", "", false, null);
                Add("a", "  ", false, null);
                Add("a", ExportersOptions.Exporters.None, false, null);
                Add("a", ExportersOptions.Exporters.Otlp, true, null);
                Add("a", ExportersOptions.Exporters.Console, true, null);
                Add("a", ExportersOptions.Exporters.AdbcFile, true, null);
                Add("a", "unknown", false, typeof(AdbcException));
                Add(null, ExportersOptions.Exporters.Console, false, typeof(ArgumentNullException));
                Add("", ExportersOptions.Exporters.Console, false, typeof(ArgumentNullException));
                Add("  ", ExportersOptions.Exporters.Console, false, typeof(ArgumentNullException));
            }
        }

        internal static string NewName() => Guid.NewGuid().ToString("N");
    }
}

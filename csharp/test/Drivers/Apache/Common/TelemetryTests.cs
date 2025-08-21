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
using System.IO;
using System.Linq;
using Apache.Arrow.Adbc.Telemetry.Traces.Exporters;
using Apache.Arrow.Adbc.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    public abstract class TelemetryTests<TConfig, TEnv> : TestBase<TConfig, TEnv>
        where TConfig : TestConfiguration
        where TEnv : CommonTestEnvironment<TConfig>
    {
        public TelemetryTests(ITestOutputHelper output, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
            : base(output, testEnvFactory) { }

        /// <summary>
        /// Note: Once an exporter is loaded, it is "sticky". So we can't reliably test the difference between loaded and unloaded
        /// in a single (or even multiople) test. This just tests the ability to load the file exporter and that it produces at least one file.
        /// </summary>
        /// <param name="exporterName"></param>
        [SkippableTheory]
        [InlineData(ExportersOptions.Exporters.AdbcFile)]
        //[InlineData(null)]
        //[InlineData(ExportersOptions.Exporters.None)]
        public void CanEnableFileTracingExporterViaEnvVariable(string? exporterName)
        {
            Environment.SetEnvironmentVariable(ExportersOptions.Environment.Exporter, exporterName);

            DirectoryInfo directoryInfo = GetTracesDirectoryInfo();
            ResetTraceDirectory(directoryInfo);

            directoryInfo.Refresh();
            IEnumerable<FileInfo> files = directoryInfo.EnumerateFiles();
            Assert.Empty(files);
            string activitySourceName = string.Empty;

            try
            {
                Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);
                using (AdbcDatabase database = NewDriver.Open(parameters))
                {
                    try
                    {
                        using AdbcConnection connection = database.Connect(new Dictionary<string, string>());
                        TracingConnection? tc = connection as TracingConnection;
                        Assert.NotNull(tc);
                        Assert.True(string.IsNullOrEmpty(tc.ActivitySourceName), "expecting non-empty ActivitySourceName");
                        activitySourceName = tc.ActivitySourceName;
                    }
                    catch (Exception ex)
                    {
                        // We don't really need the connection to succeed for this test,
                        OutputHelper?.WriteLine(ex.Message);
                    }
                }

                directoryInfo.Refresh();
                files = directoryInfo.EnumerateFiles();
                switch (exporterName)
                {
                    case ExportersOptions.Exporters.AdbcFile:
                        Assert.NotEmpty(files);
                        Assert.NotEqual(0, files.First().Length);
                        Assert.StartsWith(activitySourceName, files.First().Name);
                        break;
                    default:
                        Assert.Empty(files);
                        break;
                }
            }
            finally
            {
                ResetTraceDirectory(directoryInfo, create: false);
            }
        }

        private static void ResetTraceDirectory(DirectoryInfo directoryInfo, bool create = true)
        {
            if (directoryInfo.Exists)
            {
                directoryInfo.Delete(recursive: true);
            }
            if (create)
            {
                directoryInfo.Create();
            }
        }

        private static DirectoryInfo GetTracesDirectoryInfo() =>
            new DirectoryInfo(Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "Apache.Arrow.Adbc",
                "Traces"));
    }
}

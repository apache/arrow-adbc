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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests
{
    public class TracingFileExporterTests : IDisposable
    {
        private readonly ITestOutputHelper? _outputHelper;
        private bool _disposed;
        private readonly string _activitySourceName;
        private readonly ActivitySource _activitySource;
        private static readonly string s_localApplicationDataFolderPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);

        public TracingFileExporterTests(ITestOutputHelper? outputHelper)
        {
            _outputHelper = outputHelper;
            _activitySourceName = Guid.NewGuid().ToString().Replace("-", "");
            _activitySource = new ActivitySource(_activitySourceName);
        }

        [Fact]
        internal async Task CanSetCustomTraceFolder()
        {
            string customFolderName = Guid.NewGuid().ToString().Replace("-", "").ToLower();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder)
                    .Build();

                await AddEvent("test");

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.Single(files);
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetCustomFileBaseName()
        {
            const string customFileBaseName = "custom-base-name";
            string customFolderName = Guid.NewGuid().ToString().Replace("-", "").ToLower();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(customFileBaseName, traceFolder)
                    .Build();

                await AddEvent("test");

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.Single(files);
                Assert.StartsWith(customFileBaseName, files[0].Name);
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetCustomMaxFileSize()
        {
            const long maxTraceFileSizeKb = 5;
            const long kilobyte = 1024;
            string customFolderName = Guid.NewGuid().ToString().Replace("-", "").ToLower();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder, maxTraceFileSizeKb)
                    .Build();

                for (int i = 0; i < 100; i++) await AddEvent("test");

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.True(files.Length > 2, $"actual # of trace files: {files.Length}");
                Assert.True(files.All(f => f.Name.StartsWith(_activitySourceName)));
                for (int i = 0; i < files.Length; i++)
                {
                    Assert.True(files[i].Length < (maxTraceFileSizeKb + 2) * kilobyte, $"actual file length: {files[i].Length}");
                }
                _outputHelper?.WriteLine($"number of files: {files.Length}");
                Console.WriteLine($"number of files: {files.Length}");
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetCustomMaxFiles()
        {
            const long maxTraceFileSizeKb = 5;
            const int maxTraceFiles = 3;
            const int delayMs = 5000;
            string customFolderName = Guid.NewGuid().ToString().Replace("-", "").ToLower();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder, maxTraceFileSizeKb, maxTraceFiles)
                    .Build();

                for (int i = 0; i < 100; i++) await AddEvent("test");

                // Wait for clean-up task to poll and clean-up
                await Task.Delay(delayMs);

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.True(files.Length > 2, $"actual # of trace files: {files.Length}");
                Assert.True(files.Length <= maxTraceFiles, $"actual # of files: {files.Length}");
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetSingleMaxFiles()
        {
            const long maxTraceFileSizeKb = 5;
            const int maxTraceFiles = 1;
            const int delayMs = 5000;
            string customFolderName = Guid.NewGuid().ToString().Replace("-", "").ToLower();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder, maxTraceFileSizeKb, maxTraceFiles)
                    .Build();

                for (int i = 0; i < 100; i++) await AddEvent("test");

                // Wait for clean-up task to poll and clean-up
                await Task.Delay(delayMs);

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.Single(files);
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Theory]
        [InlineData("", "abc", 1, 1, typeof(ArgumentNullException))]
        [InlineData(" ", "abc", 1, 1, typeof(ArgumentNullException))]
        [InlineData("|", "abc", 1, 1, typeof(ArgumentException))]
        [InlineData("abc", "abc", 0, 1, typeof(ArgumentException))]
        [InlineData("abc", "abc", -1, 1, typeof(ArgumentException))]
        [InlineData("abc", "abc", 1, 0, typeof(ArgumentException))]
        [InlineData("abc", "abc", 1, -1, typeof(ArgumentException))]
        internal void CanDetectInvalidOptions(string fileBaseName, string? traceLocation, long maxTraceFileSizeKb, int maxTraceFiles, Type expectedException)
        {
            string customFolderName = Guid.NewGuid().ToString().Replace("-", "").ToLower();
            string? traceFolder = traceLocation != null ? Path.Combine(s_localApplicationDataFolderPath, traceLocation) : null;
                _ = Assert.Throws(expectedException, () =>
                    Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(fileBaseName, traceFolder, maxTraceFileSizeKb, maxTraceFiles)
                    .Build());
        }

        private Task AddEvent(string eventName, string activityName = nameof(AddEvent))
        {
            using Activity? activity = _activitySource.StartActivity(activityName);
            activity?.AddEvent(new ActivityEvent(eventName));
            return Task.CompletedTask;
        }

        private Task StartActivity(string activityName = nameof(StartActivity))
        {
            using Activity? activity = _activitySource.StartActivity(activityName);
            return Task.CompletedTask;

        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _activitySource.Dispose();
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

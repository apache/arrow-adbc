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
using System.Text;
using System.Text.Json;
using Apache.Arrow.Adbc.Tracing;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Tracing
{
    public class TracingTests : IDisposable
    {
        private readonly ITestOutputHelper? _outputHelper;
        private bool _disposed;
        private readonly string _activitySourceName;
        private readonly MemoryStream _stream;

        public TracingTests(ITestOutputHelper? outputHelper)
        {
            _outputHelper = outputHelper;
            _activitySourceName = Guid.NewGuid().ToString().Replace("-", "");
            _stream = new MemoryStream();
        }

        [Fact]
        internal void CanStartActivity()
        {
            MemoryStream stream = _stream;
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(_activitySourceName)
                .AddTestMemoryExporter(_stream)
                .Build();

            var testClass = new TraceInheritor(_activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Equal(0, stream.Length);

            testClass.MethodWithActivity();
            Assert.True(stream.Length > 0);
            long currLength = stream.Length;

            testClass.MethodWithNoInstrumentation();
            Assert.Equal(currLength, stream.Length);

            stream.Seek(0, SeekOrigin.Begin);
            StreamReader reader = new(stream);

            int lineCount = 0;
            string? text = reader.ReadLine();
            while (text != null)
            {
                lineCount++;
                Assert.Contains("MethodWithActivity", text);
                Assert.DoesNotContain("MethodWithNoInstrumentation", text);
                var activity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.NotNull(activity);
                text = reader.ReadLine();
            }
            Assert.Equal(1, lineCount);
        }

        [Fact]
        internal void CanAddEvent()
        {
            MemoryStream stream = _stream;
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(_activitySourceName)
                .AddTestMemoryExporter(_stream)
                .Build();

            var testClass = new TraceInheritor(_activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Equal(0, stream.Length);

            testClass.MethodWithEvent("eventName");
            Assert.True(stream.Length > 0);
            long currLength = stream.Length;

            testClass.MethodWithNoInstrumentation();
            Assert.Equal(currLength, stream.Length);

            stream.Seek(0, SeekOrigin.Begin);
            StreamReader reader = new(stream);

            int lineCount = 0;
            string? text = reader.ReadLine();
            while (text != null)
            {
                lineCount++;
                Assert.Contains("MethodWithEvent", text);
                Assert.DoesNotContain("MethodWithNoInstrumentation", text);
                Assert.Contains("eventName", text);
                var activity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.NotNull(activity);
                text = reader.ReadLine();
            }
            Assert.Equal(1, lineCount);
        }

        [Fact]
        internal void CanAddActivityWithDepth()
        {
            MemoryStream stream = _stream;
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(_activitySourceName)
                .AddTestMemoryExporter(_stream)
                .Build();

            var testClass = new TraceInheritor(_activitySourceName);
            const int recurseCount = 5;
            testClass.MethodWithActivityRecursive("MethodWithActivityRecursive", recurseCount);

            stream.Seek(0, SeekOrigin.Begin);
            StreamReader reader = new(stream);

            int lineCount = 0;
            string? text = reader.ReadLine();
            while (text != null)
            {
                if (string.IsNullOrWhiteSpace(text)) continue;
                lineCount++;
                Assert.Contains("MethodWithActivityRecursive", text);
                Assert.DoesNotContain("MethodWithNoInstrumentation", text);
                var activity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.Contains("MethodWithActivityRecursive", activity?.OperationName);
                Assert.NotNull(activity);
                text = reader.ReadLine();
            }
            Assert.Equal(recurseCount, lineCount);
        }

        [Fact]
        internal void CanAddTraceParent()
        {
            MemoryStream stream = _stream;
            stream.SetLength(0);
            using TracerProvider provider1 = Sdk.CreateTracerProviderBuilder()
                .AddSource(_activitySourceName)
                .AddTestMemoryExporter(_stream)
                .Build();

            var testClass = new TraceInheritor(_activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Equal(0, stream.Length);

            const string eventNameWithoutParent = "eventNameWithoutParent";
            testClass.MethodWithActivity(eventNameWithoutParent);
            Assert.True(stream.Length > 0);
            long currLength = stream.Length;

            const string traceParent = "00-3236da27af79882bd317c4d1c3776982-a3cc9bd52ccd58e6-01";
            testClass.SetTraceParent(traceParent);
            const string eventNameWithParent = "eventNameWithParent";
            testClass.MethodWithActivity(eventNameWithParent);
            Assert.True(stream.Length > 0);

            stream.Seek(0, SeekOrigin.Begin);
            StreamReader reader = new(stream);

            int lineCount = 0;
            string? text = reader.ReadLine();
            while (text != null)
            {
                lineCount++;
                SerializableActivity? clientActivity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.NotNull(clientActivity);
                if (clientActivity.OperationName.Contains(eventNameWithoutParent))
                {
                    Assert.Null(clientActivity.ParentId);
                }
                else if (clientActivity.OperationName.Contains(eventNameWithParent))
                {
                    Assert.Equal(traceParent, clientActivity.ParentId);
                }

                text = reader.ReadLine();
            }

        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
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

        private class TraceInheritor : TracingBase
        {
            internal TraceInheritor(string? activitySourceName = default) : base(activitySourceName) { }

            internal void MethodWithNoInstrumentation()
            {

            }

            internal void MethodWithActivity()
            {
                TraceActivity(_ => { });
            }

            internal void MethodWithActivity(string activityName)
            {
                TraceActivity(activity => { Console.WriteLine(activity?.OperationName); }, activityName);
            }

            internal void MethodWithActivityRecursive(string activityName, int recurseCount)
            {
                TraceActivity(_ =>
                {
                    recurseCount--;
                    if (recurseCount > 0)
                    {
                        MethodWithActivityRecursive(activityName, recurseCount);
                    }
                }, activityName + recurseCount.ToString());
            }

            internal void MethodWithEvent(string eventName)
            {
                TraceActivity((acitivity) => acitivity?.AddEvent(new ActivityEvent(eventName)));
            }

            internal void SetTraceParent(string? traceParent)
            {
                TraceParent = traceParent;
            }
        }

        internal class MemoryStreamExporter : BaseExporter<Activity>
        {
            private readonly MemoryStream _stream;

            public MemoryStreamExporter(MemoryStream stream)
            {
                _stream = stream;
            }

            public override ExportResult Export(in Batch<Activity> batch)
            {
                byte[] newLine = Encoding.UTF8.GetBytes(Environment.NewLine);
                foreach (Activity activity in batch)
                {
                    byte[] jsonString = JsonSerializer.SerializeToUtf8Bytes(activity);
                    _stream.Write(jsonString, 0, jsonString.Length);
                    _stream.Write(newLine, 0, newLine.Length);
                }
                return ExportResult.Success;
            }
        }
    }

    public static class AdbcMemoryTestExporterExtensions
    {
        public static TracerProviderBuilder AddTestMemoryExporter(this TracerProviderBuilder builder, MemoryStream stream)
        {
            return builder.AddProcessor(sp => new SimpleActivityExportProcessor(new TracingTests.MemoryStreamExporter(stream)));
        }
    }
}

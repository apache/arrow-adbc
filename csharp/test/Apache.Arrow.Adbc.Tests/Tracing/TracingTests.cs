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
    public class TracingTests(ITestOutputHelper? outputHelper) : IDisposable
    {
        private readonly ITestOutputHelper? _outputHelper = outputHelper;
        private bool _disposed;

        [Fact]
        internal void CanStartActivity()
        {
            string activitySourceName = NewName();
            using MemoryStream stream = new();
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestMemoryExporter(stream)
                .Build();

            var testClass = new TraceInheritor(activitySourceName);
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
                SerializableActivity? activity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.NotNull(activity);
                Assert.Contains(nameof(TraceInheritor.MethodWithActivity), activity.OperationName);
                Assert.DoesNotContain(nameof(TraceInheritor.MethodWithNoInstrumentation), activity.OperationName);
                text = reader.ReadLine();
            }
            Assert.Equal(1, lineCount);
        }

        [Fact]
        internal void CanAddEvent()
        {
            string activitySourceName = NewName();
            using MemoryStream stream = new();
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestMemoryExporter(stream)
                .Build();

            var testClass = new TraceInheritor(activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Equal(0, stream.Length);

            string eventName = NewName();
            testClass.MethodWithEvent(eventName);
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
                Assert.Contains(nameof(TraceInheritor.MethodWithEvent), text);
                Assert.DoesNotContain(nameof(TraceInheritor.MethodWithNoInstrumentation), text);
                Assert.Contains(eventName, text);
                text = reader.ReadLine();
            }
            Assert.Equal(1, lineCount);
        }

        [Fact]
        internal void CanSerializeDeserializeActivity()
        {
            string activitySourceName = NewName();
            using MemoryStream stream = new();
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestMemoryExporter(stream)
                .Build();

            var testClass = new TraceInheritor(activitySourceName);
            string activityName = NewName();
            string eventName = NewName();
            const string rootId = "3236da27af79882bd317c4d1c3776982";
            string traceParent = $"00-{rootId}-a3cc9bd52ccd58e6-01";
            IReadOnlyList<KeyValuePair<string, object?>> tags =
                [
                    new (NewName(), NewName()),
                    new (NewName(), NewName()),
                ];
            testClass.MethodWithAllProperties(activityName, eventName, tags, traceParent);
            stream.Seek(0, SeekOrigin.Begin);
            StreamReader reader = new(stream);

            int lineCount = 0;
            string? text = reader.ReadLine();
            while (text != null)
            {
                lineCount++;
                SerializableActivity? activity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.NotNull(activity);
                string activityJson = JsonSerializer.Serialize(activity);
                Assert.Equal(rootId, activity.TraceId);
                Assert.Equal(rootId, activity.RootId);
                Assert.Contains(rootId, activity.ParentId);
                Assert.True(activity.HasRemoteParent);
                Assert.Equal(text, activityJson);

                text = reader.ReadLine();
            }
            Assert.Equal(1, lineCount);
        }

        [Fact]
        internal void CanAddActivityWithDepth()
        {
            string activitySourceName = NewName();
            using MemoryStream stream = new();
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestMemoryExporter(stream)
                .Build();

            var testClass = new TraceInheritor(activitySourceName);
            const int recurseCount = 5;
            testClass.MethodWithActivityRecursive(nameof(TraceInheritor.MethodWithActivityRecursive), recurseCount);

            stream.Seek(0, SeekOrigin.Begin);
            StreamReader reader = new(stream);

            int lineCount = 0;
            string? text = reader.ReadLine();
            while (text != null)
            {
                if (string.IsNullOrWhiteSpace(text)) continue;
                lineCount++;
                Assert.Contains(nameof(TraceInheritor.MethodWithActivityRecursive), text);
                Assert.DoesNotContain(nameof(TraceInheritor.MethodWithNoInstrumentation), text);
                SerializableActivity? activity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.Contains(nameof(TraceInheritor.MethodWithActivityRecursive), activity?.OperationName);
                Assert.NotNull(activity);
                text = reader.ReadLine();
            }
            Assert.Equal(recurseCount, lineCount);
        }

        [Fact]
        internal void CanAddTraceParent()
        {
            string activitySourceName = NewName();
            using MemoryStream stream = new();
            stream.SetLength(0);
            using TracerProvider provider1 = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestMemoryExporter(stream)
                .Build();

            var testClass = new TraceInheritor(activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Equal(0, stream.Length);

            const string eventNameWithParent = "eventNameWithParent";
            const string eventNameWithoutParent = "eventNameWithoutParent";
            testClass.MethodWithActivity(eventNameWithoutParent);
            Assert.True(stream.Length > 0);

            const string traceParent = "00-3236da27af79882bd317c4d1c3776982-a3cc9bd52ccd58e6-01";

            testClass.SetTraceParent(traceParent);
            const int withParentCountExpected = 10;
            for (int i = 0; i < withParentCountExpected; i++)
            {
                testClass.MethodWithActivity(eventNameWithParent);
            }
            testClass.SetTraceParent(null);

            testClass.MethodWithActivity(eventNameWithoutParent);
            Assert.True(stream.Length > 0);

            stream.Seek(0, SeekOrigin.Begin);
            StreamReader reader = new(stream);

            int lineCount = 0;
            int withParentCount = 0;
            int withoutParentCount = 0;
            string? text = reader.ReadLine();
            while (text != null)
            {
                lineCount++;
                SerializableActivity? clientActivity = JsonSerializer.Deserialize<SerializableActivity>(text);
                Assert.NotNull(clientActivity);
                if (clientActivity.OperationName.Contains(eventNameWithoutParent))
                {
                    withoutParentCount++;
                    Assert.Null(clientActivity.ParentId);
                }
                else if (clientActivity.OperationName.Contains(eventNameWithParent))
                {
                    withParentCount++;
                    Assert.Equal(traceParent, clientActivity.ParentId);
                }
                text = reader.ReadLine();
            }
            Assert.Equal(2, withoutParentCount);
            Assert.Equal(withParentCountExpected, withParentCount);
        }

        internal static string NewName() => Guid.NewGuid().ToString().Replace("-", "").ToLower();

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

        private class TraceInheritor : ActivityTrace
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
                TraceActivity(activity => { }, activityName: activityName);
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
                }, activityName: activityName + recurseCount.ToString());
            }

            internal void MethodWithEvent(string eventName)
            {
                TraceActivity((activity) => activity?.AddEvent(eventName));
            }

            internal void MethodWithAllProperties(
                string activityName,
                string eventName,
                IReadOnlyList<KeyValuePair<string, object?>> tags,
                string traceParent)
            {
                TraceActivity(activity =>
                {
                    foreach (KeyValuePair<string, object?> tag in tags)
                    {
                        activity?.AddTag(tag.Key, tag.Value)
                            .AddBaggage(tag.Key, tag.Value?.ToString());
                    }
                    activity?.AddEvent(eventName, tags)
                        .AddLink(traceParent, tags);
                }, activityName: activityName, traceParent: traceParent);
            }

            internal void SetTraceParent(string? traceParent)
            {
                TraceParent = traceParent;
            }
        }

        internal class MemoryStreamExporter(MemoryStream stream) : BaseExporter<Activity>
        {
            private readonly MemoryStream _stream = stream;

            public override ExportResult Export(in Batch<Activity> batch)
            {
                byte[] newLine = Encoding.UTF8.GetBytes(Environment.NewLine);
                foreach (Activity activity in batch)
                {
                    var sa = new SerializableActivity(activity);
                    byte[] jsonString = JsonSerializer.SerializeToUtf8Bytes(sa);
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

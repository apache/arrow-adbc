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
using System.Linq;
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
            Queue<Activity> exportedActivities = new();
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestActivityQueueExporter(exportedActivities)
                .Build();

            var testClass = new TraceProducer(activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Empty(exportedActivities);

            testClass.MethodWithActivity();
            Assert.True(exportedActivities.Count > 0);
            long currLength = exportedActivities.Count;

            testClass.MethodWithNoInstrumentation();
            Assert.Equal(currLength, exportedActivities.Count);

            int lineCount = 0;
            foreach (var exportedActivity in exportedActivities)
            {
                lineCount++;
                Assert.NotNull(exportedActivity);
                Assert.Contains(nameof(TraceProducer.MethodWithActivity), exportedActivity.OperationName);
                Assert.DoesNotContain(nameof(TraceProducer.MethodWithNoInstrumentation), exportedActivity.OperationName);
            }
            Assert.Equal(1, lineCount);
        }

        [Fact]
        internal void CanAddEvent()
        {
            string activitySourceName = NewName();
            Queue<Activity> exportedActivities = new();
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestActivityQueueExporter(exportedActivities)
                .Build();

            var testClass = new TraceProducer(activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Empty(exportedActivities);

            string eventName = NewName();
            testClass.MethodWithEvent(eventName);
            Assert.True(exportedActivities.Count > 0);
            long currLength = exportedActivities.Count;

            testClass.MethodWithNoInstrumentation();
            Assert.Equal(currLength, exportedActivities.Count);

            int lineCount = 0;
            foreach (var exportedActivity in exportedActivities)
            {
                lineCount++;
                Assert.NotNull(exportedActivity);
                Assert.Contains(nameof(TraceProducer.MethodWithEvent), exportedActivity.OperationName);
                Assert.DoesNotContain(nameof(TraceProducer.MethodWithNoInstrumentation), exportedActivity.OperationName);
                Assert.Contains(eventName, exportedActivity.Events.FirstOrDefault().Name);
            }

        }

        [Fact]
        internal void CanAddActivityWithDepth()
        {
            string activitySourceName = NewName();
            Queue<Activity> exportedActivities = new();
            using TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestActivityQueueExporter(exportedActivities)
                .Build();

            var testClass = new TraceProducer(activitySourceName);
            const int recurseCount = 5;
            testClass.MethodWithActivityRecursive(nameof(TraceProducer.MethodWithActivityRecursive), recurseCount);

            int lineCount = 0;
            foreach(var exportedActivity in exportedActivities)
            {
                lineCount++;
                Assert.NotNull(exportedActivity);
                Assert.Contains(nameof(TraceProducer.MethodWithActivityRecursive), exportedActivity.OperationName);
                Assert.DoesNotContain(nameof(TraceProducer.MethodWithNoInstrumentation), exportedActivity.OperationName);
                Assert.NotNull(exportedActivity);
            }
            Assert.Equal(recurseCount, lineCount);
        }

        [Fact]
        internal void CanAddTraceParent()
        {
            string activitySourceName = NewName();
            Queue<Activity> exportedActivities = new();
            using TracerProvider provider1 = Sdk.CreateTracerProviderBuilder()
                .AddSource(activitySourceName)
                .AddTestActivityQueueExporter(exportedActivities)
                .Build();

            var testClass = new TraceProducer(activitySourceName);
            testClass.MethodWithNoInstrumentation();
            Assert.Empty(exportedActivities);

            const string eventNameWithParent = "eventNameWithParent";
            const string eventNameWithoutParent = "eventNameWithoutParent";
            testClass.MethodWithActivity(eventNameWithoutParent);
            Assert.True(exportedActivities.Count() > 0);

            const string traceParent = "00-3236da27af79882bd317c4d1c3776982-a3cc9bd52ccd58e6-01";

            const int withParentCountExpected = 10;
            for (int i = 0; i < withParentCountExpected; i++)
            {
                testClass.MethodWithActivity(eventNameWithParent, traceParent);
            }

            testClass.MethodWithActivity(eventNameWithoutParent);
            Assert.True(exportedActivities.Count() > 0);

            int lineCount = 0;
            int withParentCount = 0;
            int withoutParentCount = 0;
            foreach (var exportedActivity in exportedActivities)
            {
                lineCount++;
                Assert.NotNull(exportedActivity);
                if (exportedActivity.OperationName.Contains(eventNameWithoutParent))
                {
                    withoutParentCount++;
                    Assert.Null(exportedActivity.ParentId);
                }
                else if (exportedActivity.OperationName.Contains(eventNameWithParent))
                {
                    withParentCount++;
                    Assert.Equal(traceParent, exportedActivity.ParentId);
                }
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

        private class TraceProducer : IDisposable
        {
            private readonly ActivityTrace _trace;
            private bool _isDisposed;

            internal TraceProducer(string? activitySourceName = default, string? traceParent = default)
            {
                _trace = new ActivityTrace(activitySourceName, traceParent: traceParent);
            }

            internal void MethodWithNoInstrumentation()
            {

            }

            internal void MethodWithActivity()
            {
                _trace.TraceActivity(_ => { });
            }

            internal void MethodWithActivity(string activityName, string? traceParent = default)
            {
                _trace.TraceActivity(activity => { }, activityName: activityName, traceParent: traceParent);
            }

            internal void MethodWithActivityRecursive(string activityName, int recurseCount)
            {
                _trace.TraceActivity(_ =>
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
                _trace.TraceActivity((activity) => activity?.AddEvent(eventName));
            }

            internal void MethodWithAllProperties(
                string activityName,
                string eventName,
                IReadOnlyList<KeyValuePair<string, object?>> tags,
                string traceParent)
            {
                _trace.TraceActivity(activity =>
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

            protected virtual void Dispose(bool disposing)
            {
                if (!_isDisposed && disposing)
                {
                    _trace.Dispose();
                    _isDisposed = true;
                }
            }

            public void Dispose()
            {
                // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
                Dispose(disposing: true);
                GC.SuppressFinalize(this);
            }
        }

        internal class ActivityQueueExporter(Queue<Activity> exportedActivities) : BaseExporter<Activity>
        {
            private Queue<Activity> ExportedActivities { get; } = exportedActivities;

            public override ExportResult Export(in Batch<Activity> batch)
            {
                foreach (Activity activity in batch)
                {
                    ExportedActivities.Enqueue(activity);
                }
                return ExportResult.Success;
            }
        }
    }

    public static class AdbcMemoryTestExporterExtensions
    {
        public static TracerProviderBuilder AddTestActivityQueueExporter(this TracerProviderBuilder builder, Queue<Activity> queue)
        {
            return builder.AddProcessor(sp => new SimpleActivityExportProcessor(new TracingTests.ActivityQueueExporter(queue)));
        }
    }
}

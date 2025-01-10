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
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Mathematics;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Tracing
{
    public class TracingPerformance(ITestOutputHelper outputHelper)
    {
        private readonly ITestOutputHelper _outputHelper = outputHelper;

        [SkippableFact]
        public void TestPerformance()
        {
#if DEBUG
            Skip.If(true, "Must be run using 'Release' build.");
#endif
            Skip.If(Environment.Version.Major < 6, $"Must be run fron .NET 6+. Current version: {Environment.Version} - '{RuntimeInformation.FrameworkDescription}'");

            // Runs the performance tests
            Summary summary = BenchmarkRunner.Run<Tracing>();

            Dictionary<int, Dictionary<bool, double>> testCasesByNumberOfEvent = new Dictionary<int, Dictionary<bool, double>>();

            // Confirm the mean time in the reports are within tolerance
            foreach (BenchmarkReport report in summary.Reports)
            {
                int? numberOfEvents = report.BenchmarkCase.Parameters[nameof(Tracing.NumberOfEvents)] as int?;
                Assert.NotNull(numberOfEvents);
                bool? isTracingEnabled = report.BenchmarkCase.Parameters[nameof(Tracing.IsTracingEnabled)] as bool?;
                Assert.NotNull(isTracingEnabled);
                Statistics? stats = report.ResultStatistics;
                Assert.NotNull(stats);

                double meanTimeInTicks = stats.Mean / 100;
                double meanTimeInTicksPerEvent = meanTimeInTicks / numberOfEvents.Value;
                TimeSpan meanTimePerEvent = TimeSpan.FromTicks((long)meanTimeInTicksPerEvent);

                // Note: these value are twice as large as the largest observed mean times on development machine.
                const double maxForTracingEnabled = 0.06;
                const double maxForTracingDisabled = 0.04;
                double maxExpectedMeanTimeMilliseconds = isTracingEnabled.Value
                    ? maxForTracingEnabled
                    : maxForTracingDisabled;

                _outputHelper.WriteLine($"EnableTracing: {isTracingEnabled.Value} - Events: {numberOfEvents.Value} - Mean (ms/event): {meanTimePerEvent.TotalMilliseconds}");
                Assert.True(meanTimePerEvent.TotalMilliseconds < maxExpectedMeanTimeMilliseconds, $"Mean was {meanTimePerEvent.TotalMilliseconds} ms/iteration for {numberOfEvents} events.");

                if (testCasesByNumberOfEvent.TryGetValue(numberOfEvents.Value, out Dictionary<bool, double>? value))
                {
                    value.Add(isTracingEnabled.Value, meanTimePerEvent.TotalMilliseconds);
                }
                else
                {
                    testCasesByNumberOfEvent[numberOfEvents.Value] = new Dictionary<bool, double>() { { isTracingEnabled.Value, meanTimePerEvent.TotalMilliseconds } };
                }
            }

            _outputHelper.WriteLine(string.Empty);
            const double diffTolerance = 0.05;
            foreach (int numberOfEvents in testCasesByNumberOfEvent.Keys)
            {
                Dictionary<bool, double> performanceByTracingEnabled = testCasesByNumberOfEvent[numberOfEvents];
                Assert.Equal(2, performanceByTracingEnabled.Count);
                double performanceDifference = performanceByTracingEnabled[true] - performanceByTracingEnabled[false];
                _outputHelper.WriteLine(string.Format("Events: {0} - Disabled: {1} ms - Enabled {2} ms - Difference {3} ms",
                    numberOfEvents,
                    performanceByTracingEnabled[false],
                    performanceByTracingEnabled[true],
                    performanceDifference));
                Assert.True(performanceDifference < diffTolerance, $"Expect performance difference to be less than '{diffTolerance}'. Actual '{performanceDifference}'");
            }
        }
    }

    [MinColumn, MaxColumn]
    [SimpleJob(warmupCount: 3, iterationCount: 20)]
    public class Tracing : TracingBase
    {
        private const string MyActivitySourceName = "MyActivitySourceName";

        private TracerProvider? _tracerProvider = default;

        public Tracing()
            : base(MyActivitySourceName)
        { }

        [Params(10, 100, 1000)]
        public int NumberOfEvents { get; set; }

        [Params(false, true)]
        public bool IsTracingEnabled { get; set; }

        [Benchmark]
        public async Task TestTracing()
        {
            for (int i = 0; i < NumberOfEvents; i++)
            {
                await TraceActivity(async activity =>
                {
                    await DoWork(activity, 20);
                });
            }
        }

        private static async Task DoWork(Activity? activity, int iterations)
        {
            for (int i = 0; i < iterations; i++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(0.1));
            }
        }

        [GlobalSetup]
        public void GlobalSetup()
        {
            if (IsTracingEnabled)
            {
                _tracerProvider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(MyActivitySourceName)
                    .AddAdbcFileExporter(
                        MyActivitySourceName,
                        maxTraceFiles: 999999)
                    .Build();
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _tracerProvider?.Dispose();
            var location = new DirectoryInfo(
                Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    "Apache.Arrow.Adbc",
                    "Traces")
                );
            if (location.Exists) location.Delete(recursive: true);
        }
    }
}

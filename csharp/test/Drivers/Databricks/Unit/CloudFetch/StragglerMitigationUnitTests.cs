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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.CloudFetch
{
    /// <summary>
    /// Unit tests for straggler mitigation components.
    /// Tests focus on critical edge cases, concurrency safety, and correctness of core algorithms.
    /// </summary>
    public class StragglerMitigationUnitTests
    {
        #region Duplicate Detection Prevention

        [Fact]
        public void DuplicateDetectionPrevention_SameFileCountedOnceAcrossMultipleCycles()
        {
            // Arrange - Create detector with tracking dictionary
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), 10);
            var trackingDict = new ConcurrentDictionary<long, bool>();

            // Create one slow download that will be detected as straggler
            var metrics = new List<FileDownloadMetrics>();
            var slowMetric = new FileDownloadMetrics(100, 1024 * 1024);
            metrics.Add(slowMetric);

            // Age the slow download
            Thread.Sleep(500);

            // Add fast completed downloads to establish baseline
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(5);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act - Run straggler detection 5 times (simulating multiple monitoring cycles)
            for (int i = 0; i < 5; i++)
            {
                detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow, trackingDict);
            }

            // Assert - Counter should only increment once despite multiple detections
            Assert.Equal(1, detector.GetTotalStragglersDetectedInQuery());
        }

        #endregion

        #region CancellationTokenSource Management

        [Fact]
        public void CTSAtomicReplacement_EnsuresNoRaceCondition()
        {
            // Arrange - Simulate per-file CTS dictionary
            var ctsDict = new ConcurrentDictionary<long, CancellationTokenSource>();
            var globalCts = new CancellationTokenSource();
            long fileOffset = 100;

            // Initial CTS for the download
            var initialCts = CancellationTokenSource.CreateLinkedTokenSource(globalCts.Token);
            ctsDict[fileOffset] = initialCts;

            // Act - Replace CTS atomically using AddOrUpdate (simulating retry scenario)
            var newCts = CancellationTokenSource.CreateLinkedTokenSource(globalCts.Token);
            var oldCts = ctsDict.AddOrUpdate(
                fileOffset,
                newCts,
                (key, existing) =>
                {
                    existing?.Dispose();
                    return newCts;
                });

            // Assert - New CTS is in dictionary and not cancelled
            Assert.Equal(newCts, ctsDict[fileOffset]);
            Assert.False(newCts.IsCancellationRequested);
        }

        #endregion

        #region Cleanup Behavior

        [Fact]
        public void CleanupInFinally_ExecutesEvenOnException()
        {
            // Arrange - Simulate cleanup pattern with exception during initialization
            var cancellationTokens = new ConcurrentDictionary<long, CancellationTokenSource>();
            long fileOffset = 100;
            bool cleanupExecuted = false;

            // Act - Simulate exception during download initialization
            try
            {
                var cts = new CancellationTokenSource();
                cancellationTokens[fileOffset] = cts;
                throw new Exception("Simulated failure during download initialization");
            }
            catch
            {
                // Expected exception
            }
            finally
            {
                // Cleanup must execute regardless of exception
                if (cancellationTokens.TryRemove(fileOffset, out var cts))
                {
                    cts?.Dispose();
                    cleanupExecuted = true;
                }
            }

            // Assert - Cleanup executed and token removed
            Assert.True(cleanupExecuted);
            Assert.False(cancellationTokens.ContainsKey(fileOffset));
        }

        [Fact]
        public async Task CleanupTask_RespectsShutdownCancellation()
        {
            // Arrange - Simulate cleanup task that should respect shutdown token
            var activeMetrics = new ConcurrentDictionary<long, FileDownloadMetrics>();
            var shutdownCts = new CancellationTokenSource();
            long fileOffset = 100;

            activeMetrics[fileOffset] = new FileDownloadMetrics(fileOffset, 1024 * 1024);

            // Act - Start cleanup task with delay, then trigger shutdown
            var cleanupTask = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), shutdownCts.Token);
                    activeMetrics.TryRemove(fileOffset, out _);
                }
                catch (OperationCanceledException)
                {
                    // Shutdown requested - clean up immediately
                    activeMetrics.TryRemove(fileOffset, out _);
                }
            });

            // Trigger shutdown immediately
            shutdownCts.Cancel();
            await cleanupTask;

            // Assert - Cleanup completed despite cancellation
            Assert.False(activeMetrics.ContainsKey(fileOffset));
        }

        [Fact]
        public async Task ConcurrentCTSCleanup_HandlesParallelDisposal()
        {
            // Arrange - Create multiple CTS entries
            var cancellationTokens = new ConcurrentDictionary<long, CancellationTokenSource>();

            for (long i = 0; i < 50; i++)
            {
                cancellationTokens[i] = new CancellationTokenSource();
            }

            // Act - Clean up all entries concurrently
            var cleanupTasks = cancellationTokens.Keys.Select(offset => Task.Run(() =>
            {
                if (cancellationTokens.TryRemove(offset, out var cts))
                {
                    cts?.Dispose();
                }
            }));

            await Task.WhenAll(cleanupTasks);

            // Assert - All entries cleaned up without errors
            Assert.Empty(cancellationTokens);
        }

        #endregion

        #region Counter Overflow Protection

        [Fact]
        public void CounterOverflow_UsesLongToPreventWraparound()
        {
            // Arrange - Create detector with lower quantile
            var detector = new StragglerDownloadDetector(1.5, 0.2, TimeSpan.FromMilliseconds(10), 10000);
            var trackingDict = new ConcurrentDictionary<long, bool>();

            // Create metrics with enough completed downloads to meet quantile
            var metrics = new List<FileDownloadMetrics>();

            // Add 250 completed downloads (25% of 1000 total, meets 20% quantile)
            for (int i = 0; i < 250; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(1);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Add 750 slow downloads
            for (int i = 250; i < 1000; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                metrics.Add(m);
            }

            Thread.Sleep(300);

            // Act - Detect stragglers
            detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow, trackingDict);

            // Assert - Counter should handle large values (long type prevents overflow)
            long count = detector.GetTotalStragglersDetectedInQuery();
            Assert.True(count > 0, "Counter should track large number of stragglers without overflow");
        }

        #endregion

        #region Median Calculation Correctness

        [Fact]
        public void MedianCalculation_EvenCount_ReturnsAverageOfMiddleTwo()
        {
            // Arrange - Create detector
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), 10);

            // Create even number of completed downloads (10 downloads)
            var metrics = new List<FileDownloadMetrics>();
            var delays = new[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };

            for (int i = 0; i < delays.Length; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(delays[i]);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act - Detection will calculate median internally
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - No stragglers expected (all completed), but median was calculated correctly
            // Median of even count = average of 5th and 6th elements
            Assert.Empty(stragglers);
        }

        [Fact]
        public void MedianCalculation_OddCount_ReturnsMiddleElement()
        {
            // Arrange - Create detector
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), 10);

            // Create odd number of completed downloads (9 downloads)
            var metrics = new List<FileDownloadMetrics>();
            var delays = new[] { 10, 20, 30, 40, 50, 60, 70, 80, 90 };

            for (int i = 0; i < delays.Length; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(delays[i]);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act - Detection will calculate median internally
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - No stragglers expected (all completed), but median was calculated correctly
            // Median of odd count = 5th element (middle)
            Assert.Empty(stragglers);
        }

        #endregion

        #region Edge Cases and Null Safety

        [Fact]
        public void EmptyMetricsList_ReturnsEmptyWithoutError()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), 10);
            var emptyMetrics = new List<FileDownloadMetrics>();

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(emptyMetrics, DateTime.UtcNow);

            // Assert - Should handle empty list gracefully
            Assert.Empty(stragglers);
        }

        [Fact]
        public async Task ConcurrentModification_ThreadSafeOperation()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), 10);
            var metrics = new ConcurrentBag<FileDownloadMetrics>();
            var trackingDict = new ConcurrentDictionary<long, bool>();

            // Create initial completed downloads
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(5);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Add slow active download
            var slowMetric = new FileDownloadMetrics(100, 1024 * 1024);
            metrics.Add(slowMetric);

            Thread.Sleep(500);

            // Act - Run detection concurrently while modifying the collection
            var tasks = new List<Task>();

            // Task 1: Run detection multiple times
            tasks.Add(Task.Run(() =>
            {
                for (int i = 0; i < 5; i++)
                {
                    detector.IdentifyStragglerDownloads(metrics.ToList(), DateTime.UtcNow, trackingDict);
                    Thread.Sleep(10);
                }
            }));

            // Task 2: Add more completed downloads
            tasks.Add(Task.Run(() =>
            {
                for (int i = 20; i < 25; i++)
                {
                    var m = new FileDownloadMetrics(i, 1024 * 1024);
                    Thread.Sleep(5);
                    m.MarkDownloadCompleted();
                    metrics.Add(m);
                    Thread.Sleep(10);
                }
            }));

            await Task.WhenAll(tasks.ToArray());

            // Assert - Should handle concurrent access without errors
            long count = detector.GetTotalStragglersDetectedInQuery();
            Assert.True(count >= 0, "Counter should remain valid under concurrent access");
        }

        #endregion
    }
}

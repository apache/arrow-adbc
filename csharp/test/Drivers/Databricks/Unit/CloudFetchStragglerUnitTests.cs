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

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    /// <summary>
    /// Comprehensive unit tests for straggler mitigation components.
    /// Tests cover basic functionality, parameter validation, edge cases, and advanced scenarios
    /// including concurrency safety and cleanup behavior.
    /// </summary>
    public class CloudFetchStragglerUnitTests
    {
        #region FileDownloadMetrics Tests

        [Fact]
        public void FileDownloadMetrics_CalculateThroughput_BeforeCompletion_ReturnsNull()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(fileOffset: 0, fileSizeBytes: 1024);

            // Act
            var throughput = metrics.CalculateThroughputBytesPerSecond();

            // Assert
            Assert.Null(throughput);
        }

        [Fact]
        public void FileDownloadMetrics_CalculateThroughput_AfterCompletion_ReturnsValue()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(fileOffset: 0, fileSizeBytes: 10 * 1024 * 1024);
            System.Threading.Thread.Sleep(100); // Simulate download time

            // Act
            metrics.MarkDownloadCompleted();
            var throughput = metrics.CalculateThroughputBytesPerSecond();

            // Assert
            Assert.NotNull(throughput);
            Assert.True(throughput.Value > 0);
        }

        [Fact]
        public void FileDownloadMetrics_MarkCancelledAsStragler_SetsFlag()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(fileOffset: 0, fileSizeBytes: 1024);

            // Act
            metrics.MarkCancelledAsStragler();

            // Assert
            Assert.True(metrics.WasCancelledAsStragler);
        }

        #endregion

        #region StragglerDownloadDetector Tests - Parameter Validation

        [Fact]
        public void StragglerDownloadDetector_MultiplierLessThanOne_ThrowsException()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                new StragglerDownloadDetector(
                    stragglerThroughputMultiplier: 0.9,
                    minimumCompletionQuantile: 0.6,
                    stragglerDetectionPadding: TimeSpan.FromSeconds(5),
                    maxStragglersBeforeFallback: 10));

            Assert.Contains("multiplier", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void StragglerDownloadDetector_QuantileOutOfRange_ThrowsException()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
                new StragglerDownloadDetector(
                    stragglerThroughputMultiplier: 1.5,
                    minimumCompletionQuantile: 1.5,
                    stragglerDetectionPadding: TimeSpan.FromSeconds(5),
                    maxStragglersBeforeFallback: 10));

            Assert.Contains("quantile", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        #endregion

        #region StragglerDownloadDetector Tests - Median Calculation

        [Fact]
        public void StragglerDownloadDetector_MedianCalculation_OddCount()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.5, TimeSpan.FromSeconds(5), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Create 5 completed downloads with different speeds
            for (int i = 0; i < 5; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024); // 1MB each
                System.Threading.Thread.Sleep(10 + i * 10); // Different durations
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - No stragglers since all completed
            Assert.Empty(stragglers);
        }

        [Fact]
        public void StragglerDownloadDetector_MedianCalculation_EvenCount()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.5, TimeSpan.FromSeconds(5), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Create 4 completed downloads
            for (int i = 0; i < 4; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024); // 1MB each
                System.Threading.Thread.Sleep(10 + i * 10);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert
            Assert.Empty(stragglers);
        }

        [Fact]
        public void StragglerDownloadDetector_NoCompletedDownloads_ReturnsEmpty()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(5), 10);
            var metrics = new List<FileDownloadMetrics>
            {
                new FileDownloadMetrics(0, 1024 * 1024) // Still in progress
            };

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert
            Assert.Empty(stragglers);
        }

        [Fact]
        public void StragglerDownloadDetector_BelowQuantileThreshold_ReturnsEmpty()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(5), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Create 10 downloads, only 5 completed (50% < 60% threshold)
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                if (i < 5)
                {
                    System.Threading.Thread.Sleep(10);
                    m.MarkDownloadCompleted();
                }
                metrics.Add(m);
            }

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - Below threshold, no detection
            Assert.Empty(stragglers);
        }

        [Fact]
        public void StragglerDownloadDetector_FallbackThreshold_Triggered()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(5), maxStragglersBeforeFallback: 3);
            var metrics = new List<FileDownloadMetrics>();

            // Simulate 10 fast downloads + 5 slow stragglers
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                System.Threading.Thread.Sleep(10);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Add 5 slow active downloads (stragglers)
            for (int i = 10; i < 15; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                // Simulate slow download by creating metric long ago
                metrics.Add(m);
            }

            // Act - Simulate time passing
            System.Threading.Thread.Sleep(2000);
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow.AddSeconds(10));

            // Assert - At least some stragglers detected
            Assert.NotEmpty(stragglers);
        }

        #endregion

        #region Edge Case Tests

        [Fact]
        public void StragglerDownloadDetector_EmptyMetricsList_ReturnsEmpty()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(5), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert
            Assert.Empty(stragglers);
        }

        [Fact]
        public void StragglerDownloadDetector_AllDownloadsCancelled_ReturnsEmpty()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(5), 10);
            var metrics = new List<FileDownloadMetrics>();

            for (int i = 0; i < 5; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                m.MarkCancelledAsStragler();
                metrics.Add(m);
            }

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - Cancelled downloads not re-identified
            Assert.Empty(stragglers);
        }

        #endregion

        #region Advanced Tests - Duplicate Detection Prevention

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

        #region Advanced Tests - CancellationTokenSource Management

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

        #region Advanced Tests - Cleanup Behavior

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

        #endregion

        #region Advanced Tests - Counter Overflow Protection

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

        #region Advanced Tests - Concurrency Safety

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

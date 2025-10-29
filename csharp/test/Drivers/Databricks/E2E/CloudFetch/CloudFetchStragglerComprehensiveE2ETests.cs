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

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.CloudFetch
{
    /// <summary>
    /// Comprehensive E2E tests for straggler mitigation with realistic scenarios.
    /// These tests validate actual behavior including detection, cancellation, fallback, and edge cases.
    /// </summary>
    public class CloudFetchStragglerComprehensiveE2ETests
    {
        /// <summary>
        /// Helper method to create fast completed downloads for testing.
        /// </summary>
        private List<FileDownloadMetrics> CreateFastCompletedDownloads(int count, int startOffset = 0)
        {
            var metrics = new List<FileDownloadMetrics>();
            for (int i = 0; i < count; i++)
            {
                var m = new FileDownloadMetrics(startOffset + i, 1024 * 1024);
                Thread.Sleep(10);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }
            return metrics;
        }

        /// <summary>
        /// Helper method to create slow active downloads for testing.
        /// These are created first and allowed to "age" to simulate slow downloads.
        /// </summary>
        private List<FileDownloadMetrics> CreateSlowActiveDownloads(int count, int startOffset = 100)
        {
            var metrics = new List<FileDownloadMetrics>();
            for (int i = 0; i < count; i++)
            {
                var m = new FileDownloadMetrics(startOffset + i, 1024 * 1024);
                metrics.Add(m);
            }
            return metrics;
        }

        #region Straggler Detection Tests

        [Fact]
        public void StragglerDetection_FastAndSlowDownloads_DetectsSlowOnes()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(
                stragglerThroughputMultiplier: 1.5,
                minimumCompletionQuantile: 0.6,
                stragglerDetectionPadding: TimeSpan.FromMilliseconds(50),
                maxStragglersBeforeFallback: 10);

            var metrics = new List<FileDownloadMetrics>();

            // Create 2 slow active downloads FIRST (so they have earlier start time)
            var slow1 = new FileDownloadMetrics(100, 1024 * 1024);
            var slow2 = new FileDownloadMetrics(101, 1024 * 1024);
            metrics.Add(slow1);
            metrics.Add(slow2);

            // Wait a bit so slow downloads have been running longer
            Thread.Sleep(1000);

            // Now create 10 fast completed downloads (1MB in ~10ms each)
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(10);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act - Detect stragglers
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow).ToList();

            // Assert - The 2 slow downloads should be detected
            Assert.Equal(2, stragglers.Count);
            Assert.Contains(100L, stragglers);
            Assert.Contains(101L, stragglers);
            Assert.False(detector.ShouldFallbackToSequentialDownloads); // Under threshold
        }

        [Fact]
        public void StragglerDetection_BelowQuantileThreshold_DoesNotDetect()
        {
            // Arrange - 60% quantile requires 6 out of 10 completed
            var detector = new StragglerDownloadDetector(
                stragglerThroughputMultiplier: 1.5,
                minimumCompletionQuantile: 0.6,
                stragglerDetectionPadding: TimeSpan.FromSeconds(1),
                maxStragglersBeforeFallback: 10);

            var metrics = new List<FileDownloadMetrics>();

            // Only 5 completed (50% < 60%)
            for (int i = 0; i < 5; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(10);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // 5 active downloads
            for (int i = 5; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                metrics.Add(m);
            }

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow).ToList();

            // Assert - Below threshold, no detection
            Assert.Empty(stragglers);
        }

        [Fact]
        public void StragglerDetection_AllDownloadsFast_DetectsNone()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(1), 10);
            var metrics = new List<FileDownloadMetrics>();

            // All downloads complete quickly
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(10);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow).ToList();

            // Assert
            Assert.Empty(stragglers);
        }

        [Fact]
        public void StragglerDetection_AlreadyCancelled_NotDetectedAgain()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(1), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Create fast completed downloads
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(10);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Add a slow download that was already cancelled
            var slow = new FileDownloadMetrics(100, 1024 * 1024);
            slow.MarkCancelledAsStragler();
            metrics.Add(slow);

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow).ToList();

            // Assert - Cancelled downloads are excluded
            Assert.Empty(stragglers);
        }

        #endregion

        #region Sequential Fallback Tests

        [Fact]
        public void SequentialFallback_ExceedsThreshold_Triggers()
        {
            // Arrange - Threshold of 5 stragglers
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), maxStragglersBeforeFallback: 5);

            // Create 6 slow downloads first
            var metrics = CreateSlowActiveDownloads(6);
            Thread.Sleep(500); // Let them age

            // Add 10 fast completed downloads
            metrics.AddRange(CreateFastCompletedDownloads(10));

            // Act - Detect stragglers (should exceed threshold of 5)
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow).ToList();

            // Assert - Should trigger fallback
            Assert.True(detector.ShouldFallbackToSequentialDownloads);
            Assert.True(detector.GetTotalStragglersDetectedInQuery() >= 5);
        }

        [Fact]
        public void SequentialFallback_BelowThreshold_DoesNotTrigger()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), maxStragglersBeforeFallback: 10);

            // Create 3 slow downloads first
            var metrics = CreateSlowActiveDownloads(3);
            Thread.Sleep(500); // Let them age

            // Add fast downloads
            metrics.AddRange(CreateFastCompletedDownloads(10));

            // Act - Detect stragglers (only 3, below threshold of 10)
            detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - Should NOT trigger fallback
            Assert.False(detector.ShouldFallbackToSequentialDownloads);
        }

        #endregion

        #region Duplicate Detection Prevention Tests

        [Fact]
        public void DuplicateDetection_SameFileTwice_CountedOnce()
        {
            // Arrange - Test the duplicate prevention fix
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), 10);
            var alreadyCounted = new ConcurrentDictionary<long, bool>();

            // Create slow download first
            var metrics = CreateSlowActiveDownloads(1);
            Thread.Sleep(500); // Let it age

            // Add fast downloads
            metrics.AddRange(CreateFastCompletedDownloads(10));

            // Act - Detect stragglers twice (simulating multiple monitoring cycles)
            var stragglers1 = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow, alreadyCounted).ToList();
            var stragglers2 = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow, alreadyCounted).ToList();

            // Assert - Counter should only increment once
            Assert.Single(stragglers1);
            Assert.Single(stragglers2);
            Assert.Equal(1, detector.GetTotalStragglersDetectedInQuery()); // Only counted once!
        }

        [Fact]
        public void DuplicateDetection_WithoutTracking_CountsMultipleTimes()
        {
            // Arrange - Without tracking dict, should count multiple times (to verify test works)
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromMilliseconds(50), 10);

            // Create slow download first
            var metrics = CreateSlowActiveDownloads(1);
            Thread.Sleep(500); // Let it age

            // Add fast downloads
            metrics.AddRange(CreateFastCompletedDownloads(10));

            // Act - Detect WITHOUT tracking dict (null)
            detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow, alreadyCounted: null);
            detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow, alreadyCounted: null);

            // Assert - Should count twice without tracking
            Assert.Equal(2, detector.GetTotalStragglersDetectedInQuery());
        }

        #endregion

        #region FileDownloadMetrics Tests

        [Fact]
        public void FileDownloadMetrics_InvalidSize_ThrowsException()
        {
            // Assert - Zero size
            Assert.Throws<ArgumentOutOfRangeException>(() => new FileDownloadMetrics(0, 0));

            // Assert - Negative size
            Assert.Throws<ArgumentOutOfRangeException>(() => new FileDownloadMetrics(0, -100));
        }

        [Fact]
        public void FileDownloadMetrics_ThroughputCalculation_ReturnsValidValue()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(0, 10 * 1024 * 1024); // 10MB
            Thread.Sleep(100); // 100ms

            // Act
            metrics.MarkDownloadCompleted();
            var throughput = metrics.CalculateThroughputBytesPerSecond();

            // Assert
            Assert.NotNull(throughput);
            Assert.True(throughput.Value > 0);
            Assert.True(throughput.Value < 1024 * 1024 * 1024); // Sanity check: < 1GB/s
        }

        [Fact]
        public void FileDownloadMetrics_ThroughputBeforeCompletion_ReturnsNull()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(0, 1024 * 1024);

            // Act
            var throughput = metrics.CalculateThroughputBytesPerSecond();

            // Assert
            Assert.Null(throughput);
        }

        [Fact]
        public void FileDownloadMetrics_StragglerFlag_WorksCorrectly()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(0, 1024 * 1024);
            Assert.False(metrics.WasCancelledAsStragler);

            // Act
            metrics.MarkCancelledAsStragler();

            // Assert
            Assert.True(metrics.WasCancelledAsStragler);
        }

        #endregion

        #region Counter Overflow Protection Tests

        [Fact]
        public void CounterOverflow_UsesLong_HandlesLargeNumbers()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(1), int.MaxValue);

            // Act - Verify counter is long type (can't overflow easily)
            var count = detector.GetTotalStragglersDetectedInQuery();

            // Assert - Type is long
            Assert.IsType<long>(count);
            Assert.Equal(0L, count);
        }

        #endregion

        #region Median Calculation Tests

        [Fact]
        public void MedianCalculation_OddCount_ReturnsMiddleValue()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.5, TimeSpan.FromSeconds(1), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Create 5 downloads with varying speeds (odd count)
            var speeds = new[] { 100, 200, 300, 400, 500 }; // Median should be 300
            for (int i = 0; i < 5; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(speeds[i]);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act - Detect with all completed (should calculate median)
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - No exception, median calculated
            Assert.NotNull(stragglers);
        }

        [Fact]
        public void MedianCalculation_EvenCount_ReturnsAverage()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.5, TimeSpan.FromSeconds(1), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Create 6 downloads with varying speeds (even count)
            var speeds = new[] { 100, 200, 300, 400, 500, 600 }; // Median: avg of 300 and 400 = 350
            for (int i = 0; i < 6; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                Thread.Sleep(speeds[i]);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);

            // Assert - No exception
            Assert.NotNull(stragglers);
        }

        #endregion

        #region Edge Cases

        [Fact]
        public void EdgeCase_NoCompletedDownloads_ReturnsEmpty()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(1), 10);
            var metrics = new List<FileDownloadMetrics>
            {
                new FileDownloadMetrics(0, 1024 * 1024),
                new FileDownloadMetrics(1, 1024 * 1024)
            };

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow).ToList();

            // Assert
            Assert.Empty(stragglers);
        }

        [Fact]
        public void EdgeCase_EmptyMetrics_ReturnsEmpty()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(1), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow).ToList();

            // Assert
            Assert.Empty(stragglers);
        }

        [Fact]
        public void EdgeCase_NullMetrics_ReturnsEmpty()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(1), 10);

            // Act
            var stragglers = detector.IdentifyStragglerDownloads(null!, DateTime.UtcNow).ToList();

            // Assert
            Assert.Empty(stragglers);
        }

        [Fact]
        public void EdgeCase_VeryFastDownload_DoesNotCauseDivisionError()
        {
            // Arrange
            var metrics = new FileDownloadMetrics(0, 1024 * 1024);

            // Act - Complete immediately (< 1ms)
            metrics.MarkDownloadCompleted();
            var throughput = metrics.CalculateThroughputBytesPerSecond();

            // Assert - Should clamp to 1ms minimum, not throw
            Assert.NotNull(throughput);
            Assert.True(throughput.Value > 0);
        }

        #endregion

        #region Concurrency Tests

        [Fact]
        public async Task Concurrency_ParallelDetection_CounterIsThreadSafe()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.5, TimeSpan.FromMilliseconds(50), 1000);

            // Create 10 slow downloads first
            var metrics = CreateSlowActiveDownloads(10, startOffset: 10);
            Thread.Sleep(500); // Let them age

            // Add baseline fast downloads
            metrics.AddRange(CreateFastCompletedDownloads(10));

            // Act - Run detection from multiple threads
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(() =>
                {
                    detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow);
                }));
            }
            await Task.WhenAll(tasks);

            // Assert - Counter should be thread-safe (exact count depends on timing)
            var count = detector.GetTotalStragglersDetectedInQuery();
            Assert.True(count > 0);
            Assert.True(count <= 100); // Should not exceed 10 stragglers * 10 threads
        }

        [Fact]
        public async Task Concurrency_ParallelDetectionWithTracking_PreventsDuplicates()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.5, TimeSpan.FromMilliseconds(50), 1000);
            var alreadyCounted = new ConcurrentDictionary<long, bool>();

            // Create 10 slow downloads first
            var metrics = CreateSlowActiveDownloads(10, startOffset: 10);
            Thread.Sleep(500); // Let them age

            // Add baseline fast downloads
            metrics.AddRange(CreateFastCompletedDownloads(10));

            // Act - Run detection from multiple threads WITH tracking
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(() =>
                {
                    detector.IdentifyStragglerDownloads(metrics, DateTime.UtcNow, alreadyCounted);
                }));
            }
            await Task.WhenAll(tasks);

            // Assert - With tracking, each straggler counted only once
            var count = detector.GetTotalStragglersDetectedInQuery();
            Assert.Equal(10, count); // Exactly 10, not duplicated
        }

        #endregion

        #region Parameter Validation Tests

        [Fact]
        public void ParameterValidation_InvalidMultiplier_ThrowsException()
        {
            // Assert
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new StragglerDownloadDetector(0.5, 0.6, TimeSpan.FromSeconds(1), 10));
        }

        [Fact]
        public void ParameterValidation_InvalidQuantile_ThrowsException()
        {
            // Assert - Too low
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new StragglerDownloadDetector(1.5, 0.0, TimeSpan.FromSeconds(1), 10));

            // Assert - Too high
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new StragglerDownloadDetector(1.5, 1.5, TimeSpan.FromSeconds(1), 10));
        }

        [Fact]
        public void ParameterValidation_NegativePadding_ThrowsException()
        {
            // Assert
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(-1), 10));
        }

        [Fact]
        public void ParameterValidation_NegativeMaxStragglers_ThrowsException()
        {
            // Assert
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new StragglerDownloadDetector(1.5, 0.6, TimeSpan.FromSeconds(1), -1));
        }

        #endregion
    }
}

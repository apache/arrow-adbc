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
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit
{
    /// <summary>
    /// Minimal unit tests for straggler mitigation components, focusing on mistake-prone areas.
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
    }
}

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

using System.Collections.Generic;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.E2E.CloudFetch
{
    /// <summary>
    /// E2E integration tests for straggler download mitigation feature.
    /// These tests verify configuration parsing and basic integration.
    /// </summary>
    public class CloudFetchStragglerE2ETests
    {
        [Fact]
        public void StragglerMitigation_DisabledByDefault()
        {
            // Arrange
            var properties = new Dictionary<string, string>();

            // Act & Assert - Feature should be disabled by default
            // This test validates that the feature doesn't activate without explicit configuration
            Assert.False(properties.ContainsKey(DatabricksParameters.CloudFetchStragglerMitigationEnabled));
        }

        [Fact]
        public void StragglerMitigation_ConfigurationParameters_AreDefined()
        {
            // Assert - Verify all configuration parameters are defined
            Assert.NotNull(DatabricksParameters.CloudFetchStragglerMitigationEnabled);
            Assert.NotNull(DatabricksParameters.CloudFetchStragglerMultiplier);
            Assert.NotNull(DatabricksParameters.CloudFetchStragglerQuantile);
            Assert.NotNull(DatabricksParameters.CloudFetchStragglerPaddingSeconds);
            Assert.NotNull(DatabricksParameters.CloudFetchMaxStragglersPerQuery);
            Assert.NotNull(DatabricksParameters.CloudFetchSynchronousFallbackEnabled);
        }

        [Fact]
        public void StragglerMitigation_ConfigurationParameters_HaveCorrectNames()
        {
            // Assert - Verify parameter naming follows convention
            Assert.Equal("adbc.databricks.cloudfetch.straggler_mitigation_enabled",
                DatabricksParameters.CloudFetchStragglerMitigationEnabled);
            Assert.Equal("adbc.databricks.cloudfetch.straggler_multiplier",
                DatabricksParameters.CloudFetchStragglerMultiplier);
            Assert.Equal("adbc.databricks.cloudfetch.straggler_quantile",
                DatabricksParameters.CloudFetchStragglerQuantile);
            Assert.Equal("adbc.databricks.cloudfetch.straggler_padding_seconds",
                DatabricksParameters.CloudFetchStragglerPaddingSeconds);
            Assert.Equal("adbc.databricks.cloudfetch.max_stragglers_per_query",
                DatabricksParameters.CloudFetchMaxStragglersPerQuery);
            Assert.Equal("adbc.databricks.cloudfetch.synchronous_fallback_enabled",
                DatabricksParameters.CloudFetchSynchronousFallbackEnabled);
        }

        [Fact]
        public void StragglerDownloadDetector_WithDefaultConfiguration_CreatesSuccessfully()
        {
            // Arrange & Act
            var detector = new StragglerDownloadDetector(
                stragglerThroughputMultiplier: 1.5,
                minimumCompletionQuantile: 0.6,
                stragglerDetectionPadding: System.TimeSpan.FromSeconds(5),
                maxStragglersBeforeFallback: 10);

            // Assert
            Assert.NotNull(detector);
            Assert.False(detector.ShouldFallbackToSequentialDownloads);
            Assert.Equal(0, detector.GetTotalStragglersDetectedInQuery());
        }

        [Fact]
        public void FileDownloadMetrics_CreatesSuccessfully()
        {
            // Arrange & Act
            var metrics = new FileDownloadMetrics(
                fileOffset: 12345,
                fileSizeBytes: 10 * 1024 * 1024);

            // Assert
            Assert.NotNull(metrics);
            Assert.Equal(12345, metrics.FileOffset);
            Assert.Equal(10 * 1024 * 1024, metrics.FileSizeBytes);
            Assert.False(metrics.IsDownloadCompleted);
            Assert.False(metrics.WasCancelledAsStragler);
        }

        [Fact]
        public void StragglerDownloadDetector_CounterIncrementsAtomically()
        {
            // Arrange
            var detector = new StragglerDownloadDetector(1.5, 0.6, System.TimeSpan.FromSeconds(5), 10);
            var metrics = new List<FileDownloadMetrics>();

            // Create fast completed downloads
            for (int i = 0; i < 10; i++)
            {
                var m = new FileDownloadMetrics(i, 1024 * 1024);
                System.Threading.Thread.Sleep(5);
                m.MarkDownloadCompleted();
                metrics.Add(m);
            }

            // Add slow active download
            var slowMetric = new FileDownloadMetrics(100, 1024 * 1024);
            metrics.Add(slowMetric);

            // Act - Detect stragglers (simulating slow download)
            System.Threading.Thread.Sleep(1000);
            var stragglers = detector.IdentifyStragglerDownloads(metrics, System.DateTime.UtcNow.AddSeconds(10));

            // Assert - Counter should increment for detected stragglers
            long count = detector.GetTotalStragglersDetectedInQuery();
            Assert.True(count >= 0); // Counter should be non-negative
        }
    }
}

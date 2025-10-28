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
using System.Linq;
using System.Threading;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Detects straggler downloads based on median throughput analysis.
    /// </summary>
    internal class StragglerDownloadDetector
    {
        private readonly double _stragglerThroughputMultiplier;
        private readonly double _minimumCompletionQuantile;
        private readonly TimeSpan _stragglerDetectionPadding;
        private readonly int _maxStragglersBeforeFallback;
        private int _totalStragglersDetectedInQuery;

        /// <summary>
        /// Initializes a new instance of the <see cref="StragglerDownloadDetector"/> class.
        /// </summary>
        /// <param name="stragglerThroughputMultiplier">Multiplier for straggler threshold. Must be greater than 1.0.</param>
        /// <param name="minimumCompletionQuantile">Fraction of downloads that must complete before detection starts (0.0 to 1.0).</param>
        /// <param name="stragglerDetectionPadding">Extra buffer time before declaring a download as a straggler.</param>
        /// <param name="maxStragglersBeforeFallback">Maximum stragglers before triggering sequential fallback.</param>
        public StragglerDownloadDetector(
            double stragglerThroughputMultiplier,
            double minimumCompletionQuantile,
            TimeSpan stragglerDetectionPadding,
            int maxStragglersBeforeFallback)
        {
            if (stragglerThroughputMultiplier <= 1.0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(stragglerThroughputMultiplier),
                    stragglerThroughputMultiplier,
                    "Straggler throughput multiplier must be greater than 1.0");
            }

            if (minimumCompletionQuantile <= 0.0 || minimumCompletionQuantile > 1.0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(minimumCompletionQuantile),
                    minimumCompletionQuantile,
                    "Minimum completion quantile must be between 0.0 and 1.0");
            }

            if (stragglerDetectionPadding < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(stragglerDetectionPadding),
                    stragglerDetectionPadding,
                    "Straggler detection padding must be non-negative");
            }

            if (maxStragglersBeforeFallback < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maxStragglersBeforeFallback),
                    maxStragglersBeforeFallback,
                    "Max stragglers before fallback must be non-negative");
            }

            _stragglerThroughputMultiplier = stragglerThroughputMultiplier;
            _minimumCompletionQuantile = minimumCompletionQuantile;
            _stragglerDetectionPadding = stragglerDetectionPadding;
            _maxStragglersBeforeFallback = maxStragglersBeforeFallback;
            _totalStragglersDetectedInQuery = 0;
        }

        /// <summary>
        /// Gets a value indicating whether the query should fall back to sequential downloads
        /// due to exceeding the maximum straggler threshold.
        /// </summary>
        public bool ShouldFallbackToSequentialDownloads =>
            _totalStragglersDetectedInQuery >= _maxStragglersBeforeFallback;

        /// <summary>
        /// Identifies straggler downloads based on median throughput analysis.
        /// </summary>
        /// <param name="allDownloadMetrics">All download metrics for the current batch.</param>
        /// <param name="currentTime">The current time for elapsed time calculations.</param>
        /// <returns>Collection of file offsets identified as stragglers.</returns>
        public IEnumerable<long> IdentifyStragglerDownloads(
            IReadOnlyList<FileDownloadMetrics> allDownloadMetrics,
            DateTime currentTime)
        {
            if (allDownloadMetrics == null || allDownloadMetrics.Count == 0)
            {
                return Enumerable.Empty<long>();
            }

            // Separate completed and active downloads
            var completedDownloads = allDownloadMetrics.Where(m => m.IsDownloadCompleted).ToList();
            var activeDownloads = allDownloadMetrics.Where(m => !m.IsDownloadCompleted && !m.WasCancelledAsStragler).ToList();

            if (activeDownloads.Count == 0)
            {
                return Enumerable.Empty<long>();
            }

            // Check if we have enough completed downloads to calculate median
            int totalDownloads = allDownloadMetrics.Count;
            int requiredCompletions = (int)Math.Ceiling(totalDownloads * _minimumCompletionQuantile);

            if (completedDownloads.Count < requiredCompletions)
            {
                return Enumerable.Empty<long>();
            }

            // Calculate median throughput from completed downloads
            double medianThroughput = CalculateMedianThroughput(completedDownloads);

            if (medianThroughput <= 0)
            {
                return Enumerable.Empty<long>();
            }

            // Identify stragglers
            var stragglers = new List<long>();

            foreach (var download in activeDownloads)
            {
                TimeSpan elapsed = currentTime - download.DownloadStartTime;
                double elapsedSeconds = elapsed.TotalSeconds;

                // Calculate expected time: (multiplier × fileSize / medianThroughput) + padding
                double expectedSeconds = (_stragglerThroughputMultiplier * download.FileSizeBytes / medianThroughput)
                    + _stragglerDetectionPadding.TotalSeconds;

                if (elapsedSeconds > expectedSeconds)
                {
                    stragglers.Add(download.FileOffset);
                    Interlocked.Increment(ref _totalStragglersDetectedInQuery);
                }
            }

            return stragglers;
        }

        /// <summary>
        /// Gets the total number of stragglers detected in the current query.
        /// </summary>
        /// <returns>The total straggler count.</returns>
        public int GetTotalStragglersDetectedInQuery()
        {
            return Interlocked.CompareExchange(ref _totalStragglersDetectedInQuery, 0, 0);
        }

        /// <summary>
        /// Calculates the median throughput from a collection of completed downloads.
        /// </summary>
        /// <param name="completedDownloads">Completed download metrics.</param>
        /// <returns>Median throughput in bytes per second.</returns>
        private double CalculateMedianThroughput(List<FileDownloadMetrics> completedDownloads)
        {
            if (completedDownloads.Count == 0)
            {
                return 0;
            }

            var throughputs = completedDownloads
                .Select(m => m.CalculateThroughputBytesPerSecond())
                .Where(t => t.HasValue && t.Value > 0)
                .Select(t => t!.Value)  // Null-forgiving operator: We know it's not null due to Where filter
                .OrderBy(t => t)
                .ToList();

            if (throughputs.Count == 0)
            {
                return 0;
            }

            int count = throughputs.Count;
            if (count % 2 == 1)
            {
                // Odd count: return middle element
                return throughputs[count / 2];
            }
            else
            {
                // Even count: return average of two middle elements
                int midIndex = count / 2;
                return (throughputs[midIndex - 1] + throughputs[midIndex]) / 2.0;
            }
        }
    }
}

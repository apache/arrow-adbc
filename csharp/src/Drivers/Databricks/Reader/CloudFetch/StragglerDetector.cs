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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Detects and mitigates straggler downloads in CloudFetch operations.
    /// A straggler is a download that is significantly slower than the median throughput
    /// of other downloads in the same batch.
    /// </summary>
    internal sealed class StragglerDetector
    {
        private readonly bool _enabled;
        private readonly double _multiplier;
        private readonly double _completionQuantile;
        private readonly int _paddingSeconds;
        private readonly int _maxStragglersPerQuery;
        private readonly bool _enableSequentialFallback;
        private readonly ConcurrentDictionary<long, DownloadMetrics> _allDownloads = new ConcurrentDictionary<long, DownloadMetrics>();
        private int _stragglerCount;

        /// <summary>
        /// Initializes a new instance of the <see cref="StragglerDetector"/> class.
        /// </summary>
        /// <param name="enabled">Whether straggler detection is enabled.</param>
        /// <param name="multiplier">How many times slower a download must be to be considered a straggler. Must be greater than 1.0.</param>
        /// <param name="completionQuantile">Fraction of downloads that must complete before straggler detection activates. Must be between 0.0 and 1.0.</param>
        /// <param name="paddingSeconds">Extra buffer in seconds before declaring a download as a straggler. Must be non-negative.</param>
        /// <param name="maxStragglersPerQuery">Maximum stragglers to retry per query before taking further action. Must be positive.</param>
        /// <param name="enableSequentialFallback">Whether to automatically switch to sequential download mode when max stragglers exceeded.</param>
        public StragglerDetector(
            bool enabled,
            double multiplier = 1.5,
            double completionQuantile = 0.6,
            int paddingSeconds = 5,
            int maxStragglersPerQuery = 10,
            bool enableSequentialFallback = false)
        {
            if (multiplier <= 1.0)
                throw new ArgumentOutOfRangeException(nameof(multiplier), multiplier, "Multiplier must be greater than 1.0");

            if (completionQuantile <= 0.0 || completionQuantile >= 1.0)
                throw new ArgumentOutOfRangeException(nameof(completionQuantile), completionQuantile, "CompletionQuantile must be between 0.0 and 1.0");

            if (paddingSeconds < 0)
                throw new ArgumentOutOfRangeException(nameof(paddingSeconds), paddingSeconds, "PaddingSeconds must be non-negative");

            if (maxStragglersPerQuery <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxStragglersPerQuery), maxStragglersPerQuery, "MaxStragglersPerQuery must be positive");

            _enabled = enabled;
            _multiplier = multiplier;
            _completionQuantile = completionQuantile;
            _paddingSeconds = paddingSeconds;
            _maxStragglersPerQuery = maxStragglersPerQuery;
            _enableSequentialFallback = enableSequentialFallback;
        }

        /// <summary>
        /// Gets the total number of stragglers detected in the query.
        /// </summary>
        public int StragglerCount => _stragglerCount;

        /// <summary>
        /// Gets whether straggler detection is enabled.
        /// </summary>
        public bool Enabled => _enabled;

        /// <summary>
        /// Gets whether sequential fallback is enabled.
        /// </summary>
        public bool EnableSequentialFallback => _enableSequentialFallback;

        /// <summary>
        /// Records the start of a download.
        /// </summary>
        /// <param name="offset">The row offset of the download.</param>
        /// <param name="fileSize">The size of the file in bytes.</param>
        public void RecordStartedDownload(long offset, long fileSize)
        {
            var metrics = new DownloadMetrics
            {
                Offset = offset,
                FileSize = fileSize,
                StartTime = DateTime.UtcNow,
                IsCompleted = false
            };
            _allDownloads[offset] = metrics;
        }

        /// <summary>
        /// Records the completion of a download.
        /// </summary>
        /// <param name="offset">The row offset of the download.</param>
        public void RecordCompletedDownload(long offset)
        {
            if (_allDownloads.TryGetValue(offset, out var metrics))
            {
                metrics.EndTime = DateTime.UtcNow;
                metrics.IsCompleted = true;
            }
        }

        /// <summary>
        /// Gets the median throughput of completed downloads in bytes per second.
        /// </summary>
        /// <returns>The median throughput, or 0 if no completed downloads.</returns>
        public double GetMedianThroughput()
        {
            var completedThroughputs = _allDownloads.Values
                .Where(m => m.IsCompleted && m.ThroughputBytesPerSecond > 0)
                .Select(m => m.ThroughputBytesPerSecond)
                .OrderBy(t => t)
                .ToList();

            if (completedThroughputs.Count == 0) return 0;

            int mid = completedThroughputs.Count / 2;
            return completedThroughputs.Count % 2 == 0
                ? (completedThroughputs[mid - 1] + completedThroughputs[mid]) / 2.0
                : completedThroughputs[mid];
        }

        /// <summary>
        /// Gets the list of offsets for downloads that are currently stragglers.
        /// </summary>
        /// <returns>List of row offsets for straggler downloads.</returns>
        public IReadOnlyList<long> GetStragglerOffsets()
        {
            if (!_enabled || IsMaxStragglersExceeded())
                return new List<long>();

            var completed = _allDownloads.Values.Where(m => m.IsCompleted).ToList();
            var inProgress = _allDownloads.Values.Where(m => !m.IsCompleted).ToList();
            var totalCount = _allDownloads.Count;

            if (completed.Count < totalCount * _completionQuantile)
                return new List<long>();

            double medianThroughput = GetMedianThroughput();
            if (medianThroughput <= 0)
                return new List<long>();

            return inProgress
                .Where(m => IsStraggler(m, medianThroughput))
                .Select(m => m.Offset)
                .ToList();
        }

        /// <summary>
        /// Increments the query-level straggler counter.
        /// </summary>
        public void IncrementStragglerCount()
        {
            System.Threading.Interlocked.Increment(ref _stragglerCount);
        }

        /// <summary>
        /// Checks if the maximum number of stragglers per query has been exceeded.
        /// </summary>
        /// <returns>True if max stragglers exceeded, false otherwise.</returns>
        public bool IsMaxStragglersExceeded()
        {
            return _stragglerCount >= _maxStragglersPerQuery;
        }

        /// <summary>
        /// Resets batch-level metrics. Called when a batch completes.
        /// </summary>
        public void ResetBatchMetrics()
        {
            _allDownloads.Clear();
        }

        /// <summary>
        /// Determines if a download is a straggler based on median throughput.
        /// </summary>
        /// <param name="metrics">The download metrics to check.</param>
        /// <param name="medianThroughput">The median throughput in bytes per second.</param>
        /// <returns>True if the download is a straggler, false otherwise.</returns>
        private bool IsStraggler(DownloadMetrics metrics, double medianThroughput)
        {
            if (medianThroughput <= 0) return false;

            double expectedTimeSeconds = metrics.FileSize / medianThroughput;
            double thresholdTimeSeconds = expectedTimeSeconds * _multiplier + _paddingSeconds;
            double elapsedSeconds = metrics.ElapsedMilliseconds / 1000.0;

            return elapsedSeconds > thresholdTimeSeconds;
        }

        /// <summary>
        /// Internal class to track timing and throughput metrics for a file download.
        /// </summary>
        private sealed class DownloadMetrics
        {
            public long FileSize { get; set; }
            public long Offset { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime? EndTime { get; set; }
            public bool IsCompleted { get; set; }

            public long ElapsedMilliseconds
            {
                get
                {
                    DateTime end = EndTime ?? DateTime.UtcNow;
                    return (long)(end - StartTime).TotalMilliseconds;
                }
            }

            public double ThroughputBytesPerSecond
            {
                get
                {
                    if (ElapsedMilliseconds == 0) return 0;
                    return FileSize / (ElapsedMilliseconds / 1000.0);
                }
            }
        }
    }
}

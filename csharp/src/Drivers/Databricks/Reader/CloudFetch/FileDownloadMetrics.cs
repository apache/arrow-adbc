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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch
{
    /// <summary>
    /// Tracks timing and throughput metrics for individual file downloads.
    /// </summary>
    internal class FileDownloadMetrics
    {
        private DateTime? _downloadEndTime;
        private bool _wasCancelledAsStragler;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDownloadMetrics"/> class.
        /// </summary>
        /// <param name="fileOffset">The file offset in the download batch.</param>
        /// <param name="fileSizeBytes">The size of the file in bytes.</param>
        public FileDownloadMetrics(long fileOffset, long fileSizeBytes)
        {
            FileOffset = fileOffset;
            FileSizeBytes = fileSizeBytes;
            DownloadStartTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the file offset in the download batch.
        /// </summary>
        public long FileOffset { get; }

        /// <summary>
        /// Gets the size of the file in bytes.
        /// </summary>
        public long FileSizeBytes { get; }

        /// <summary>
        /// Gets the time when the download started.
        /// </summary>
        public DateTime DownloadStartTime { get; }

        /// <summary>
        /// Gets the time when the download completed, or null if still in progress.
        /// </summary>
        public DateTime? DownloadEndTime => _downloadEndTime;

        /// <summary>
        /// Gets a value indicating whether the download has completed.
        /// </summary>
        public bool IsDownloadCompleted => _downloadEndTime.HasValue;

        /// <summary>
        /// Gets a value indicating whether this download was cancelled as a straggler.
        /// </summary>
        public bool WasCancelledAsStragler => _wasCancelledAsStragler;

        /// <summary>
        /// Calculates the download throughput in bytes per second.
        /// Returns null if the download has not completed.
        /// </summary>
        /// <returns>The throughput in bytes per second, or null if not completed.</returns>
        public double? CalculateThroughputBytesPerSecond()
        {
            if (!_downloadEndTime.HasValue)
            {
                return null;
            }

            TimeSpan elapsed = _downloadEndTime.Value - DownloadStartTime;
            double elapsedSeconds = elapsed.TotalSeconds;

            // Avoid division by zero for very fast downloads
            if (elapsedSeconds < 0.001)
            {
                elapsedSeconds = 0.001;
            }

            return FileSizeBytes / elapsedSeconds;
        }

        /// <summary>
        /// Marks the download as completed and records the end time.
        /// </summary>
        public void MarkDownloadCompleted()
        {
            _downloadEndTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Marks this download as having been cancelled due to being identified as a straggler.
        /// </summary>
        public void MarkCancelledAsStragler()
        {
            _wasCancelledAsStragler = true;
        }
    }
}

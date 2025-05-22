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
using System.IO;
using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch
{
    /// <summary>
    /// Represents a downloaded result file with its associated metadata.
    /// </summary>
    internal sealed class DownloadResult : IDownloadResult
    {
        private readonly TaskCompletionSource<bool> _downloadCompletionSource;
        private readonly ICloudFetchMemoryBufferManager _memoryManager;
        private Stream? _dataStream;
        private bool _isDisposed;
        private long _size;

        /// <summary>
        /// Initializes a new instance of the <see cref="DownloadResult"/> class.
        /// </summary>
        /// <param name="link">The link information for this result.</param>
        /// <param name="memoryManager">The memory buffer manager.</param>
        public DownloadResult(TSparkArrowResultLink link, ICloudFetchMemoryBufferManager memoryManager)
        {
            Link = link ?? throw new ArgumentNullException(nameof(link));
            _memoryManager = memoryManager ?? throw new ArgumentNullException(nameof(memoryManager));
            _downloadCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _size = link.BytesNum;
        }

        /// <inheritdoc />
        public TSparkArrowResultLink Link { get; private set; }

        /// <inheritdoc />
        public Stream DataStream
        {
            get
            {
                ThrowIfDisposed();
                if (!IsCompleted)
                {
                    throw new InvalidOperationException("Download has not completed yet.");
                }
                return _dataStream!;
            }
        }

        /// <inheritdoc />
        public long Size => _size;

        /// <inheritdoc />
        public Task DownloadCompletedTask => _downloadCompletionSource.Task;

        /// <inheritdoc />
        public bool IsCompleted => _downloadCompletionSource.Task.IsCompleted && !_downloadCompletionSource.Task.IsFaulted;

        /// <summary>
        /// Gets the number of URL refresh attempts for this download.
        /// </summary>
        public int RefreshAttempts { get; private set; } = 0;

        /// <summary>
        /// Checks if the URL is expired or about to expire.
        /// </summary>
        /// <param name="expirationBufferSeconds">Buffer time in seconds before expiration to consider a URL as expiring soon.</param>
        /// <returns>True if the URL is expired or about to expire, false otherwise.</returns>
        public bool IsExpiredOrExpiringSoon(int expirationBufferSeconds = 60)
        {
            // Convert expiry time to DateTime
            var expiryTime = DateTimeOffset.FromUnixTimeMilliseconds(Link.ExpiryTime).UtcDateTime;

            // Check if the URL is already expired or will expire soon
            return DateTime.UtcNow.AddSeconds(expirationBufferSeconds) >= expiryTime;
        }

        /// <summary>
        /// Updates this download result with a refreshed link.
        /// </summary>
        /// <param name="refreshedLink">The refreshed link information.</param>
        public void UpdateWithRefreshedLink(TSparkArrowResultLink refreshedLink)
        {
            ThrowIfDisposed();
            Link = refreshedLink ?? throw new ArgumentNullException(nameof(refreshedLink));
            RefreshAttempts++;
        }

        /// <inheritdoc />
        public void SetCompleted(Stream dataStream, long size)
        {
            ThrowIfDisposed();
            _dataStream = dataStream ?? throw new ArgumentNullException(nameof(dataStream));
            _downloadCompletionSource.TrySetResult(true);
            _size = size;
        }

        /// <inheritdoc />
        public void SetFailed(Exception exception)
        {
            ThrowIfDisposed();
            _downloadCompletionSource.TrySetException(exception ?? throw new ArgumentNullException(nameof(exception)));
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_isDisposed)
            {
                return;
            }

            if (_dataStream != null)
            {
                _dataStream.Dispose();
                _dataStream = null;

                // Release memory back to the manager
                if (_size > 0)
                {
                    _memoryManager.ReleaseMemory(_size);
                }
            }

            // Ensure any waiting tasks are completed if not already
            if (!_downloadCompletionSource.Task.IsCompleted)
            {
                _downloadCompletionSource.TrySetCanceled();
            }

            _isDisposed = true;
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(DownloadResult));
            }
        }
    }
}

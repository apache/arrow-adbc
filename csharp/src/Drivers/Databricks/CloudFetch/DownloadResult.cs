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

namespace Apache.Arrow.Adbc.Drivers.Apache.Databricks.CloudFetch
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
        public TSparkArrowResultLink Link { get; }

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

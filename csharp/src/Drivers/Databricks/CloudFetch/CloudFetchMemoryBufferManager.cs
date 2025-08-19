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
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch
{
    /// <summary>
    /// Manages memory allocation for prefetched files.
    /// </summary>
    internal sealed class CloudFetchMemoryBufferManager : ICloudFetchMemoryBufferManager
    {
        private const int DefaultMemoryBufferSizeMB = 200;
        private readonly long _maxMemory;
        private long _usedMemory;
        private readonly SemaphoreSlim _memorySemaphore;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchMemoryBufferManager"/> class.
        /// </summary>
        /// <param name="maxMemoryMB">The maximum memory allowed for buffering in megabytes.</param>
        public CloudFetchMemoryBufferManager(int? maxMemoryMB = null)
        {
            int memoryMB = maxMemoryMB ?? DefaultMemoryBufferSizeMB;
            if (memoryMB <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxMemoryMB), "Memory buffer size must be positive.");
            }

            // Convert MB to bytes
            _maxMemory = memoryMB * 1024L * 1024L;
            _usedMemory = 0;
            _memorySemaphore = new SemaphoreSlim(1, 1);
        }

        /// <inheritdoc />
        public long MaxMemory => _maxMemory;

        /// <inheritdoc />
        public long UsedMemory => Interlocked.Read(ref _usedMemory);

        /// <inheritdoc />
        public bool TryAcquireMemory(long size)
        {
            if (size <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size), "Size must be positive.");
            }

            // Try to acquire memory
            long originalValue;
            long newValue;
            do
            {
                originalValue = Interlocked.Read(ref _usedMemory);
                newValue = originalValue + size;

                // Check if we would exceed the maximum memory
                if (newValue > _maxMemory)
                {
                    return false;
                }
            }
            while (Interlocked.CompareExchange(ref _usedMemory, newValue, originalValue) != originalValue);

            return true;
        }

        /// <inheritdoc />
        public async Task AcquireMemoryAsync(long size, CancellationToken cancellationToken)
        {
            if (size <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size), "Size must be positive.");
            }

            // Special case: if size is greater than max memory, we'll never be able to acquire it
            if (size > _maxMemory)
            {
                throw new ArgumentOutOfRangeException(nameof(size), $"Requested size ({size} bytes) exceeds maximum memory ({_maxMemory} bytes).");
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                // Try to acquire memory without blocking
                if (TryAcquireMemory(size))
                {
                    return;
                }

                // If we couldn't acquire memory, wait for some to be released
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }

            // If we get here, cancellation was requested
            cancellationToken.ThrowIfCancellationRequested();
        }

        /// <inheritdoc />
        public void ReleaseMemory(long size)
        {
            if (size <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size), "Size must be positive.");
            }

            // Release memory
            long newValue = Interlocked.Add(ref _usedMemory, -size);

            // Ensure we don't go negative
            if (newValue < 0)
            {
                // This should never happen if the code is correct
                Interlocked.Exchange(ref _usedMemory, 0);
                throw new InvalidOperationException("Memory buffer manager released more memory than was acquired.");
            }
        }
    }
}

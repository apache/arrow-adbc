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
using System.Buffers;
using System.IO;
using K4os.Compression.LZ4.Encoders;
using K4os.Compression.LZ4.Streams;
using K4os.Compression.LZ4.Streams.Frames;
using K4os.Compression.LZ4.Streams.Internal;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Custom LZ4 frame reader that uses a custom ArrayPool to support pooling of 4MB+ buffers.
    /// This solves the issue where Databricks LZ4 frames declare maxBlockSize=4MB but .NET's
    /// default ArrayPool only pools buffers up to 1MB, causing 900MB of fresh LOH allocations.
    /// </summary>
    internal sealed class CustomLZ4FrameReader : StreamLZ4FrameReader
    {
        /// <summary>
        /// Custom ArrayPool that supports pooling of buffers up to 4MB.
        /// This allows the 4MB buffers required by Databricks LZ4 frames to be pooled and reused.
        /// maxArraysPerBucket=10 means we keep up to 10 buffers of each size in the pool.
        /// </summary>
        private static readonly ArrayPool<byte> LargeBufferPool =
            ArrayPool<byte>.Create(maxArrayLength: 4 * 1024 * 1024, maxArraysPerBucket: 10);

        /// <summary>
        /// Creates a new CustomLZ4FrameReader instance.
        /// </summary>
        /// <param name="stream">The stream to read compressed LZ4 data from.</param>
        /// <param name="leaveOpen">Whether to leave the stream open when disposing.</param>
        /// <param name="decoderFactory">Factory function to create the LZ4 decoder.</param>
        public CustomLZ4FrameReader(
            Stream stream,
            bool leaveOpen,
            Func<ILZ4Descriptor, ILZ4Decoder> decoderFactory)
            : base(stream, leaveOpen, decoderFactory)
        {
        }

        /// <summary>
        /// Overrides buffer allocation to use our custom ArrayPool that supports 4MB+ buffers.
        /// </summary>
        /// <param name="size">The size of buffer to allocate (typically 4MB for Databricks).</param>
        /// <returns>A buffer of at least the requested size, pooled if possible.</returns>
        protected override byte[] AllocBuffer(int size)
        {
            // Use our custom pool instead of the default BufferPool (which uses ArrayPool.Shared with 1MB limit)
            return LargeBufferPool.Rent(size);
        }

        /// <summary>
        /// Overrides buffer release to return buffers to our custom ArrayPool.
        /// </summary>
        /// <param name="buffer">The buffer to return to the pool.</param>
        protected override void ReleaseBuffer(byte[] buffer)
        {
            if (buffer != null)
            {
                // Clear the buffer to prevent stale data from previous decompressions
                // from corrupting subsequent operations. The performance overhead (~1-2ms
                // per 4MB buffer) is negligible compared to network I/O and decompression time.
                LargeBufferPool.Return(buffer, clearArray: true);
            }
        }
    }
}

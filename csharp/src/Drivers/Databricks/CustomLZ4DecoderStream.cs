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
using System.Threading;
using System.Threading.Tasks;
using K4os.Compression.LZ4.Encoders;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Custom LZ4 decoder stream that uses CustomLZ4FrameReader for buffer pooling.
    /// This replaces K4os.Compression.LZ4.Streams.LZ4DecoderStream to use our custom reader
    /// that pools 4MB+ buffers.
    ///
    /// Why not inherit from LZ4DecoderStream or LZ4StreamOnStreamEssentials?
    /// - LZ4DecoderStream directly instantiates StreamLZ4FrameReader (no injection point)
    /// - LZ4StreamOnStreamEssentials has a 'private protected' constructor (inaccessible from external assemblies)
    ///
    /// What features from K4os base classes are intentionally omitted:
    /// - Timeout support: Not needed since inner stream (MemoryStream) doesn't support timeouts
    /// - Write operations: This is a read-only decompression stream
    /// - DisposeAsync: Optional - base Stream.DisposeAsync() calls our Dispose(bool) which is sufficient
    /// </summary>
    internal sealed class CustomLZ4DecoderStream : Stream
    {
        private readonly CustomLZ4FrameReader _reader;
        private readonly Stream _inner;
        private readonly bool _leaveOpen;
        private readonly bool _interactive;
        private bool _disposed;

        /// <summary>
        /// Creates a new CustomLZ4DecoderStream instance.
        /// </summary>
        /// <param name="inner">The inner stream containing compressed LZ4 data.</param>
        /// <param name="decoderFactory">Factory function to create the LZ4 decoder.</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <param name="leaveOpen">Whether to leave the inner stream open when disposing.</param>
        /// <param name="interactive">Interactive mode - provide bytes as soon as available.</param>
        public CustomLZ4DecoderStream(
            Stream inner,
            Func<ILZ4Descriptor, ILZ4Decoder> decoderFactory,
            System.Buffers.ArrayPool<byte> bufferPool,
            bool leaveOpen = false,
            bool interactive = false)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _reader = new CustomLZ4FrameReader(inner, true, decoderFactory, bufferPool);
            _leaveOpen = leaveOpen;
            _interactive = interactive;
        }

        public override bool CanRead => !_disposed && _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;

        // Timeout properties are not implemented since:
        // - The inner stream (MemoryStream in our use case) doesn't support timeouts
        // - LZ4 decompression is CPU-bound, not I/O-bound, so timeouts don't apply
        public override bool CanTimeout => false;
        public override int ReadTimeout
        {
            get => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
            set => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
        }
        public override int WriteTimeout
        {
            get => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
            set => throw new InvalidOperationException("LZ4 decoder stream does not support timeouts");
        }

        public override long Length => _reader.GetFrameLength() ?? -1;
        public override long Position
        {
            get => _reader.GetBytesRead();
            set => throw new NotSupportedException("LZ4 stream does not support setting position");
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException("LZ4 stream does not support seeking");

        public override void SetLength(long value) =>
            throw new NotSupportedException("LZ4 stream does not support SetLength");

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException("LZ4 decoder stream does not support writing");

        public override int ReadByte() => _reader.ReadOneByte();

        public override int Read(byte[] buffer, int offset, int count) =>
            _reader.ReadManyBytes(buffer.AsSpan(offset, count), _interactive);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _reader.ReadManyBytesAsync(cancellationToken, buffer.AsMemory(offset, count), _interactive);

#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
        public override int Read(Span<byte> buffer) =>
            _reader.ReadManyBytes(buffer, _interactive);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            new(_reader.ReadManyBytesAsync(cancellationToken, buffer, _interactive));
#endif

        public override void Flush()
        {
            // No-op for read-only stream - nothing to flush since we only read
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            // No-op for read-only stream - nothing to flush since we only read
            return Task.CompletedTask;
        }

        protected override void Dispose(bool disposing)
        {
            // Double-dispose protection: only dispose once
            if (!_disposed)
            {
                if (disposing)
                {
                    // Dispose managed resources
                    _reader.Dispose();  // Returns 4MB buffer to pool
                    if (!_leaveOpen)
                    {
                        _inner?.Dispose();  // Dispose inner stream if we own it
                    }
                }
                // No unmanaged resources to clean up (no finalizer needed)
                _disposed = true;
            }
            base.Dispose(disposing);
        }
    }
}

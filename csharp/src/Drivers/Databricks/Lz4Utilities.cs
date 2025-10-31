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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using K4os.Compression.LZ4.Encoders;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Utility class for LZ4 compression/decompression operations.
    /// Uses CustomLZ4DecoderStream with custom buffer pooling to handle Databricks' 4MB LZ4 frames efficiently.
    /// </summary>
    internal static class Lz4Utilities
    {
        /// <summary>
        /// Default buffer size for LZ4 decompression operations (80KB).
        /// </summary>
        private const int DefaultBufferSize = 81920;

        /// <summary>
        /// Decompresses LZ4 compressed data into memory.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData)
        {
            return DecompressLz4(compressedData, DefaultBufferSize);
        }

        /// <summary>
        /// Decompresses LZ4 compressed data into memory with a specified buffer size.
        /// Uses CustomLZ4DecoderStream with custom ArrayPool to efficiently pool 4MB+ buffers.
        /// Pre-allocates output buffer based on compressed size to avoid MemoryStream growth reallocations.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData, int bufferSize)
        {
            try
            {
                // Estimate decompressed size based on compressed size
                // LZ4 typically achieves 2x-4x compression ratio, so we use 4x as initial estimate
                // with a minimum of 64KB and maximum of 8MB to handle edge cases
                int estimatedSize = Math.Max(64 * 1024, Math.Min(compressedData.Length * 4, 8 * 1024 * 1024));

                using (var outputStream = new MemoryStream(estimatedSize))
                {
                    using (var inputStream = new MemoryStream(compressedData))
                    using (var decompressor = new CustomLZ4DecoderStream(
                        inputStream,
                        descriptor => descriptor.CreateDecoder(),
                        leaveOpen: false,
                        interactive: false))
                    {
                        decompressor.CopyTo(outputStream, bufferSize);
                    }

                    // Get the underlying buffer and its valid length without copying
                    // The buffer remains valid after MemoryStream disposal since we hold a reference to it
                    byte[] buffer = outputStream.GetBuffer();
                    int length = (int)outputStream.Length;

                    Activity.Current?.AddEvent("lz4.decompress_sync", [
                        new("compressed_size_bytes", compressedData.Length),
                        new("estimated_size_bytes", estimatedSize),
                        new("actual_size_bytes", length),
                        new("buffer_allocated_bytes", buffer.Length),
                        new("buffer_waste_bytes", buffer.Length - length),
                        new("compression_ratio", (double)length / compressedData.Length)
                    ]);

                    return new ReadOnlyMemory<byte>(buffer, 0, length);
                }
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Asynchronously decompresses LZ4 compressed data into memory.
        /// Returns the buffer and length as a tuple for efficient wrapping in a MemoryStream.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A tuple containing the decompressed buffer and its valid length.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static Task<(byte[] buffer, int length)> DecompressLz4Async(
            byte[] compressedData,
            CancellationToken cancellationToken = default)
        {
            return DecompressLz4Async(compressedData, DefaultBufferSize, cancellationToken);
        }

        /// <summary>
        /// Asynchronously decompresses LZ4 compressed data into memory with a specified buffer size.
        /// Returns the buffer and length as a tuple for efficient wrapping in a MemoryStream.
        /// Uses CustomLZ4DecoderStream with custom ArrayPool to efficiently pool 4MB+ buffers.
        /// Pre-allocates output buffer using LZ4 frame header's content-length if available,
        /// otherwise estimates based on compressed size to avoid MemoryStream growth reallocations.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A tuple containing the decompressed buffer and its valid length.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static async Task<(byte[] buffer, int length)> DecompressLz4Async(
            byte[] compressedData,
            int bufferSize,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // First, peek at the frame header to get actual decompressed size if available
                int estimatedSize;
                bool usedFrameHeader;
                using (var peekStream = new MemoryStream(compressedData))
                using (var peekDecompressor = new CustomLZ4DecoderStream(
                    peekStream,
                    descriptor => descriptor.CreateDecoder(),
                    leaveOpen: false,
                    interactive: false))
                {
                    long? frameLength = peekDecompressor.Length;
                    if (frameLength.HasValue && frameLength.Value > 0 && frameLength.Value <= int.MaxValue)
                    {
                        // Use exact size from LZ4 frame header
                        estimatedSize = (int)frameLength.Value;
                        usedFrameHeader = true;
                    }
                    else
                    {
                        // Fallback: estimate based on compressed size
                        // LZ4 typically achieves 2x-4x compression ratio, so use 4x as estimate
                        // Minimum 64KB, no maximum cap (let it be sized appropriately)
                        estimatedSize = Math.Max(64 * 1024, compressedData.Length * 4);
                        usedFrameHeader = false;
                    }
                }

                using (var outputStream = new MemoryStream(estimatedSize))
                {
                    using (var inputStream = new MemoryStream(compressedData))
                    using (var decompressor = new CustomLZ4DecoderStream(
                        inputStream,
                        descriptor => descriptor.CreateDecoder(),
                        leaveOpen: false,
                        interactive: false))
                    {
                        await decompressor.CopyToAsync(outputStream, bufferSize, cancellationToken).ConfigureAwait(false);
                    }

                    // Get the underlying buffer and its valid length without copying
                    // The buffer remains valid after MemoryStream disposal since we hold a reference to it
                    byte[] buffer = outputStream.GetBuffer();
                    int length = (int)outputStream.Length;

                    Activity.Current?.AddEvent("lz4.decompress_async", [
                        new("compressed_size_bytes", compressedData.Length),
                        new("used_frame_header", usedFrameHeader),
                        new("estimated_size_bytes", estimatedSize),
                        new("actual_size_bytes", length),
                        new("buffer_allocated_bytes", buffer.Length),
                        new("buffer_waste_bytes", buffer.Length - length),
                        new("compression_ratio", (double)length / compressedData.Length)
                    ]);

                    return (buffer, length);
                }
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }
    }
}

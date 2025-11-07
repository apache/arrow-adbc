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
using System.Threading;
using System.Threading.Tasks;
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
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData, ArrayPool<byte> bufferPool)
        {
            return DecompressLz4(compressedData, DefaultBufferSize, bufferPool);
        }

        /// <summary>
        /// Decompresses LZ4 compressed data into memory with a specified buffer size.
        /// Uses CustomLZ4DecoderStream with custom ArrayPool to efficiently pool 4MB+ buffers.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData, int bufferSize, ArrayPool<byte> bufferPool)
        {
            try
            {
                using (var outputStream = new MemoryStream())
                {
                    using (var inputStream = new MemoryStream(compressedData))
                    using (var decompressor = new CustomLZ4DecoderStream(
                        inputStream,
                        descriptor => descriptor.CreateDecoder(),
                        bufferPool,
                        leaveOpen: false,
                        interactive: false))
                    {
                        decompressor.CopyTo(outputStream, bufferSize);
                    }

                    // Get the underlying buffer and its valid length without copying
                    // The buffer remains valid after MemoryStream disposal since we hold a reference to it
                    byte[] buffer = outputStream.GetBuffer();
                    int length = (int)outputStream.Length;

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
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A tuple containing the decompressed buffer and its valid length.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static Task<(byte[] buffer, int length)> DecompressLz4Async(
            byte[] compressedData,
            ArrayPool<byte> bufferPool,
            CancellationToken cancellationToken = default)
        {
            return DecompressLz4Async(compressedData, DefaultBufferSize, bufferPool, cancellationToken);
        }

        /// <summary>
        /// Asynchronously decompresses LZ4 compressed data into memory with a specified buffer size.
        /// Returns the buffer and length as a tuple for efficient wrapping in a MemoryStream.
        /// Uses CustomLZ4DecoderStream with custom ArrayPool to efficiently pool 4MB+ buffers.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <param name="bufferPool">The ArrayPool to use for buffer allocation (from DatabricksDatabase).</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A tuple containing the decompressed buffer and its valid length.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static async Task<(byte[] buffer, int length)> DecompressLz4Async(
            byte[] compressedData,
            int bufferSize,
            ArrayPool<byte> bufferPool,
            CancellationToken cancellationToken = default)
        {
            try
            {
                using (var outputStream = new MemoryStream())
                {
                    using (var inputStream = new MemoryStream(compressedData))
                    using (var decompressor = new CustomLZ4DecoderStream(
                        inputStream,
                        descriptor => descriptor.CreateDecoder(),
                        bufferPool,
                        leaveOpen: false,
                        interactive: false))
                    {
                        await decompressor.CopyToAsync(outputStream, bufferSize, cancellationToken).ConfigureAwait(false);
                    }

                    // Get the underlying buffer and its valid length without copying
                    // The buffer remains valid after MemoryStream disposal since we hold a reference to it
                    byte[] buffer = outputStream.GetBuffer();
                    int length = (int)outputStream.Length;

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

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
using K4os.Compression.LZ4.Streams;
using Microsoft.IO;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Utility class for LZ4 compression/decompression operations.
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
        /// NOTE: This method uses regular MemoryStream (not RecyclableMemoryStream) because the buffer
        /// must remain valid after the method returns for the caller to use.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData, int bufferSize)
        {
            try
            {
                // Use regular MemoryStream here because we return the buffer after disposal
                // RecyclableMemoryStream would return the buffer to the pool, making it unsafe
                using (var outputStream = new MemoryStream())
                {
                    using (var inputStream = new MemoryStream(compressedData))
                    using (var decompressor = LZ4Stream.Decode(inputStream))
                    {
                        decompressor.CopyTo(outputStream, bufferSize);
                    }

                    // Get the underlying buffer and its valid length without copying
                    // The buffer remains valid after MemoryStream disposal since we hold a reference to it
                    byte[] buffer = outputStream.GetBuffer();
                    return new ReadOnlyMemory<byte>(buffer, 0, (int)outputStream.Length);
                }
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Asynchronously decompresses LZ4 compressed data into memory.
        /// Returns a RecyclableMemoryStream that must be disposed by the caller.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="memoryStreamManager">The RecyclableMemoryStreamManager to use (from DatabricksDatabase).</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A RecyclableMemoryStream containing the decompressed data. Caller must dispose.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static Task<RecyclableMemoryStream> DecompressLz4Async(
            byte[] compressedData,
            RecyclableMemoryStreamManager memoryStreamManager,
            CancellationToken cancellationToken = default)
        {
            return DecompressLz4Async(compressedData, DefaultBufferSize, memoryStreamManager, cancellationToken);
        }

        /// <summary>
        /// Asynchronously decompresses LZ4 compressed data into memory with a specified buffer size.
        /// Returns a RecyclableMemoryStream that must be disposed by the caller to return the buffer to the pool.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="bufferSize">The buffer size to use for decompression operations.</param>
        /// <param name="memoryStreamManager">The RecyclableMemoryStreamManager to use (from DatabricksDatabase).</param>
        /// <param name="cancellationToken">Cancellation token for the async operation.</param>
        /// <returns>A RecyclableMemoryStream containing the decompressed data. Caller must dispose.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static async Task<RecyclableMemoryStream> DecompressLz4Async(
            byte[] compressedData,
            int bufferSize,
            RecyclableMemoryStreamManager memoryStreamManager,
            CancellationToken cancellationToken = default)
        {
            try
            {
                // Use RecyclableMemoryStream for pooled memory allocation
                var outputStream = memoryStreamManager.GetStream();
                try
                {
                    using (var inputStream = new MemoryStream(compressedData))
                    using (var decompressor = LZ4Stream.Decode(inputStream))
                    {
                        await decompressor.CopyToAsync(outputStream, bufferSize, cancellationToken).ConfigureAwait(false);
                    }

                    // Reset position to beginning for reading
                    outputStream.Position = 0;
                    return outputStream;
                }
                catch
                {
                    // If an error occurs, dispose the stream to return it to the pool
                    outputStream?.Dispose();
                    throw;
                }
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }
    }
}

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

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    /// <summary>
    /// Utility class for LZ4 compression/decompression operations.
    /// </summary>
    internal static class Lz4Utilities
    {
        /// <summary>
        /// Decompresses LZ4 compressed data into a memory stream.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <param name="cancellationToken">A cancellation token.</param>
        /// <returns>A memory stream containing the decompressed data, positioned at the beginning of the stream.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static async Task<MemoryStream> DecompressLz4Async(byte[] compressedData, CancellationToken cancellationToken = default)
        {
            try
            {
                var outputStream = new MemoryStream();
                using (var inputStream = new MemoryStream(compressedData))
                using (var decompressor = LZ4Stream.Decode(inputStream))
                {
                    await decompressor.CopyToAsync(outputStream);
                }
                outputStream.Position = 0;
                return outputStream;
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }
    }
} 

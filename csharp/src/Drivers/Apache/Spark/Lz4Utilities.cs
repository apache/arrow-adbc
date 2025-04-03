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

        /// <summary>
        /// Checks if the provided data appears to be LZ4 compressed.
        /// </summary>
        /// <param name="data">The data to check.</param>
        /// <returns>True if the data has the LZ4 frame magic number at the beginning, false otherwise.</returns>
        public static bool IsLz4Compressed(byte[] data)
        {
            // LZ4 frame format starts with a 4-byte magic number: [0x04, 0x22, 0x4D, 0x18]
            if (data == null || data.Length < 4)
            {
                return false;
            }

            return data[0] == 0x04 && data[1] == 0x22 && data[2] == 0x4D && data[3] == 0x18;
        }
    }
} 

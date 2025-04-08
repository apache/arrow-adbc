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
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Utility class for LZ4 compression/decompression operations.
    /// </summary>
    internal static class Lz4Utilities
    {
        /// <summary>
        /// Decompresses LZ4 compressed data into memory.
        /// </summary>
        /// <param name="compressedData">The compressed data bytes.</param>
        /// <returns>A ReadOnlyMemory containing the decompressed data.</returns>
        /// <exception cref="AdbcException">Thrown when decompression fails.</exception>
        public static ReadOnlyMemory<byte> DecompressLz4(byte[] compressedData)
        {
            try
            {
                var outputStream = new MemoryStream();
                using (var inputStream = new MemoryStream(compressedData))
                using (var decompressor = LZ4Stream.Decode(inputStream))
                {
                    decompressor.CopyTo(outputStream);
                }
                // Get the underlying buffer and its valid length without copying
                return new ReadOnlyMemory<byte>(outputStream.GetBuffer(), 0, (int)outputStream.Length);
                // Note: We're not disposing the outputStream here because we're returning its buffer.
                // The memory will be reclaimed when the ReadOnlyMemory is no longer referenced.
            }
            catch (Exception ex)
            {
                throw new AdbcException($"Failed to decompress LZ4 data: {ex.Message}", ex);
            }
        }
    }
}

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
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    public static class StreamExtensions
    {
        public static void WriteInt32LittleEndian(int value, Span<byte> buffer, int offset)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0 || offset > buffer.Length - sizeof(int))
                throw new ArgumentOutOfRangeException(nameof(offset), "Offset is outside the bounds of the buffer.");

            // Ensure the buffer is large enough to hold an int starting from the offset
            if (buffer.Length < offset + sizeof(int))
                throw new ArgumentException("Buffer too small to write an Int32 at the specified offset.");

            // Write the integer in little-endian format
            buffer[offset] = (byte)value;
            buffer[offset + 1] = (byte)(value >> 8);
            buffer[offset + 2] = (byte)(value >> 16);
            buffer[offset + 3] = (byte)(value >> 24);
        }

        public static void ReverseEndianI64AtOffset(Span<byte> buffer, int offset)
        {
            // Check if the buffer is large enough to contain an i64 at the given offset
            if (offset < 0 || buffer.Length < offset + sizeof(long))
                throw new ArgumentOutOfRangeException(nameof(offset), "Buffer is too small or offset is out of bounds.");

            // Swap the bytes to reverse the endianness of the i64
            byte temp;
            for (int startIndex = offset, endIndex = offset + (sizeof(long) - 1); startIndex < endIndex; startIndex++, endIndex--)
            {
                temp = buffer[startIndex];
                buffer[startIndex] = buffer[endIndex];
                buffer[endIndex] = temp;
            }
        }

        public static void ReverseEndianI32AtOffset(Span<byte> buffer, int offset)
        {
            // Check if the buffer is large enough to contain an i32 at the given offset
            if (offset < 0 || buffer.Length < offset + sizeof(int))
                throw new ArgumentException("Buffer is too small or offset is out of bounds.");

            // Swap the bytes to reverse the endianness of the i32
            // buffer[offset] and buffer[offset + 3]
            // buffer[offset + 1] and buffer[offset + 2]
            byte temp;

            temp = buffer[offset];
            buffer[offset] = buffer[offset + 3];
            buffer[offset + 3] = temp;

            temp = buffer[offset + 1];
            buffer[offset + 1] = buffer[offset + 2];
            buffer[offset + 2] = temp;
        }

        public static void ReverseEndiannessInt16(Span<byte> buffer, int offset)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0 || offset > buffer.Length - sizeof(short))
                throw new ArgumentOutOfRangeException(nameof(offset), "Offset is outside the bounds of the buffer.");

            // Swap the bytes to reverse the endianness of a 16-bit integer
            (buffer[offset], buffer[offset + 1]) = (buffer[offset + 1], buffer[offset]);
        }

        public static TValue? GetValueOrDefault<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> dictionary, TKey key, TValue? defaultValue = default)
        {
            if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));

            return dictionary.TryGetValue(key, out TValue? value) ? value : defaultValue;
        }

        public static async Task<bool> ReadExactlyAsync(this Stream stream, Memory<byte> memory, CancellationToken cancellationToken = default)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));

            // Try to get the underlying array from the Memory<byte>
            if (!MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> arraySegment))
            {
                throw new InvalidOperationException("The provided Memory<byte> does not have an accessible underlying array.");
            }

            int totalBytesRead = 0;
            int count = memory.Length;

            while (totalBytesRead < count)
            {
                int bytesRead = await stream.ReadAsync(arraySegment.Array!, arraySegment.Offset + totalBytesRead, count - totalBytesRead, cancellationToken).ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    // End of the stream reached before reading the desired amount
                    return totalBytesRead == 0;
                }

                totalBytesRead += bytesRead;
            }

            return true;
        }

        public static async Task<bool> ReadExactlyAsync(this Stream stream, byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || offset >= buffer.Length) throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0 || (count + offset) > buffer.Length) throw new ArgumentOutOfRangeException(nameof(count));

            int bytesReadTotal = 0;

            while (bytesReadTotal < count)
            {
                int bytesRead = await stream.ReadAsync(buffer, offset + bytesReadTotal, count - bytesReadTotal, cancellationToken).ConfigureAwait(false);

                // If ReadAsync returns 0, it means the end of the stream has been reached
                if (bytesRead == 0)
                {
                    // If we haven't read any bytes at all, it's okay (might be at the end of the stream)
                    // But if we've read some bytes and then hit the end of the stream, it's unexpected
                    return bytesReadTotal == 0;
                }

                bytesReadTotal += bytesRead;
            }

            return true;
        }
    }
}

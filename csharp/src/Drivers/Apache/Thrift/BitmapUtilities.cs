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
using System.Runtime.CompilerServices;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    internal static class BitmapUtilities
    {
        private static readonly byte[] s_bitMasks = [0, 0b00000001, 0b00000011, 0b00000111, 0b00001111, 0b00011111, 0b00111111, 0b01111111, 0b11111111];

        /// <summary>
        /// Gets the "validity" bitmap buffer from a 'nulls' bitmap.
        /// </summary>
        /// <param name="nulls">The bitmap of rows where the value is a null value (i.e., "invalid")</param>
        /// <param name="arrayLength">The length of the array.</param>
        /// <param name="nullCount">Returns the number of bits set in the bitmap.</param>
        /// <returns>A <see cref="ArrowBuffer"/> bitmap of "valid" rows (i.e., not null values).</returns>
        /// <remarks>Inverts the bits in the incoming bitmap to reverse the null to valid indicators.</remarks>
        internal static ArrowBuffer GetValidityBitmapBuffer(ref byte[] nulls, int arrayLength, out int nullCount)
        {
            nullCount = BitUtility.CountBits(nulls);
            int fullBytes = arrayLength / 8;
            int fullInts = fullBytes / 4;
            int remainingBits = arrayLength % 8;
            int requiredBytes = fullBytes + (remainingBits == 0 ? 0 : 1);
            int remainingBytes = fullBytes % 4;
            if (nulls.Length < requiredBytes)
            {
                // Note: Spark may return a nulls bitmap buffer that is shorter than required - implying that missing bits indicate non-null.
                // However, since we need to invert the bits and return a "validity" bitmap, we need to have a full length bitmap.
                byte[] temp = new byte[requiredBytes];
                nulls.CopyTo(temp, 0);
                nulls = temp;
            }

            // Handle full integers
#if NET6_0_OR_GREATER
            Memory<byte> byteMemory = nulls.AsMemory(0, fullInts * 4);
            Memory<int> intMemory = Unsafe.As<Memory<byte>, Memory<int>>(ref byteMemory);
            for (int i = 0; i < fullInts; i++)
            {
                intMemory.Span[i] = ~intMemory.Span[i];
            }
#else
            for (int i = 0; i < fullInts; i++)
            {
                BitConverter.GetBytes(~BitConverter.ToInt32(nulls, i * 4)).CopyTo(nulls, i * 4);
            }
#endif
            // Handle remaining full bytes
            if (remainingBytes > 0)
            {
                for (int i = fullInts * 4; i < fullBytes; i++)
                {
                    nulls[i] = (byte)~nulls[i];
                }
            }
            // Handle remaing bits
            if (remainingBits > 0)
            {
                int lastByteIndex = requiredBytes - 1;
                nulls[lastByteIndex] = (byte)(s_bitMasks[remainingBits] & (byte)~nulls[lastByteIndex]);
            }
            return new ArrowBuffer(nulls);
        }
    }
}

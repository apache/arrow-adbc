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

using static Apache.Arrow.ArrowBuffer;

namespace Apache.Arrow.Adbc.Drivers.Apache.Thrift
{
    internal static class BitmapUtilities
    {
        /// <summary>
        /// Gets the "validity" bitmap buffer from a 'nulls' bitmap.
        /// </summary>
        /// <param name="nulls">The bitmap of rows where the value is a null value (i.e., "invalid")</param>
        /// <param name="nullCount">Returns the number of bits set in the bitmap.</param>
        /// <returns>A <see cref="ArrowBuffer"/> bitmap of "valid" rows (i.e., not null values).</returns>
        /// <remarks>Inverts the bits in the incoming bitmap to reverse the null to valid indicators.</remarks>
        internal static ArrowBuffer GetValidityBitmapBuffer(byte[] nulls, out int nullCount)
        {
            nullCount = BitUtility.CountBits(nulls);
            for (int i = 0; i < nullCount; i++)
            {
                BitUtility.ToggleBit(nulls, i);
            }
            return new ArrowBuffer(nulls);
        }
    }
}

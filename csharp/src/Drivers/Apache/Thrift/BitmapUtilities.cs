using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            byte[] valids = new byte[nulls.Length];
            for (int i = 0; i < nulls.Length; i++)
            {
                valids[i] = (byte)~nulls[i];
            }
            nullCount = BitUtility.CountBits(nulls);
            return new ArrowBuffer(valids);
        }
    }
}

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
using System.Globalization;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Conversions shared by the default implementations of the typed option
    /// accessors on <see cref="AdbcDatabase"/>, <see cref="AdbcConnection"/>
    /// and <see cref="AdbcStatement"/>.
    /// </summary>
    internal static class AdbcOptionValue
    {
        /// <summary>
        /// Converts a value supplied through an <c>object</c>-typed API into the
        /// string form used by <c>SetOption(string, string)</c>.
        /// </summary>
        public static string ToOptionString(string key, object? value)
        {
            switch (value)
            {
                case string s: return s;
                case bool b: return AdbcOptions.GetEnabled(b);
                case long l: return FormatInt64(l);
                case int i: return FormatInt64(i);
                case short s16: return FormatInt64(s16);
                case sbyte s8: return FormatInt64(s8);
                case uint u32: return FormatInt64(u32);
                case ushort u16: return FormatInt64(u16);
                case byte u8: return FormatInt64(u8);
                case double d: return FormatDouble(d);
                case float f: return FormatDouble(f);
                case null: throw new ArgumentNullException(nameof(value), $"Value for option '{key}' must not be null");
                default:
                    throw new AdbcException(
                        $"Value of type {value.GetType().Name} for option '{key}' cannot be converted to a string",
                        AdbcStatusCode.InvalidArgument);
            }
        }

        /// <summary>
        /// Gets the bytes of a binary value supplied through an <c>object</c>-typed API.
        /// </summary>
        public static bool TryGetBytes(object? value, out ReadOnlyMemory<byte> bytes)
        {
            switch (value)
            {
                case byte[] array: bytes = array; return true;
                case ReadOnlyMemory<byte> memory: bytes = memory; return true;
                case Memory<byte> memory: bytes = memory; return true;
                case ArraySegment<byte> segment: bytes = segment; return true;
                default: bytes = default; return false;
            }
        }

        /// <summary>
        /// Gets an integer value supplied through an <c>object</c>-typed API.
        /// </summary>
        public static bool TryGetInt64(object? value, out long result)
        {
            switch (value)
            {
                case long l: result = l; return true;
                case int i: result = i; return true;
                case short s16: result = s16; return true;
                case sbyte s8: result = s8; return true;
                case uint u32: result = u32; return true;
                case ushort u16: result = u16; return true;
                case byte u8: result = u8; return true;
                default: result = 0; return false;
            }
        }

        /// <summary>
        /// Gets a floating-point value supplied through an <c>object</c>-typed API.
        /// </summary>
        public static bool TryGetDouble(object? value, out double result)
        {
            switch (value)
            {
                case double d: result = d; return true;
                case float f: result = f; return true;
                default: result = 0; return false;
            }
        }

        public static string FormatInt64(long value) => value.ToString(CultureInfo.InvariantCulture);

        public static string FormatDouble(double value)
        {
            string result = value.ToString("R", CultureInfo.InvariantCulture);

            // On .NET Framework, "R" doesn't always round-trip; "G17" always does, but
            // gives longer strings for values like 0.1, so it's only used when needed.
            if (!value.Equals(double.Parse(result, NumberStyles.Float, CultureInfo.InvariantCulture)))
            {
                result = value.ToString("G17", CultureInfo.InvariantCulture);
            }
            return result;
        }

        public static long ParseInt64(string key, string value)
        {
            if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long result))
            {
                return result;
            }
            throw new AdbcException($"Value of option '{key}' is not an integer", AdbcStatusCode.InvalidArgument);
        }

        public static double ParseDouble(string key, string value)
        {
            if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double result))
            {
                return result;
            }
            throw new AdbcException($"Value of option '{key}' is not a number", AdbcStatusCode.InvalidArgument);
        }

        public static AdbcException NotFound(string key) =>
            new AdbcException($"Option '{key}' is not recognized or has no value", AdbcStatusCode.NotFound);
    }
}

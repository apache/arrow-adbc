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
using System.Buffers.Text;
using System.Numerics;
using System.Text;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal static class DecimalUtility
    {
        private const byte AsciiZero = (byte)'0';
        private const int AsciiDigitMaxIndex = '9' - AsciiZero;
        private const byte AsciiMinus = (byte)'-';
        private const byte AsciiPlus = (byte)'+';
        private const byte AsciiUpperE = (byte)'E';
        private const byte AsciiLowerE = (byte)'e';
        private const byte AsciiPeriod = (byte)'.';
        private const byte AsciiSpace = (byte)' ';

        /// <summary>
        /// Gets the BigInteger bytes for the given string value.
        /// </summary>
        /// <param name="value">The numeric string value to get bytes for.</param>
        /// <param name="precision">The decimal precision for the target Decimal[128|256]</param>
        /// <param name="scale">The decimal scale for the target Decimal[128|256]</param>
        /// <param name="byteWidth">The width in bytes for the target buffer. Should match the length of the bytes parameter.</param>
        /// <param name="bytes">The buffer to place the BigInteger bytes into.</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        internal static void GetBytes(ReadOnlySpan<byte> value, int precision, int scale, int byteWidth, Span<byte> bytes)
        {
            if (precision < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(precision), precision, "precision value must be greater than zero.");
            }
            if (scale < 0 || scale > precision)
            {
                throw new ArgumentOutOfRangeException(nameof(scale), scale, "scale value must be in the range 0 .. precision.");
            }
            if (byteWidth > bytes.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(byteWidth), byteWidth, $"value for byteWidth {byteWidth} exceeds the the size of bytes.");
            }

            BigInteger integerValue = ToBigInteger(value, precision, scale);

            FillBytes(bytes, integerValue, byteWidth);
        }

        private static void FillBytes(Span<byte> bytes, BigInteger integerValue, int byteWidth)
        {
            int bytesWritten = 0;
#if NETCOREAPP
            if (!integerValue.TryWriteBytes(bytes, out bytesWritten, false, !BitConverter.IsLittleEndian))
            {
                throw new OverflowException("Could not extract bytes from integer value " + integerValue);
            }
#else
            byte[] tempBytes = integerValue.ToByteArray();
            bytesWritten = tempBytes.Length;
            if (bytesWritten > byteWidth)
            {
                throw new OverflowException($"Decimal size greater than {byteWidth} bytes: {bytesWritten}");
            }
            tempBytes.CopyTo(bytes);
#endif
            byte fillByte = (byte)(integerValue < 0 ? 255 : 0);
            for (int i = bytesWritten; i < byteWidth; i++)
            {
                bytes[i] = fillByte;
            }
        }

        private static BigInteger ToBigInteger(ReadOnlySpan<byte> value, int precision, int scale)
        {
            ReadOnlySpan<byte> significantValue = GetSignificantValue(value, precision, scale);
#if NETCOREAPP
            // We can rely on the fact that all the characters in the span have already been confirmed to be ASCII (i.e., < 128)
            Span<char> chars = stackalloc char[significantValue.Length];
            Encoding.UTF8.GetChars(significantValue, chars);
            return BigInteger.Parse(chars);
#else
            return BigInteger.Parse(Encoding.UTF8.GetString(significantValue));
#endif
        }

        private static ReadOnlySpan<byte> GetSignificantValue(ReadOnlySpan<byte> value, int precision, int scale)
        {
            ParseDecimal(value, out ParserState state);

            ProcessDecimal(value,
                precision,
                scale,
                state,
                out byte sign,
                out ReadOnlySpan<byte> integerSpan,
                out ReadOnlySpan<byte> fractionalSpan,
                out int neededScale);

            Span<byte> significant = new byte[precision + 1];
            BuildSignificantValue(
                sign,
                scale,
                integerSpan,
                fractionalSpan,
                neededScale,
                significant);

            return significant;
        }

        private static void ProcessDecimal(ReadOnlySpan<byte> value, int precision, int scale, ParserState state, out byte sign, out ReadOnlySpan<byte> integerSpan, out ReadOnlySpan<byte> fractionalSpan, out int neededScale)
        {
            int int_length = 0;
            int frac_length = 0;
            int exponent = 0;

            if (state.IntegerStart != -1 && state.IntegerEnd != -1) int_length = state.IntegerEnd - state.IntegerStart + 1;
            if (state.FractionalStart != -1 && state.FractionalEnd != -1) frac_length = state.FractionalEnd - state.FractionalStart + 1;
            if (state.ExponentIndex != -1 && state.ExponentStart != -1 && state.ExponentEnd != -1 && state.ExponentEnd >= state.ExponentStart)
            {
                int expStart = state.ExpSignIndex != -1 ? state.ExpSignIndex : state.ExponentStart;
                int expLength = state.ExponentEnd - expStart + 1;
                ReadOnlySpan<byte> exponentSpan = value.Slice(expStart, expLength);
                if (!Utf8Parser.TryParse(exponentSpan, out exponent, out int _))
                {
                    throw new FormatException($"unable to parse exponent value '{Encoding.UTF8.GetString(exponentSpan)}'");
                }
            }
            integerSpan = int_length > 0 ? value.Slice(state.IntegerStart, state.IntegerEnd - state.IntegerStart + 1) : [];
            fractionalSpan = frac_length > 0 ? value.Slice(state.FractionalStart, state.FractionalEnd - state.FractionalStart + 1) : [];
            Span<byte> tempSignificant;
            if (exponent != 0)
            {
                tempSignificant = new byte[int_length + frac_length];
                if (int_length > 0) value.Slice(state.IntegerStart, state.IntegerEnd - state.IntegerStart + 1).CopyTo(tempSignificant.Slice(0));
                if (frac_length > 0) value.Slice(state.FractionalStart, state.FractionalEnd - state.FractionalStart + 1).CopyTo(tempSignificant.Slice(int_length));
                // Trim trailing zeros from combined string
                while (tempSignificant[tempSignificant.Length - 1] == AsciiZero)
                {
                    tempSignificant = tempSignificant.Slice(0, tempSignificant.Length - 1);
                }
                // Recalculate integer and fractional length
                if (exponent > 0)
                {
                    int_length = Math.Min(int_length + exponent, tempSignificant.Length);
                    frac_length = Math.Max(Math.Min(frac_length - exponent, tempSignificant.Length - int_length), 0);
                }
                else
                {
                    int_length = Math.Max(int_length + exponent, 0);
                    frac_length = Math.Max(Math.Min(frac_length - exponent, tempSignificant.Length - int_length), 0);
                }
                // Reset the integer and fractional span
                fractionalSpan = tempSignificant.Slice(int_length, frac_length);
                integerSpan = tempSignificant.Slice(0, int_length);
                // Trim leading zeros from new integer span
                while (integerSpan.Length > 0 && integerSpan[0] == AsciiZero)
                {
                    integerSpan = integerSpan.Slice(1);
                    int_length -= 1;
                }
            }

            int neededPrecision = int_length + frac_length;
            neededScale = frac_length;
            if (neededPrecision > precision)
            {
                throw new OverflowException($"Decimal precision cannot be greater than that in the Arrow vector: {Encoding.UTF8.GetString(value)} has precision > {precision}");
            }
            if (neededScale > scale)
            {
                throw new OverflowException($"Decimal scale cannot be greater than that in the Arrow vector: {Encoding.UTF8.GetString(value)} has scale > {scale}");
            }
            sign = state.SignIndex != -1 ? value[state.SignIndex] : AsciiPlus;
        }

        private static void BuildSignificantValue(
            byte sign,
            int scale,
            ReadOnlySpan<byte> integerSpan,
            ReadOnlySpan<byte> fractionalSpan,
            int neededScale,
            Span<byte> significant)
        {
            significant[0] = sign;
            int end = 0;
            integerSpan.CopyTo(significant.Slice(end + 1));
            end += integerSpan.Length;
            fractionalSpan.CopyTo(significant.Slice(end + 1));
            end += fractionalSpan.Length;

            // Add trailing zeros to adjust for scale
            while (neededScale < scale)
            {
                neededScale++;
                end++;
                significant[end] = AsciiZero;
            }
        }

        private enum ParseState
        {
            StartWhiteSpace,
            SignOrDigitOrDecimal,
            DigitOrDecimalOrExponent,
            FractionOrExponent,
            ExpSignOrExpValue,
            ExpValue,
            EndWhiteSpace,
            Invalid,
        }

        private struct ParserState
        {
            public ParseState CurrentState = ParseState.StartWhiteSpace;
            public int SignIndex = -1;
            public int IntegerStart = -1;
            public int IntegerEnd = -1;
            public int DecimalIndex = -1;
            public int FractionalStart = -1;
            public int FractionalEnd = -1;
            public int ExponentIndex = -1;
            public int ExpSignIndex = -1;
            public int ExponentStart = -1;
            public int ExponentEnd = -1;
            public bool HasZero = false;

            public ParserState() { }
        }

        private static void ParseDecimal(ReadOnlySpan<byte> value, out ParserState parserState)
        {
            ParserState state = new();
            int index = 0;
            int length = value.Length;
            while (index < length)
            {
                byte c = value[index];
                switch (state.CurrentState)
                {
                    case ParseState.StartWhiteSpace:
                        if (c != AsciiSpace)
                        {
                            state.CurrentState = ParseState.SignOrDigitOrDecimal;
                        }
                        else
                        {
                            index++;
                        }
                        break;
                    case ParseState.SignOrDigitOrDecimal:
                        // Is Ascii Numeric
                        if ((uint)(c - AsciiZero) <= AsciiDigitMaxIndex)
                        {
                            if (!state.HasZero && c == AsciiZero) state.HasZero |= true;
                            state.IntegerStart = index;
                            state.IntegerEnd = index;
                            index++;
                            state.CurrentState = ParseState.DigitOrDecimalOrExponent;
                        }
                        else if (c == AsciiMinus || c == AsciiPlus)
                        {
                            state.SignIndex = index;
                            index++;
                            state.CurrentState = ParseState.DigitOrDecimalOrExponent;
                        }
                        else if (c == AsciiPeriod)
                        {
                            state.DecimalIndex = index;
                            index++;
                            state.CurrentState = ParseState.FractionOrExponent;
                        }
                        else if (c == AsciiSpace)
                        {
                            index++;
                            state.CurrentState = ParseState.EndWhiteSpace;
                        }
                        else
                        {
                            state.CurrentState = ParseState.Invalid;
                        }
                        break;
                    case ParseState.DigitOrDecimalOrExponent:
                        // Is Ascii Numeric
                        if ((uint)(c - AsciiZero) <= AsciiDigitMaxIndex)
                        {
                            if (state.IntegerStart == -1) state.IntegerStart = index;
                            if (!state.HasZero && c == AsciiZero) state.HasZero |= true;
                            state.IntegerEnd = index;
                            index++;
                        }
                        else if (c == AsciiPeriod)
                        {
                            state.DecimalIndex = index;
                            index++;
                            state.CurrentState = ParseState.FractionOrExponent;
                        }
                        else if (c == AsciiUpperE || c == AsciiLowerE)
                        {
                            state.ExponentIndex = index;
                            index++;
                            state.CurrentState = ParseState.ExpSignOrExpValue;
                        }
                        else if (c == AsciiSpace)
                        {
                            index++;
                            state.CurrentState = ParseState.EndWhiteSpace;
                        }
                        else
                        {
                            state.CurrentState = ParseState.Invalid;
                        }
                        break;
                    case ParseState.FractionOrExponent:
                        // Is Ascii Numeric
                        if ((uint)(c - AsciiZero) <= AsciiDigitMaxIndex)
                        {
                            if (state.FractionalStart == -1) state.FractionalStart = index;
                            if (!state.HasZero && c == AsciiZero) state.HasZero |= true;
                            state.FractionalEnd = index;
                            index++;
                        }
                        else if (c == AsciiUpperE || c == AsciiLowerE)
                        {
                            state.ExponentIndex = index;
                            index++;
                            state.CurrentState = ParseState.ExpSignOrExpValue;
                        }
                        else if (c == AsciiSpace)
                        {
                            index++;
                            state.CurrentState = ParseState.EndWhiteSpace;
                        }
                        else
                        {
                            state.CurrentState = ParseState.Invalid;
                        }
                        break;
                    case ParseState.ExpSignOrExpValue:
                        // Is Ascii Numeric
                        if ((uint)(c - AsciiZero) <= AsciiDigitMaxIndex)
                        {
                            if (state.ExponentStart == -1) state.ExponentStart = index;
                            state.ExponentEnd = index;
                            index++;
                            state.CurrentState = ParseState.ExpValue;
                        }
                        else if (c == AsciiMinus || c == AsciiPlus)
                        {
                            state.ExpSignIndex = index;
                            index++;
                            state.CurrentState = ParseState.ExpValue;
                        }
                        else if (c == AsciiSpace)
                        {
                            index++;
                            state.CurrentState = ParseState.EndWhiteSpace;
                        }
                        else
                        {
                            state.CurrentState = ParseState.Invalid;
                        }
                        break;
                    case ParseState.ExpValue:
                        // Is Ascii Numeric
                        if ((uint)(c - AsciiZero) <= AsciiDigitMaxIndex)
                        {
                            if (state.ExponentStart == -1) state.ExponentStart = index;
                            state.ExponentEnd = index;
                            index++;
                        }
                        else if (c == AsciiSpace)
                        {
                            index++;
                            state.CurrentState = ParseState.EndWhiteSpace;
                        }
                        else
                        {
                            state.CurrentState = ParseState.Invalid;
                        }
                        break;
                    case ParseState.EndWhiteSpace:
                        if (c == AsciiSpace)
                        {
                            index++;
                            state.CurrentState = ParseState.EndWhiteSpace;
                        }
                        else
                        {
                            state.CurrentState = ParseState.Invalid;
                        }
                        break;
                    case ParseState.Invalid:
                        throw new ArgumentOutOfRangeException(nameof(value), Encoding.UTF8.GetString(value), $"Invalid numeric value at index {index}.");
                }
            }
            // Trim leading zeros from integer portion
            if (state.IntegerStart != -1 && state.IntegerEnd != -1)
            {
                for (int i = state.IntegerStart; i <= state.IntegerEnd; i++)
                {
                    if (value[i] != AsciiZero) break;

                    state.IntegerStart = i + 1;
                    if (state.IntegerStart > state.IntegerEnd)
                    {
                        state.IntegerStart = -1;
                        state.IntegerEnd = -1;
                        break;
                    }
                }
            }
            // Trim trailing zeros from fractional portion
            if (state.FractionalStart != -1 && state.FractionalEnd != -1)
            {
                for (int i = state.FractionalEnd; i >= state.FractionalStart; i--)
                {
                    if (value[i] != AsciiZero) break;

                    state.FractionalEnd = i - 1;
                    if (state.FractionalStart > state.FractionalEnd)
                    {
                        state.FractionalStart = -1;
                        state.FractionalEnd = -1;
                        break;
                    }
                }
            }
            // Must have a integer or fractional part.
            if (state.IntegerStart == -1 && state.FractionalStart == -1)
            {
                if (!state.HasZero)
                    throw new ArgumentOutOfRangeException(nameof(value), Encoding.UTF8.GetString(value), "input does not contain a valid numeric value.");
                else
                {
                    state.IntegerStart = value.IndexOf(AsciiZero);
                    state.IntegerEnd = state.IntegerStart;
                }
            }

            parserState = state;
        }
    }

#if !NETCOREAPP
    internal static class EncodingExtensions
    {
        public static string GetString(this Encoding encoding, ReadOnlySpan<byte> source)
        {
            return encoding.GetString(source.ToArray());
        }
    }
#endif
}

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
using System.Globalization;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// A minimal TOML parser that handles the subset of TOML used by ADBC driver
    /// manifests and connection profiles:
    /// <list type="bullet">
    ///   <item><description>Root-level key = value assignments (bare keys only)</description></item>
    ///   <item><description>Table section headers: <c>[section]</c> and dotted <c>[a.b]</c></description></item>
    ///   <item><description>Basic strings (<c>"..."</c>) with <c>\"</c>, <c>\\</c>, <c>\n</c>, <c>\r</c>, <c>\t</c> escapes</description></item>
    ///   <item><description>Literal strings (<c>'...'</c>) with no escape processing</description></item>
    ///   <item><description>Single-line arrays of supported scalars</description></item>
    ///   <item><description>Integer, floating-point, and lowercase boolean (<c>true</c>/<c>false</c>) values</description></item>
    ///   <item><description>Line comments beginning with <c>#</c></description></item>
    /// </list>
    /// Unsupported but recognized TOML productions -- multi-line strings, inline tables,
    /// dates, multi-line/nested arrays, underscored/hex/oct/bin integers, dotted keys, etc.
    /// -- are rejected with a <see cref="FormatException"/> rather than silently misread.
    ///
    /// A full-featured TOML library (e.g. Tomlyn) was considered but cannot be used here
    /// because the assembly is strongly-named and Tomlyn does not publish a strongly-named
    /// package that is compatible with the project's pinned dependency versions.
    /// </summary>
    internal static class TomlParser
    {
        private const string RootSection = "";

        /// <summary>
        /// Parses <paramref name="content"/> and returns a dictionary keyed by section name.
        /// Root-level keys are stored under the empty string key.
        /// Values are typed as <see cref="string"/>, <see cref="long"/>, <see cref="double"/>,
        /// <see cref="bool"/>, or <see cref="List{T}"/> of those.
        /// </summary>
        internal static Dictionary<string, Dictionary<string, object>> Parse(string content)
        {
            if (content == null)
            {
                throw new ArgumentNullException(nameof(content));
            }

            Dictionary<string, Dictionary<string, object>> result = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase)
            {
                [RootSection] = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase),
            };

            string currentSection = RootSection;

            foreach (string rawLine in content.Split('\n'))
            {
                string line = StripComment(rawLine).Trim();

                if (line.Length == 0)
                {
                    continue;
                }

                if (line[0] == '[')
                {
                    if (line[line.Length - 1] != ']')
                    {
                        throw new FormatException(
                            "Invalid TOML section header '" + line + "': missing closing ']' or invalid trailing content.");
                    }
                    string sectionName = line.Substring(1, line.Length - 2).Trim();
                    ValidateSectionName(sectionName);
                    currentSection = sectionName;
                    if (!result.ContainsKey(currentSection))
                    {
                        result[currentSection] = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                    }
                    continue;
                }

                int eqIndex = line.IndexOf('=');
                if (eqIndex <= 0)
                {
                    throw new FormatException(
                        "Invalid TOML line '" + line + "': expected 'key = value', section header, or comment.");
                }

                string key = line.Substring(0, eqIndex).Trim();
                ValidateKeyName(key);
                string valueRaw = line.Substring(eqIndex + 1).Trim();

                object value = ParseValue(valueRaw);
                result[currentSection][key] = value;
            }

            return result;
        }

        private static void ValidateSectionName(string sectionName)
        {
            // Per the ADBC driver manifest spec (https://arrow.apache.org/adbc/current/format/driver_manifests.html)
            // manifests are TOML, and the only section headers used by the spec are bare or
            // dotted keys composed of TOML bare-key characters (A-Z a-z 0-9 _ -), e.g.
            // [ADBC], [ADBC.features], [Driver], [Driver.shared].
            //
            // This parser intentionally implements only that subset: quoted section names,
            // whitespace inside segments, empty segments, and leading/trailing dots are
            // rejected so that an unsupported construct produces a clear error instead of
            // being silently misinterpreted.
            if (sectionName.Length == 0)
            {
                throw new FormatException("Invalid TOML section header: section name is empty.");
            }

            string[] segments = sectionName.Split('.');
            foreach (string segment in segments)
            {
                if (segment.Length == 0)
                {
                    throw new FormatException(
                        "Invalid TOML section header '" + sectionName + "': empty segment in dotted name.");
                }

                for (int i = 0; i < segment.Length; i++)
                {
                    char c = segment[i];
                    bool isAllowed =
                        (c >= 'A' && c <= 'Z') ||
                        (c >= 'a' && c <= 'z') ||
                        (c >= '0' && c <= '9') ||
                        c == '_' || c == '-';
                    if (!isAllowed)
                    {
                        throw new FormatException(
                            "Invalid TOML section header '" + sectionName +
                            "': only bare keys (A-Z, a-z, 0-9, '_' , '-') and dotted keys are supported.");
                    }
                }
            }
        }

        private static void ValidateKeyName(string keyName)
        {
            // ADBC connection profiles and driver manifests use dot-namespaced option
            // names (e.g. 'adbc.snowflake.sql.warehouse') as flat keys, not as TOML
            // dotted keys that nest tables. The Rust and C++ driver managers parse
            // those as nested tables and then flatten the result back to dotted
            // strings before handing them to AdbcDatabaseSetOption; this minimal
            // parser short-circuits that by accepting '.' as part of a bare key, so
            // the dictionary entries match what the other implementations produce.
            // Quoted and whitespace-containing keys are still rejected.
            if (keyName.Length == 0)
            {
                throw new FormatException("Invalid TOML key: key is empty.");
            }

            for (int i = 0; i < keyName.Length; i++)
            {
                char c = keyName[i];
                bool isAllowed =
                    (c >= 'A' && c <= 'Z') ||
                    (c >= 'a' && c <= 'z') ||
                    (c >= '0' && c <= '9') ||
                    c == '_' || c == '-' || c == '.';
                if (!isAllowed)
                {
                    throw new FormatException(
                        "Invalid TOML key '" + keyName +
                        "': only bare keys (A-Z, a-z, 0-9, '_', '-', '.') are supported.");
                }
            }
        }

        private static object ParseValue(string raw)
        {
            if (raw.Length == 0)
            {
                throw new FormatException("Invalid TOML value: value is empty.");
            }

            if (raw[0] == '"')
            {
                return ParseBasicString(raw);
            }

            if (raw[0] == '\'')
            {
                return ParseLiteralString(raw);
            }

            if (raw[0] == '[')
            {
                return ParseArray(raw);
            }

            if (raw[0] == '{')
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw + "': inline tables are not supported.");
            }

            // Boolean (TOML booleans are lowercase; be strict.)
            if (raw == "true")
            {
                return true;
            }
            if (raw == "false")
            {
                return false;
            }

            // Integer (try before float, since integers are a subset). Reject TOML
            // integer extensions (underscores, hex/oct/bin prefixes) by restricting
            // the allowed NumberStyles and explicitly checking the input.
            if (IsPlainInteger(raw) &&
                long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long intValue))
            {
                return intValue;
            }

            // Float
            if (IsPlainFloat(raw) &&
                double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out double dblValue))
            {
                return dblValue;
            }

            // Per the ADBC driver manifest spec, values are a bounded set of TOML scalars
            // plus single-line arrays of those. Anything else -- multi-line strings,
            // inline tables, dates, hex/oct/bin/underscored integers, bare unquoted
            // strings, etc. -- is rejected with a clear error rather than silently
            // treated as a string. This matches the strict-by-default policy used for
            // section and key names.
            throw new FormatException(
                "Invalid TOML value '" + raw +
                "': only basic strings (\"...\"), literal strings ('...'), arrays of those, integers, floats, and 'true'/'false' are supported.");
        }

        private static string ParseBasicString(string raw)
        {
            // Reject multi-line basic strings ("""...""").
            if (raw.Length >= 3 && raw[1] == '"' && raw[2] == '"')
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw + "': multi-line strings are not supported.");
            }

            int close = -1;
            for (int i = 1; i < raw.Length; i++)
            {
                char c = raw[i];
                if (c == '\\')
                {
                    if (i + 1 >= raw.Length)
                    {
                        throw new FormatException(
                            "Invalid TOML value '" + raw + "': dangling escape in basic string.");
                    }
                    i++;
                    continue;
                }
                if (c == '"')
                {
                    close = i;
                    break;
                }
            }
            if (close < 0)
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw + "': unterminated basic string.");
            }
            if (close != raw.Length - 1)
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw + "': trailing content after basic string.");
            }
            return UnescapeString(raw.Substring(1, close - 1));
        }

        private static string ParseLiteralString(string raw)
        {
            // Reject multi-line literal strings ('''...''').
            if (raw.Length >= 3 && raw[1] == '\'' && raw[2] == '\'')
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw + "': multi-line literal strings are not supported.");
            }

            int close = raw.IndexOf('\'', 1);
            if (close < 0)
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw + "': unterminated literal string.");
            }
            if (close != raw.Length - 1)
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw + "': trailing content after literal string.");
            }
            return raw.Substring(1, close - 1);
        }

        private static List<object> ParseArray(string raw)
        {
            if (raw[raw.Length - 1] != ']')
            {
                throw new FormatException(
                    "Invalid TOML value '" + raw +
                    "': arrays must open and close on the same line (multi-line arrays are not supported).");
            }

            string inner = raw.Substring(1, raw.Length - 2).Trim();
            List<object> result = new List<object>();
            if (inner.Length == 0)
            {
                return result;
            }

            foreach (string element in SplitArrayElements(inner, raw))
            {
                string trimmed = element.Trim();
                if (trimmed.Length == 0)
                {
                    throw new FormatException(
                        "Invalid TOML array '" + raw + "': empty array element.");
                }
                if (trimmed[0] == '[' || trimmed[0] == '{')
                {
                    throw new FormatException(
                        "Invalid TOML array '" + raw +
                        "': nested arrays and inline tables are not supported.");
                }
                result.Add(ParseValue(trimmed));
            }
            return result;
        }

        private static List<string> SplitArrayElements(string s, string raw)
        {
            List<string> tokens = new List<string>();
            bool inBasic = false;
            bool inLiteral = false;
            int start = 0;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (inBasic)
                {
                    if (c == '\\' && i + 1 < s.Length)
                    {
                        i++;
                        continue;
                    }
                    if (c == '"')
                    {
                        inBasic = false;
                    }
                }
                else if (inLiteral)
                {
                    if (c == '\'')
                    {
                        inLiteral = false;
                    }
                }
                else
                {
                    if (c == '"')
                    {
                        inBasic = true;
                    }
                    else if (c == '\'')
                    {
                        inLiteral = true;
                    }
                    else if (c == ',')
                    {
                        tokens.Add(s.Substring(start, i - start));
                        start = i + 1;
                    }
                }
            }
            if (inBasic || inLiteral)
            {
                throw new FormatException(
                    "Invalid TOML array '" + raw + "': unterminated string in array element.");
            }
            string tail = s.Substring(start);
            // Allow (but don't require) a trailing comma per TOML spec: if tokens were
            // produced and the tail is whitespace-only, treat it as a trailing comma.
            if (tokens.Count == 0 || tail.Trim().Length > 0)
            {
                tokens.Add(tail);
            }
            return tokens;
        }

        private static bool IsPlainInteger(string raw)
        {
            // TOML allows a single optional leading sign followed by ASCII digits, with
            // no underscores and no 0x/0o/0b prefixes. Anything else is a recognized
            // production this parser doesn't support.
            int i = 0;
            if (i < raw.Length && (raw[i] == '+' || raw[i] == '-'))
            {
                i++;
            }
            if (i >= raw.Length)
            {
                return false;
            }
            for (; i < raw.Length; i++)
            {
                if (raw[i] < '0' || raw[i] > '9')
                {
                    return false;
                }
            }
            return true;
        }

        private static bool IsPlainFloat(string raw)
        {
            // Reject TOML float extensions (underscores, inf, nan) and shapes that
            // double.TryParse would otherwise accept (hex, thousand separators). A plain
            // float here is sign? digits ('.' digits)? ([eE] sign? digits)? with at least
            // one '.' or exponent so that IsPlainInteger covers the pure-integer case.
            int i = 0;
            if (i < raw.Length && (raw[i] == '+' || raw[i] == '-'))
            {
                i++;
            }
            bool seenDigit = false;
            bool seenDot = false;
            bool seenExp = false;
            for (; i < raw.Length; i++)
            {
                char c = raw[i];
                if (c >= '0' && c <= '9')
                {
                    seenDigit = true;
                }
                else if (c == '.' && !seenDot && !seenExp)
                {
                    seenDot = true;
                }
                else if ((c == 'e' || c == 'E') && !seenExp && seenDigit)
                {
                    seenExp = true;
                    seenDigit = false;
                    if (i + 1 < raw.Length && (raw[i + 1] == '+' || raw[i + 1] == '-'))
                    {
                        i++;
                    }
                }
                else
                {
                    return false;
                }
            }
            return seenDigit && (seenDot || seenExp);
        }

        private static string UnescapeString(string s)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(s.Length);
            for (int i = 0; i < s.Length; i++)
            {
                if (s[i] != '\\')
                {
                    sb.Append(s[i]);
                    continue;
                }
                if (i + 1 >= s.Length)
                {
                    throw new FormatException("Invalid TOML basic string: dangling escape '\\'.");
                }
                i++;
                switch (s[i])
                {
                    case '"': sb.Append('"'); break;
                    case '\\': sb.Append('\\'); break;
                    case 'n': sb.Append('\n'); break;
                    case 'r': sb.Append('\r'); break;
                    case 't': sb.Append('\t'); break;
                    default:
                        throw new FormatException(
                            "Invalid TOML basic string escape '\\" + s[i] +
                            "': only \\\", \\\\, \\n, \\r, and \\t are supported.");
                }
            }
            return sb.ToString();
        }

        private static string StripComment(string line)
        {
            // Strip a trailing '#' comment, but not when the '#' falls inside a
            // double-quoted basic string or a single-quoted literal string. Literal
            // strings perform no escape processing, so backslashes inside them are
            // taken literally and do not affect string termination.
            bool inBasic = false;
            bool inLiteral = false;
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (inBasic)
                {
                    if (c == '\\' && i + 1 < line.Length)
                    {
                        i++;
                        continue;
                    }
                    if (c == '"')
                    {
                        inBasic = false;
                    }
                }
                else if (inLiteral)
                {
                    if (c == '\'')
                    {
                        inLiteral = false;
                    }
                }
                else
                {
                    if (c == '"')
                    {
                        inBasic = true;
                    }
                    else if (c == '\'')
                    {
                        inLiteral = true;
                    }
                    else if (c == '#')
                    {
                        return line.Substring(0, i);
                    }
                }
            }
            return line;
        }
    }
}

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
    ///   <item><description>Root-level key = value assignments</description></item>
    ///   <item><description>Table section headers: <c>[section]</c></description></item>
    ///   <item><description>String values (double-quoted), integer values, floating-point
    ///     values, and boolean values (<c>true</c>/<c>false</c>)</description></item>
    ///   <item><description>Line comments beginning with <c>#</c></description></item>
    /// </list>
    /// This parser intentionally does not support the full TOML specification.
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
        /// or <see cref="bool"/>.
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

                if (line.StartsWith("[", StringComparison.Ordinal) && line.EndsWith("]", StringComparison.Ordinal))
                {
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
                    continue;
                }

                string key = line.Substring(0, eqIndex).Trim();
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

        private static object ParseValue(string raw)
        {
            if (raw.Length == 0)
            {
                throw new FormatException("Invalid TOML value: value is empty.");
            }

            // Double-quoted string. Multi-line triple-quoted strings ("""...""") are
            // explicitly rejected so that they don't get misread as a basic string with
            // empty quotes around the content.
            if (raw[0] == '"')
            {
                if (raw.Length >= 3 && raw[1] == '"' && raw[2] == '"')
                {
                    throw new FormatException(
                        "Invalid TOML value '" + raw + "': multi-line strings are not supported.");
                }
                if (raw.Length < 2 || raw[raw.Length - 1] != '"')
                {
                    throw new FormatException("Invalid TOML value '" + raw + "': unterminated double-quoted string.");
                }
                string inner = raw.Substring(1, raw.Length - 2);
                return UnescapeString(inner);
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

            // Integer (try before float, since integers are a subset)
            if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long intValue))
            {
                return intValue;
            }

            // Float
            if (double.TryParse(raw, NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out double dblValue))
            {
                return dblValue;
            }

            // Per the ADBC driver manifest spec, values are TOML scalars. This parser
            // intentionally implements only the subset of TOML productions documented on
            // the type (double-quoted strings, integers, floats, and lowercase booleans).
            // Anything else -- TOML literal strings ('foo'), multi-line strings ("""..."""),
            // arrays, inline tables, dates, hex/oct/bin/underscored integers, etc. -- is
            // rejected with a clear error rather than being silently treated as an unquoted
            // string. This matches the strict-by-default policy used for section names.
            throw new FormatException(
                "Invalid TOML value '" + raw +
                "': only double-quoted strings, integers, floats, and 'true'/'false' are supported.");
        }

        private static string UnescapeString(string s)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(s.Length);
            for (int i = 0; i < s.Length; i++)
            {
                if (s[i] == '\\' && i + 1 < s.Length)
                {
                    i++;
                    switch (s[i])
                    {
                        case '"': sb.Append('"'); break;
                        case '\\': sb.Append('\\'); break;
                        case 'n': sb.Append('\n'); break;
                        case 'r': sb.Append('\r'); break;
                        case 't': sb.Append('\t'); break;
                        default: sb.Append('\\'); sb.Append(s[i]); break;
                    }
                }
                else
                {
                    sb.Append(s[i]);
                }
            }
            return sb.ToString();
        }

        private static string StripComment(string line)
        {
            // Only strip # that is not inside a quoted string
            bool inString = false;
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (c == '"' && (i == 0 || line[i - 1] != '\\'))
                {
                    inString = !inString;
                }
                if (c == '#' && !inString)
                {
                    return line.Substring(0, i);
                }
            }
            return line;
        }
    }
}

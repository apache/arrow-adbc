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
using System.Text;
using System.Text.RegularExpressions;

namespace Apache.Arrow.Adbc.DriverManager
{
    /// <summary>
    /// Represents an ADBC connection profile: a driver reference and a set of typed
    /// options to apply when opening a database. Mirrors the
    /// <c>AdbcConnectionProfile</c> struct defined in <c>adbc_driver_manager.h</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A profile is a plain data-transfer object. Use an
    /// <see cref="IConnectionProfileProvider"/> (for example
    /// <see cref="FilesystemProfileProvider"/>) to load one from a backing store.
    /// </para>
    /// <para>
    /// Options come in three typed flavors: string, 64-bit integer, and double.
    /// String option values may contain <c>{{ env_var(NAME) }}</c> placeholders that
    /// <see cref="ResolveEnvVars"/> expands using process environment variables.
    /// </para>
    /// </remarks>
    public sealed class ConnectionProfile
    {
        // Per docs/source/format/connection_profiles.rst, dynamic substitutions
        // are written as `{{ <function-call> }}` and may appear anywhere inside
        // a string value. The character set inside the placeholder excludes
        // braces so adjacent placeholders don't accidentally merge.
        private static readonly Regex PlaceholderRegex = new Regex(
            @"\{\{\s*([^{}]*?)\s*\}\}",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        private const string EnvVarPrefix = "env_var(";

        private readonly Dictionary<string, string> _stringOptions;
        private readonly Dictionary<string, long> _intOptions;
        private readonly Dictionary<string, double> _doubleOptions;

        /// <summary>
        /// Initializes a new <see cref="ConnectionProfile"/>.
        /// </summary>
        /// <param name="driverName">
        /// The driver reference: a bare driver name (resolved against the manifest
        /// search path), an absolute or relative path to a shared library, or an
        /// absolute or relative path to a driver manifest <c>.toml</c> file. For
        /// managed (.NET) drivers, the manifest at this location selects the
        /// runtime via <c>[Driver].entrypoint</c>; alternatively, a profile that
        /// points directly at a managed assembly can supply the type name through
        /// an <c>entrypoint</c> option.
        /// </param>
        /// <param name="stringOptions">String options, or <c>null</c> for none.</param>
        /// <param name="intOptions">Integer options, or <c>null</c> for none.</param>
        /// <param name="doubleOptions">Double options, or <c>null</c> for none.</param>
        public ConnectionProfile(
            string? driverName = null,
            IReadOnlyDictionary<string, string>? stringOptions = null,
            IReadOnlyDictionary<string, long>? intOptions = null,
            IReadOnlyDictionary<string, double>? doubleOptions = null)
        {
            DriverName = driverName;
            _stringOptions = new Dictionary<string, string>(StringComparer.Ordinal);
            if (stringOptions != null)
            {
                foreach (KeyValuePair<string, string> kv in stringOptions) _stringOptions[kv.Key] = kv.Value;
            }
            _intOptions = new Dictionary<string, long>(StringComparer.Ordinal);
            if (intOptions != null)
            {
                foreach (KeyValuePair<string, long> kv in intOptions) _intOptions[kv.Key] = kv.Value;
            }
            _doubleOptions = new Dictionary<string, double>(StringComparer.Ordinal);
            if (doubleOptions != null)
            {
                foreach (KeyValuePair<string, double> kv in doubleOptions) _doubleOptions[kv.Key] = kv.Value;
            }
        }

        /// <summary>
        /// Gets the driver reference specified by this profile, or <c>null</c> if
        /// the profile does not specify one. May be a bare driver name, a shared
        /// library path, or a driver manifest path.
        /// </summary>
        public string? DriverName { get; }

        /// <summary>
        /// Gets the string options specified by this profile. Values may contain
        /// <c>{{ env_var(NAME) }}</c> placeholders that <see cref="ResolveEnvVars"/>
        /// expands using process environment variables.
        /// </summary>
        public IReadOnlyDictionary<string, string> StringOptions => _stringOptions;

        /// <summary>
        /// Gets the 64-bit integer options specified by this profile.
        /// </summary>
        public IReadOnlyDictionary<string, long> IntOptions => _intOptions;

        /// <summary>
        /// Gets the double-precision floating-point options specified by this profile.
        /// </summary>
        public IReadOnlyDictionary<string, double> DoubleOptions => _doubleOptions;

        /// <summary>
        /// Returns a new profile with any <c>{{ env_var(NAME) }}</c> placeholders
        /// in <see cref="StringOptions"/> expanded using process environment
        /// variables.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Placeholder syntax matches the ADBC spec (see
        /// <c>docs/source/format/connection_profiles.rst</c>):
        /// </para>
        /// <list type="bullet">
        ///   <item>
        ///     <description>
        ///       Placeholders use <c>{{ }}</c> as the escape delimiters and may
        ///       appear anywhere inside a value. Whitespace inside the braces is
        ///       optional. Multiple placeholders may appear in one value
        ///       (e.g. <c>"jdbc://{{ env_var(HOST) }}:{{ env_var(PORT) }}/db"</c>).
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       A missing environment variable expands to an empty string and
        ///       processing continues; this matches the C/C++ driver manager.
        ///     </description>
        ///   </item>
        ///   <item>
        ///     <description>
        ///       The only supported function inside a placeholder is
        ///       <c>env_var(NAME)</c>. Any other content -- including a literal
        ///       <c>{{</c> in a value -- is rejected with
        ///       <see cref="AdbcStatusCode.InvalidArgument"/>.
        ///     </description>
        ///   </item>
        /// </list>
        /// </remarks>
        /// <exception cref="AdbcException">
        /// Thrown when a placeholder uses an unrecognized function or is malformed
        /// (e.g. missing the closing parenthesis or environment variable name).
        /// </exception>
        public ConnectionProfile ResolveEnvVars()
        {
            Dictionary<string, string> resolved = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, string> kv in _stringOptions)
            {
                resolved[kv.Key] = ExpandPlaceholders(kv.Key, kv.Value);
            }
            return new ConnectionProfile(DriverName, resolved, _intOptions, _doubleOptions);
        }

        /// <summary>
        /// Substitutes every <c>{{ ... }}</c> placeholder in <paramref name="value"/>
        /// with its expansion. The only recognized function is <c>env_var(NAME)</c>;
        /// anything else is an error.
        /// </summary>
        private static string ExpandPlaceholders(string key, string value)
        {
            if (string.IsNullOrEmpty(value) || value.IndexOf("{{", StringComparison.Ordinal) < 0)
            {
                return value;
            }

            StringBuilder sb = new StringBuilder(value.Length);
            int lastIndex = 0;
            foreach (Match match in PlaceholderRegex.Matches(value))
            {
                sb.Append(value, lastIndex, match.Index - lastIndex);
                sb.Append(ExpandFunction(key, match.Groups[1].Value));
                lastIndex = match.Index + match.Length;
            }
            sb.Append(value, lastIndex, value.Length - lastIndex);
            return sb.ToString();
        }

        private static string ExpandFunction(string key, string content)
        {
            if (!content.StartsWith(EnvVarPrefix, StringComparison.Ordinal))
            {
                throw new AdbcException(
                    $"Profile option '{key}' uses an unsupported substitution '{content}'. " +
                    "Only env_var(NAME) is recognized.",
                    AdbcStatusCode.InvalidArgument);
            }
            if (content.Length == 0 || content[content.Length - 1] != ')')
            {
                throw new AdbcException(
                    $"Profile option '{key}' has a malformed env_var() placeholder: missing closing parenthesis.",
                    AdbcStatusCode.InvalidArgument);
            }

            string varName = content.Substring(EnvVarPrefix.Length, content.Length - EnvVarPrefix.Length - 1);
            if (varName.Length == 0)
            {
                throw new AdbcException(
                    $"Profile option '{key}' has a malformed env_var() placeholder: missing environment variable name.",
                    AdbcStatusCode.InvalidArgument);
            }

            // Missing environment variables expand to empty per the spec, matching
            // the C/C++ driver manager. Callers that want to require an env var
            // should validate after ResolveEnvVars returns.
            return Environment.GetEnvironmentVariable(varName) ?? string.Empty;
        }
    }
}

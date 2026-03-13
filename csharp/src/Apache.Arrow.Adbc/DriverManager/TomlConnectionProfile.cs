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
    /// An <see cref="IConnectionProfile"/> loaded from a TOML file.
    /// </summary>
    /// <remarks>
    /// The expected file format is:
    /// <code>
    /// version = 1
    /// driver = "driver_name"
    ///
    /// [options]
    /// option1 = "value1"
    /// option2 = 42
    /// option3 = 3.14
    /// </code>
    /// Boolean option values are converted to the string equivalents <c>"true"</c> or
    /// <c>"false"</c> and placed in <see cref="StringOptions"/>.
    /// Integer values are placed in <see cref="IntOptions"/> and double values in
    /// <see cref="DoubleOptions"/> (integer values are also reflected in
    /// <see cref="IntOptions"/>).
    /// Values of the form <c>env_var(ENV_VAR_NAME)</c> are expanded from the named
    /// environment variable when <see cref="ResolveEnvVars"/> is called.
    /// </remarks>
    public sealed class TomlConnectionProfile : IConnectionProfile
    {
        private const string OptionsSection = "options";
        private const string EnvVarPrefix = "env_var(";

        private readonly Dictionary<string, string> _stringOptions;
        private readonly Dictionary<string, long> _intOptions;
        private readonly Dictionary<string, double> _doubleOptions;

        private TomlConnectionProfile(
            string? driverName,
            string? driverTypeName,
            Dictionary<string, string> stringOptions,
            Dictionary<string, long> intOptions,
            Dictionary<string, double> doubleOptions)
        {
            DriverName = driverName;
            DriverTypeName = driverTypeName;
            _stringOptions = stringOptions;
            _intOptions = intOptions;
            _doubleOptions = doubleOptions;
        }

        /// <inheritdoc/>
        public string? DriverName { get; }

        /// <inheritdoc/>
        public string? DriverTypeName { get; }

        /// <inheritdoc/>
        public IReadOnlyDictionary<string, string> StringOptions => _stringOptions;

        /// <inheritdoc/>
        public IReadOnlyDictionary<string, long> IntOptions => _intOptions;

        /// <inheritdoc/>
        public IReadOnlyDictionary<string, double> DoubleOptions => _doubleOptions;

        /// <summary>
        /// Loads a <see cref="TomlConnectionProfile"/> from the given TOML content string.
        /// </summary>
        /// <param name="tomlContent">The raw TOML text to parse.</param>
        /// <returns>A new <see cref="TomlConnectionProfile"/>.</returns>
        /// <exception cref="AdbcException">
        /// Thrown when the TOML content is missing a required <c>version</c> field or
        /// has an unsupported version number.
        /// </exception>
        public static TomlConnectionProfile FromContent(string tomlContent)
        {
            if (tomlContent == null)
            {
                throw new ArgumentNullException(nameof(tomlContent));
            }

            Dictionary<string, Dictionary<string, object>> sections = TomlParser.Parse(tomlContent);

            Dictionary<string, object> root = sections.TryGetValue("", out Dictionary<string, object>? r) ? r : new Dictionary<string, object>();

            ValidateVersion(root);

            string? driverName = null;
            if (root.TryGetValue("driver", out object? driverObj) && driverObj is string driverStr)
            {
                driverName = driverStr;
            }

            string? driverTypeName = null;
            if (root.TryGetValue("driver_type", out object? driverTypeObj) && driverTypeObj is string driverTypeStr)
            {
                driverTypeName = driverTypeStr;
            }

            Dictionary<string, string> stringOpts = new Dictionary<string, string>(StringComparer.Ordinal);
            Dictionary<string, long> intOpts = new Dictionary<string, long>(StringComparer.Ordinal);
            Dictionary<string, double> doubleOpts = new Dictionary<string, double>(StringComparer.Ordinal);

            if (sections.TryGetValue(OptionsSection, out Dictionary<string, object>? optSection))
            {
                foreach (KeyValuePair<string, object> kv in optSection)
                {
                    string key = kv.Key;
                    object val = kv.Value;

                    switch (val)
                    {
                        case long lv:
                            intOpts[key] = lv;
                            break;
                        case double dv:
                            doubleOpts[key] = dv;
                            break;
                        case bool bv:
                            stringOpts[key] = bv ? "true" : "false";
                            break;
                        default:
                            stringOpts[key] = Convert.ToString(val, CultureInfo.InvariantCulture) ?? string.Empty;
                            break;
                    }
                }
            }

            return new TomlConnectionProfile(driverName, driverTypeName, stringOpts, intOpts, doubleOpts);
        }

        /// <summary>
        /// Loads a <see cref="TomlConnectionProfile"/> from the given TOML file path.
        /// </summary>
        /// <param name="filePath">Absolute or relative path to the <c>.toml</c> file.</param>
        /// <returns>A new <see cref="TomlConnectionProfile"/>.</returns>
        public static TomlConnectionProfile FromFile(string filePath)
        {
            if (filePath == null)
            {
                throw new ArgumentNullException(nameof(filePath));
            }
            string content = System.IO.File.ReadAllText(filePath, System.Text.Encoding.UTF8);
            return FromContent(content);
        }

        /// <summary>
        /// Returns a new profile with any <c>env_var(NAME)</c> values in
        /// <see cref="StringOptions"/> replaced by the value of the corresponding
        /// environment variable.
        /// </summary>
        /// <exception cref="AdbcException">
        /// Thrown when a referenced environment variable is not set.
        /// </exception>
        public TomlConnectionProfile ResolveEnvVars()
        {
            Dictionary<string, string> resolved = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, string> kv in _stringOptions)
            {
                string value = kv.Value;
                if (value.StartsWith(EnvVarPrefix, StringComparison.Ordinal) &&
                    value.EndsWith(")", StringComparison.Ordinal))
                {
                    string varName = value.Substring(EnvVarPrefix.Length, value.Length - EnvVarPrefix.Length - 1);
                    string? envValue = Environment.GetEnvironmentVariable(varName);
                    if (envValue == null)
                    {
                        throw new AdbcException(
                            $"Environment variable '{varName}' required by profile option '{kv.Key}' is not set.",
                            AdbcStatusCode.InvalidState);
                    }
                    resolved[kv.Key] = envValue;
                }
                else
                {
                    resolved[kv.Key] = value;
                }
            }
            return new TomlConnectionProfile(DriverName, DriverTypeName, resolved, _intOptions, _doubleOptions);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
        }

        private static void ValidateVersion(Dictionary<string, object> root)
        {
            if (!root.TryGetValue("version", out object? versionObj))
            {
                throw new AdbcException(
                    "TOML profile is missing the required 'version' field.",
                    AdbcStatusCode.InvalidArgument);
            }

            long version;
            if (versionObj is long lv)
            {
                version = lv;
            }
            else
            {
                try
                {
                    version = Convert.ToInt64(versionObj, CultureInfo.InvariantCulture);
                }
                catch (Exception ex) when (ex is FormatException || ex is InvalidCastException || ex is OverflowException)
                {
                    throw new AdbcException(
                        $"The 'version' field has an invalid value '{versionObj}'. It must be an integer.",
                        AdbcStatusCode.InvalidArgument);
                }
            }

            if (version != 1)
            {
                throw new AdbcException(
                    $"Unsupported profile version '{version}'. Only version 1 is supported.",
                    AdbcStatusCode.NotImplemented);
            }
        }
    }
}

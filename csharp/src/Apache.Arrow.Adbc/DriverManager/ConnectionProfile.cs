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
    /// String option values of the form <c>env_var(ENV_VAR_NAME)</c> are expanded
    /// from the named environment variable by <see cref="ResolveEnvVars"/>.
    /// </para>
    /// </remarks>
    public sealed class ConnectionProfile
    {
        private const string EnvVarPrefix = "env_var(";

        private readonly Dictionary<string, string> _stringOptions;
        private readonly Dictionary<string, long> _intOptions;
        private readonly Dictionary<string, double> _doubleOptions;

        /// <summary>
        /// Initializes a new <see cref="ConnectionProfile"/>.
        /// </summary>
        /// <param name="driverName">
        /// The driver name. For native drivers this is the path to a shared library or
        /// a bare driver name; for managed drivers this is the path to the .NET assembly.
        /// </param>
        /// <param name="driverTypeName">
        /// The fully-qualified .NET type name of the <see cref="AdbcDriver"/> subclass
        /// to instantiate for managed (pure .NET) drivers, or <c>null</c> for native drivers.
        /// </param>
        /// <param name="stringOptions">String options, or <c>null</c> for none.</param>
        /// <param name="intOptions">Integer options, or <c>null</c> for none.</param>
        /// <param name="doubleOptions">Double options, or <c>null</c> for none.</param>
        public ConnectionProfile(
            string? driverName = null,
            string? driverTypeName = null,
            IReadOnlyDictionary<string, string>? stringOptions = null,
            IReadOnlyDictionary<string, long>? intOptions = null,
            IReadOnlyDictionary<string, double>? doubleOptions = null)
        {
            DriverName = driverName;
            DriverTypeName = driverTypeName;
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
        /// Gets the name of the driver specified by this profile, or <c>null</c> if
        /// the profile does not specify a driver.
        /// </summary>
        public string? DriverName { get; }

        /// <summary>
        /// Gets the fully-qualified .NET type name of the <see cref="AdbcDriver"/>
        /// subclass to instantiate for managed (pure .NET) drivers, or <c>null</c>
        /// for native drivers.
        /// </summary>
        /// <example>
        /// <c>Apache.Arrow.Adbc.Drivers.BigQuery.BigQueryDriver</c>
        /// </example>
        public string? DriverTypeName { get; }

        /// <summary>
        /// Gets the string options specified by this profile. Values of the form
        /// <c>env_var(ENV_VAR_NAME)</c> will be expanded from the named environment
        /// variable when <see cref="ResolveEnvVars"/> is called.
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
        /// Returns a new profile with any <c>env_var(NAME)</c> values in
        /// <see cref="StringOptions"/> replaced by the value of the corresponding
        /// environment variable.
        /// </summary>
        /// <exception cref="AdbcException">
        /// Thrown when a referenced environment variable is not set.
        /// </exception>
        public ConnectionProfile ResolveEnvVars()
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
            return new ConnectionProfile(DriverName, DriverTypeName, resolved, _intOptions, _doubleOptions);
        }
    }
}

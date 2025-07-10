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
using System.Linq;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    /// <summary>
    /// Manages configuration properties from different sources with priority ordering for HiveServer2.
    /// </summary>
    internal class HiveServer2ConfigurationManager
    {
        private const string EnvVarPrefix = "ADBC_";
        
        private readonly IReadOnlyDictionary<string, string> _environmentVariables;
        private readonly IReadOnlyDictionary<string, string> _databaseProperties;
        private IReadOnlyDictionary<string, string>? _connectionOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="HiveServer2ConfigurationManager"/> class.
        /// </summary>
        /// <param name="databaseProperties">The database properties provided at database creation time.</param>
        public HiveServer2ConfigurationManager(IReadOnlyDictionary<string, string> databaseProperties)
        {
            _environmentVariables = LoadEnvironmentVariables();
            _databaseProperties = databaseProperties ?? new Dictionary<string, string>();
            _connectionOptions = null;
        }

        /// <summary>
        /// Sets the connection options.
        /// </summary>
        /// <param name="connectionOptions">The connection options provided when connecting.</param>
        public void SetConnectionOptions(IReadOnlyDictionary<string, string>? connectionOptions)
        {
            _connectionOptions = connectionOptions;
        }

        /// <summary>
        /// Gets all properties merged according to priority:
        /// 1. Connection options (highest priority)
        /// 2. Database properties
        /// 3. Environment variables (lowest priority)
        /// </summary>
        /// <returns>A dictionary containing all merged properties.</returns>
        public Dictionary<string, string> GetMergedProperties()
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            
            // Add environment variables (lowest priority)
            foreach (var kvp in _environmentVariables)
            {
                result[kvp.Key] = kvp.Value;
            }
            
            // Add database properties (overrides environment variables)
            foreach (var kvp in _databaseProperties)
            {
                result[kvp.Key] = kvp.Value;
            }
            
            // Add connection options (highest priority)
            if (_connectionOptions != null)
            {
                foreach (var kvp in _connectionOptions)
                {
                    result[kvp.Key] = kvp.Value;
                }
            }
            
            return result;
        }

        /// <summary>
        /// Gets a specific property value, respecting the priority order.
        /// </summary>
        /// <param name="key">The property key.</param>
        /// <param name="defaultValue">The default value to return if the property is not found.</param>
        /// <returns>The property value, or the default value if not found.</returns>
        public string GetProperty(string key, string defaultValue = "")
        {
            // Check connection options first (highest priority)
            if (_connectionOptions != null && _connectionOptions.TryGetValue(key, out var connectionValue))
            {
                return connectionValue;
            }
            
            // Then check database properties
            if (_databaseProperties.TryGetValue(key, out var dbValue))
            {
                return dbValue;
            }
            
            // Then check environment variables
            if (_environmentVariables.TryGetValue(key, out var envValue))
            {
                return envValue;
            }
            
            // Return default value if not found
            return defaultValue;
        }

        /// <summary>
        /// Tries to get a specific property value, respecting the priority order.
        /// </summary>
        /// <param name="key">The property key.</param>
        /// <param name="value">The property value if found; otherwise, null.</param>
        /// <returns>True if the property was found; otherwise, false.</returns>
        public bool TryGetProperty(string key, out string? value)
        {
            // Check connection options first (highest priority)
            if (_connectionOptions != null && _connectionOptions.TryGetValue(key, out value))
            {
                return true;
            }
            
            // Then check database properties
            if (_databaseProperties.TryGetValue(key, out value))
            {
                return true;
            }
            
            // Then check environment variables
            if (_environmentVariables.TryGetValue(key, out value))
            {
                return true;
            }
            
            value = null;
            return false;
        }

        private static IReadOnlyDictionary<string, string> LoadEnvironmentVariables()
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var envVar in Environment.GetEnvironmentVariables().Keys.Cast<string>())
            {
                if (envVar.StartsWith(EnvVarPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    string paramName = EnvironmentVariableToParameter(envVar);
                    string? value = Environment.GetEnvironmentVariable(envVar);
                    
                    if (value != null)
                    {
                        result[paramName] = value;
                    }
                }
            }
            
            return result;
        }

        /// <summary>
        /// Converts an environment variable name to its corresponding ADBC parameter name.
        /// </summary>
        /// <param name="envVarName">The environment variable name (e.g., "ADBC_DATABRICKS_CLOUDFETCH_ENABLED")</param>
        /// <returns>The ADBC parameter name (e.g., "adbc.databricks.cloudfetch.enabled")</returns>
        public static string EnvironmentVariableToParameter(string envVarName)
        {
            if (!envVarName.StartsWith(EnvVarPrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"Environment variable name must start with '{EnvVarPrefix}'", nameof(envVarName));
            }

            return envVarName.Substring(EnvVarPrefix.Length).ToLowerInvariant().Replace('_', '.');
        }

        /// <summary>
        /// Converts an ADBC parameter name to its corresponding environment variable name.
        /// </summary>
        /// <param name="parameterName">The ADBC parameter name (e.g., "adbc.databricks.cloudfetch.enabled")</param>
        /// <returns>The environment variable name (e.g., "ADBC_DATABRICKS_CLOUDFETCH_ENABLED")</returns>
        public static string ParameterToEnvironmentVariable(string parameterName)
        {
            return EnvVarPrefix + parameterName.ToUpperInvariant().Replace('.', '_');
        }
    }
}
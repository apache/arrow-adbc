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
using System.IO;
using System.Text.Json;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Configuration class for Databricks connection properties loaded from JSON files.
    /// </summary>
    public class DatabricksConfiguration
    {
        /// <summary>
        /// Dictionary of connection properties.
        /// </summary>
        public Dictionary<string, string> Properties { get; set; } = new();

        /// <summary>
        /// Creates a DatabricksConnection from this configuration.
        /// </summary>
        /// <returns>A configured DatabricksConnection instance.</returns>
        internal DatabricksConnection CreateConnection()
        {
            return new DatabricksConnection(Properties);
        }

        /// <summary>
        /// Loads configuration from an environment variable that points to a JSON file.
        /// </summary>
        /// <param name="environmentVariable">Name of the environment variable containing the file path.</param>
        /// <returns>DatabricksConfiguration loaded from the file.</returns>
        /// <exception cref="InvalidOperationException">Thrown when environment variable is not set or file doesn't exist.</exception>
        public static DatabricksConfiguration FromEnvironmentVariable(string environmentVariable)
        {
            if (!CanLoadFromEnvironment(environmentVariable, out string? filePath))
            {
                throw new InvalidOperationException($"Cannot load Databricks configuration from environment variable '{environmentVariable}'. Make sure the environment variable is set and points to a valid JSON file.");
            }

            return FromFile(filePath!);
        }

        /// <summary>
        /// Loads configuration from a JSON file.
        /// Supports free-form JSON structure with key-value pairs.
        /// </summary>
        /// <param name="filePath">Path to the JSON configuration file.</param>
        /// <returns>DatabricksConfiguration loaded from the file.</returns>
        /// <exception cref="FileNotFoundException">Thrown when the file doesn't exist.</exception>
        /// <exception cref="JsonException">Thrown when the JSON is invalid.</exception>
        public static DatabricksConfiguration FromFile(string filePath)
        {
            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException($"Configuration file not found: {filePath}");
            }

            try
            {
                string json = File.ReadAllText(filePath);
                var options = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                    ReadCommentHandling = JsonCommentHandling.Skip,
                    AllowTrailingCommas = true
                };

                // Deserialize as flat dictionary (free-form JSON)
                var properties = JsonSerializer.Deserialize<Dictionary<string, string>>(json, options);
                if (properties == null)
                {
                    throw new InvalidOperationException($"Failed to deserialize configuration from {filePath}");
                }

                return new DatabricksConfiguration { Properties = properties };
            }
            catch (JsonException ex)
            {
                throw new JsonException($"Invalid JSON in configuration file {filePath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Checks if a configuration can be loaded from an environment variable.
        /// </summary>
        /// <param name="environmentVariable">Name of the environment variable.</param>
        /// <param name="filePath">Output parameter containing the file path if available.</param>
        /// <returns>True if the configuration can be loaded, false otherwise.</returns>
        public static bool CanLoadFromEnvironment(string environmentVariable, out string? filePath)
        {
            filePath = null;

            if (string.IsNullOrWhiteSpace(environmentVariable))
            {
                return false;
            }

            filePath = Environment.GetEnvironmentVariable(environmentVariable);

            if (string.IsNullOrWhiteSpace(filePath))
            {
                return false;
            }

            return File.Exists(filePath);
        }

        /// <summary>
        /// Tries to load configuration from an environment variable.
        /// Returns null if the environment variable is not set or file doesn't exist.
        /// </summary>
        /// <param name="environmentVariable">Name of the environment variable.</param>
        /// <returns>DatabricksConfiguration if successful, null otherwise.</returns>
        public static DatabricksConfiguration? TryFromEnvironmentVariable(string environmentVariable)
        {
            try
            {
                if (CanLoadFromEnvironment(environmentVariable, out string? filePath))
                {
                    return FromFile(filePath!);
                }
            }
            catch
            {
                // Suppress exceptions in try method
            }

            return null;
        }
    }
}

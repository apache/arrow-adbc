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
using System.Diagnostics;

namespace Apache.Arrow.Adbc.Drivers.Apache.Common
{
    /// <summary>
    /// Utility class for loading configuration from JSON files.
    /// </summary>
    internal static class ConfigLoader
    {
        /// <summary>
        /// Environment variable name for the configuration file path.
        /// </summary>
        public const string ConfigFilePathEnvVar = "ADBC_CONFIG_FILE";
        
        /// <summary>
        /// Loads parameters from a JSON configuration file (if specified by environment variable)
        /// and merges them with explicitly provided parameters.
        /// Explicitly provided parameters take precedence over parameters from the config file.
        /// </summary>
        /// <param name="properties">Dictionary of explicitly provided parameters.</param>
        /// <returns>A new dictionary containing the merged parameters.</returns>
        public static IReadOnlyDictionary<string, string> LoadAndMergeConfig(IReadOnlyDictionary<string, string> properties)
        {
            // Check if the environment variable is set
            string? configFilePath = Environment.GetEnvironmentVariable(ConfigFilePathEnvVar);
            
            if (string.IsNullOrWhiteSpace(configFilePath))
            {
                // No config file specified, return the original properties
                return properties;
            }

            try
            {
                // Load the configuration from the JSON file
                var jsonConfig = LoadJsonConfig(configFilePath);
                
                // Create a new dictionary to store the merged parameters
                Dictionary<string, string> mergedParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                
                // Add parameters from the config file
                foreach (var param in jsonConfig)
                {
                    mergedParams[param.Key] = param.Value;
                }
                
                // Add explicitly provided parameters (these take precedence)
                foreach (var param in properties)
                {
                    mergedParams[param.Key] = param.Value;
                }
                
                return mergedParams;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error loading config file: {ex.Message}");
                throw new AdbcException(
                    AdbcStatusCode.InvalidArgument,
                    $"Failed to load configuration from file '{configFilePath}' specified by environment variable '{ConfigFilePathEnvVar}': {ex.Message}");
            }
        }

        /// <summary>
        /// Loads a JSON configuration file and returns a dictionary of parameters.
        /// </summary>
        /// <param name="configFilePath">Path to the JSON configuration file.</param>
        /// <returns>Dictionary of parameters from the JSON file.</returns>
        private static Dictionary<string, string> LoadJsonConfig(string configFilePath)
        {
            if (!File.Exists(configFilePath))
            {
                throw new FileNotFoundException($"Configuration file not found: {configFilePath}");
            }

            string jsonContent = File.ReadAllText(configFilePath);
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                ReadCommentHandling = JsonCommentHandling.Skip
            };
            
            try
            {
                // Parse the JSON directly into a dictionary
                var jsonProperties = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(jsonContent, options);
                if (jsonProperties == null)
                {
                    throw new JsonException("Failed to parse JSON configuration file.");
                }
                
                // Convert the JSON properties to driver parameters
                var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                
                foreach (var prop in jsonProperties)
                {
                    string paramValue = GetStringValue(prop.Value);
                    
                    if (string.IsNullOrEmpty(paramValue))
                    {
                        continue;
                    }
                    
                    // Use the key as-is from the JSON file
                    result[prop.Key] = paramValue;
                }
                
                return result;
            }
            catch (JsonException ex)
            {
                throw new AdbcException(
                    AdbcStatusCode.InvalidArgument,
                    $"Invalid JSON format in configuration file '{configFilePath}': {ex.Message}");
            }
        }
        
        /// <summary>
        /// Gets a string value from a JsonElement.
        /// </summary>
        private static string GetStringValue(JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.String => element.GetString() ?? string.Empty,
                JsonValueKind.Number => element.GetRawText(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => string.Empty
            };
        }
    }
}
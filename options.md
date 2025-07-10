Goose rules: feel free to edit to this file, preferably appending to the end of these requirements, and editting adjustments

In the Databricks Driver, we currently support passing in parameters explicitly. However, the user may want to configure some of the configurable optins, but the application using the driver may not offer this capability. To allow the user to adjust these settings, we should support consuming some file. For now, this can be a JSON file.

Take a look at the Databricks driver and its parent drivers. This implementation should enable a user to defile a path to a json file, and then upon connection we should attempt to read this json file for parameters.

Can you look at the driver code, then come up with an implementation?

## Implementation Plan

After examining the Databricks driver code, I propose the following implementation:

1. **Define an environment variable name for the config file path**:
   - Use a standard environment variable name like `ADBC_DATABRICKS_CONFIG_FILE` to specify the path to the JSON configuration file.
   - This removes the need to add a new parameter to DatabricksParameters.cs.

2. **Create a ConfigLoader class**:
   - Implement a utility class that checks for the environment variable.
   - If the environment variable is set, load and parse the JSON file at the specified path.
   - Convert the JSON contents to key-value pairs.
   - Merge these parameters with explicitly provided parameters, with explicit parameters taking precedence.
   - Handle error cases gracefully (file not found, invalid JSON, etc.) with appropriate error messages.

3. **Modify the DatabricksConnection constructor**:
   - Update the constructor to use the ConfigLoader to load and merge parameters from the environment-specified JSON file.
   - This ensures the configuration is loaded at the earliest point in the connection lifecycle.
   - Example: `public DatabricksConnection(IReadOnlyDictionary<string, string> properties) : base(ConfigLoader.LoadAndMergeConfig(properties))`

4. **Add documentation**:
   - Update the readme.md to document this new feature and explain how to use the environment variable.
   - Include a section on configuration file support with a sample JSON structure.

5. **Add tests**:
   - Create unit tests to verify the ConfigLoader correctly loads and merges parameters.
   - Test scenarios include valid JSON files, invalid files, and parameter precedence.
   - Create a sample JSON file in the test resources directory for testing.
   - Mock environment variables for testing purposes.

This implementation will allow users to specify a JSON configuration file path through an environment variable, and the driver will automatically load and apply those parameters. Explicitly provided parameters will take precedence over parameters from the configuration file, maintaining backward compatibility while adding flexibility.

The JSON file format will be a simple key-value structure:
```json
{
  "adbc.databricks.cloudfetch.enabled": "true",
  "adbc.databricks.cloudfetch.max_retries": "5",
  "adbc.databricks.enable_direct_results": "true",
  "adbc.databricks.cloudfetch.max_bytes_per_file": "10485760"
}
```

This approach maintains the existing parameter system while adding the ability to load parameters from an external file, providing users with more flexibility in how they configure the driver. It's especially useful in environments where the application using the driver doesn't expose all possible configuration parameters.

## Summary

The proposed implementation adds support for loading Databricks driver configuration from a JSON file specified by an environment variable. This feature will be particularly useful for users who need to configure advanced options but are using applications that don't expose all possible configuration parameters.

Key components of the implementation:
1. An environment variable (`ADBC_DATABRICKS_CONFIG_FILE`) to specify the path to a JSON configuration file
2. A utility class to check for the environment variable, load and parse the JSON file if found
3. Logic to merge file-based parameters with explicitly provided parameters
4. Comprehensive error handling for file access and JSON parsing issues
5. Documentation and tests to ensure the feature works as expected

This implementation follows the existing patterns in the Databricks driver code and maintains backward compatibility while adding new functionality. Using an environment variable instead of a driver parameter makes it easier to configure the driver in environments where you can't modify the application code but can set environment variables.

## Implementation Details

After careful consideration, I recommend implementing this feature at a more general level rather than specific to the Databricks driver. This will allow any driver to benefit from the JSON configuration file support. The implementation should be placed in a common location such as the HiveServer2 folder or even at the ADBC level.

Here's the revised implementation with a more general approach:

```csharp
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

namespace Apache.Arrow.Adbc.Drivers
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
```

This implementation would be used in the base connection classes. For example, in the HiveServer2Connection constructor:

```csharp
public HiveServer2Connection(IReadOnlyDictionary<string, string> properties) : base(
    // Load and merge parameters from config file if specified by environment variable
    ConfigLoader.LoadAndMergeConfig(properties))
{
    ValidateProperties();
}
```

Key changes in this approach:

1. **Moved to a more general namespace**: The implementation is now in `Apache.Arrow.Adbc.Drivers` namespace rather than being specific to Databricks.

2. **More general environment variable name**: Changed from `ADBC_DATABRICKS_CONFIG_FILE` to `ADBC_CONFIG_FILE` to reflect its general purpose.

3. **No parameter prefix assumptions**: The implementation no longer assumes any specific prefix for parameters. It uses the keys from the JSON file as-is, allowing each driver to interpret them according to its own conventions.

4. **No special handling for server-side properties**: Removed the special handling for server-side properties with the `ssp_` prefix, as this was specific to the Databricks driver. If needed, each driver can implement its own handling for special properties.

## Documentation Updates

The following section should be added to the general ADBC documentation to explain this feature:

```markdown
### Configuration File Support

ADBC drivers support loading configuration parameters from a JSON file specified by an environment variable. This is useful when you need to configure multiple options but your application doesn't provide a way to set all of them directly.

To use a configuration file:

1. Create a JSON file with your configuration parameters. For example:

```json
{
  "adbc.connection.catalog": "my_catalog",
  "adbc.connection.db_schema": "my_schema",
  "adbc.spark.auth_type": "oauth",
  "adbc.spark.oauth.access_token": "your_access_token",
  "adbc.spark.temporarily_unavailable_retry": "true",
  "adbc.spark.temporarily_unavailable_retry_timeout": "300"
}
```

2. Set the `ADBC_CONFIG_FILE` environment variable to the path of your JSON file:

```bash
# Linux/macOS
export ADBC_CONFIG_FILE=/path/to/your/config.json

# Windows Command Prompt
set ADBC_CONFIG_FILE=C:\path\to\your\config.json

# Windows PowerShell
$env:ADBC_CONFIG_FILE="C:\path\to\your\config.json"
```

Parameters specified directly when creating the connection take precedence over parameters from the configuration file.
```

Additionally, each driver's documentation should include information about the supported parameters that can be used in the configuration file.

## Testing Approach

For testing this general approach, we'll need to:

1. Create test JSON files with various configurations
2. Set up tests that manipulate the environment variable
3. Verify that parameters are correctly loaded, parsed, and merged

Here's an outline of the test cases:

1. Test loading a valid JSON file with various parameter types
2. Test parameter precedence (explicit parameters override file parameters)
3. Test handling of invalid JSON files
4. Test behavior when the environment variable is not set
5. Test behavior when the environment variable points to a non-existent file

For environment variable testing in C#, we can use the following approach:

```csharp
[Fact]
public void TestConfigLoaderWithEnvironmentVariable()
{
    try
    {
        // Set environment variable for test
        string testFilePath = Path.Combine(
            Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!,
            "Resources",
            "config_test.json");
        Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, testFilePath);

        // Create a dictionary with explicitly provided parameters
        var explicitParams = new Dictionary<string, string>
        {
            { "adbc.connection.catalog", "override_catalog" }, // This should override the value in the config file
            { "custom.parameter", "custom_value" } // This should be preserved
        };

        // Load and merge the parameters
        var mergedParams = ConfigLoader.LoadAndMergeConfig(explicitParams);

        // Verify the merged parameters
        Assert.Equal("override_catalog", mergedParams["adbc.connection.catalog"]);
        Assert.Equal("my_schema", mergedParams["adbc.connection.db_schema"]);
        Assert.Equal("oauth", mergedParams["adbc.spark.auth_type"]);
        Assert.Equal("custom_value", mergedParams["custom.parameter"]); // Should be preserved
    }
    finally
    {
        // Clean up
        Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, null);
    }
}
```

This testing approach ensures that the feature works correctly under various conditions and handles error cases gracefully.

## Conclusion

This implementation plan provides a comprehensive approach to adding JSON configuration file support to ADBC drivers using an environment variable. The solution:

1. Uses an environment variable (`ADBC_CONFIG_FILE`) to specify the configuration file path
2. Implements the feature at a general level to benefit all drivers
3. Takes a simple approach that directly parses the JSON into a dictionary
4. Maintains backward compatibility with existing code
5. Provides a clean, flexible way for users to configure drivers without modifying application code
6. Handles error cases gracefully with informative error messages
7. Includes thorough testing to ensure reliability

By implementing this feature, we will make all ADBC drivers more flexible and easier to use in environments where direct parameter configuration is limited. The environment variable approach is particularly suitable for containerized applications, CI/CD pipelines, and other scenarios where environment variables are commonly used for configuration.

This implementation is general enough to work with any driver while still being simple and maintainable. It avoids making assumptions about parameter naming conventions or special handling for specific drivers, allowing each driver to interpret the parameters according to its own conventions.


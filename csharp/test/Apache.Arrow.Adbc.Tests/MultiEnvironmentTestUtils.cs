using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace Apache.Arrow.Adbc.Tests
{
    public static class MultiEnvironmentTestUtils
    {
        public static T LoadMultiEnvironmentTestConfiguration<T>(string environmentVariable)
        {
            T? testConfiguration = default(T);

            if (!string.IsNullOrWhiteSpace(environmentVariable))
            {
                string? environmentValue = Environment.GetEnvironmentVariable(environmentVariable);

                if (!string.IsNullOrWhiteSpace(environmentValue))
                {
                    if (File.Exists(environmentValue))
                    {
                        // use a JSON file for the various settings
                        string json = File.ReadAllText(environmentValue);

                        testConfiguration = JsonSerializer.Deserialize<T>(json)!;
                    }
                }
            }

            if (testConfiguration == null)
                throw new InvalidOperationException($"Cannot execute test configuration from environment variable `{environmentVariable}`");

            return testConfiguration;
        }

        public static List<TEnvironment> GetTestEnvironments<TEnvironment>(MultiEnvironmentTestConfiguration<TEnvironment> testConfiguration)
            where TEnvironment : TestConfiguration
        {
            if (testConfiguration == null)
                throw new ArgumentNullException(nameof(testConfiguration));

            if (testConfiguration.Environments == null || testConfiguration.Environments.Count == 0)
                throw new InvalidOperationException("There are no environments configured");

            List<TEnvironment> environments = new List<TEnvironment>();

            foreach (string environmentName in GetEnvironmentNames(testConfiguration.TestEnvironmentNames))
            {
                if (testConfiguration.Environments.TryGetValue(environmentName, out TEnvironment? testEnvironment))
                {
                    if (testEnvironment != null)
                    {
                        testEnvironment.Name = environmentName;
                        environments.Add(testEnvironment);
                    }
                }
            }

            if (environments.Count == 0)
                throw new InvalidOperationException("Could not find a configured Flight SQL environment");

            return environments;
        }

        private static List<string> GetEnvironmentNames(List<string> names)
        {
            if (names == null)
                return new List<string>();

            return names;
        }
    }
}

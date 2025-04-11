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
using System.Reflection;
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
                        testConfiguration = JsonSerializer.Deserialize<T>(json);
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
            string term = "$ref:shared.";

            foreach (string environmentName in GetEnvironmentNames(testConfiguration.TestEnvironmentNames))
            {
                if (!testConfiguration.Environments.TryGetValue(environmentName, out TEnvironment? testEnvironment) || testEnvironment is null)
                    continue;

                testEnvironment.Name = environmentName;

                if (testConfiguration.SharedKeyValuePairs.Count > 0)
                {
                    foreach (PropertyInfo pi in testEnvironment.GetType().GetProperties())
                    {
                        if (pi.PropertyType == typeof(string) &&
                            pi.GetValue(testEnvironment) is string propertyValue &&
                            propertyValue.StartsWith(term, StringComparison.Ordinal))
                        {
                            string lookupKey = propertyValue.AsSpan(term.Length).ToString();

                            if (testConfiguration.SharedKeyValuePairs.TryGetValue(lookupKey, out string? sharedValue))
                            {
                                pi.SetValue(testEnvironment, sharedValue);
                            }
                        }
                    }
                }

                environments.Add(testEnvironment);
            }

            if (environments.Count == 0)
                throw new InvalidOperationException("Could not find a configured environment to execute the tests");

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

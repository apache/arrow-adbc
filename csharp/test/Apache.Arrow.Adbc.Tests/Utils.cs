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
using System.IO;
using System.Text.Json;

namespace Apache.Arrow.Adbc.Tests
{
    public class Utils
    {
        /// <summary>
        /// Indicates if a test can run.
        /// </summary>
        /// <param name="environmentVariable">
        /// The environment variable that contains the location of the config file.
        /// </param>
        public static bool CanExecuteTestConfig(string environmentVariable)
        {
            return CanExecuteTest(environmentVariable, out _);
        }

        /// <summary>
        /// Indicates if a test can run.
        /// </summary>
        /// <param name="environmentVariable">
        /// The environment variable that contains the location of the config file.
        /// </param>
        /// <param name="environmentValue">
        /// The value from the environment variable.
        /// </param>
        public static bool CanExecuteTest(string environmentVariable, out string? environmentValue)
        {
            if (!string.IsNullOrWhiteSpace(environmentVariable))
            {
                environmentValue = Environment.GetEnvironmentVariable(environmentVariable);

                if (!string.IsNullOrWhiteSpace(environmentValue))
                {
                    if (File.Exists(environmentValue))
                    {
                        return true;
                    }
                }
            }

            Console.WriteLine($"Cannot load test configuration from environment variable {environmentVariable}. The execution of this test will be skipped.");

            environmentValue = string.Empty;
            return false;
        }

        /// <summary>
        /// Loads a test configuration
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="environmentVariable">
        /// The name of the environment variable.
        /// </param>
        /// <returns>T</returns>
        public static T LoadTestConfiguration<T>(string environmentVariable)
            where T : TestConfiguration
        {
            if (CanExecuteTest(environmentVariable, out string? environmentValue))
                return GetTestConfiguration<T>(environmentValue);

            throw new InvalidOperationException($"Cannot execute test configuration from environment variable `{environmentVariable}`");
        }

        /// <summary>
        /// Loads a test configuration
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="fileName">
        /// The path of the configuration file
        /// </param>
        /// <returns>T</returns>
        public static T GetTestConfiguration<T>(string? fileName)
            where T : TestConfiguration
        {
            if (fileName == null || !File.Exists(fileName))
                throw new FileNotFoundException(fileName);

            // use a JSON file for the various settings
            string json = File.ReadAllText(fileName);

            T testConfiguration = JsonSerializer.Deserialize<T>(json)!;

            return testConfiguration;
        }
    }
}

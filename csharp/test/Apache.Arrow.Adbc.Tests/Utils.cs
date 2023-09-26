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

using System.IO;
using System.Text.Json;

namespace Apache.Arrow.Adbc.Tests
{
    public class Utils
    {
        /// <summary>
        /// Loads a test configuration
        /// </summary>
        /// <typeparam name="T">Return type</typeparam>
        /// <param name="fileName">
        /// The path of the configuration file
        /// </param>
        /// <returns>T</returns>
        public static T GetTestConfiguration<T>(string fileName)
            where T : TestConfiguration
        {
            // use a JSON file vs. setting up environment variables
            string json = File.ReadAllText(fileName);

            T testConfiguration = JsonSerializer.Deserialize<T>(json);

            return testConfiguration;
        }
    }
}

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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Tests
{
    public abstract class MultiEnvironmentTestConfiguration<T>
        where T : TestConfiguration
    {
        /// <summary>
        /// List of test environments that are used when running tests.
        /// </summary>
        /// <remarks>
        /// Multiple environments can be defined in the Environments dictionary. This is an array
        /// of testable environment names in the JSON file:
        ///   "testEnvironments": [ "Environment_A", "Environment_B", "Environment_C" ]
        ///
        /// These names match the keys in the Environments dictionary.
        /// </remarks>
        [JsonPropertyName("testEnvironments")]
        public List<string> TestEnvironmentNames { get; set; } = new List<string>();

        /// <summary>
        /// Contains the configured environments where the key is the name of the environment
        /// and the value is the test configuration for that environment.
        /// </summary>
        [JsonPropertyName("environments")]
        public Dictionary<string, T> Environments { get; set; } = new Dictionary<string, T>();

        /// <summary>
        /// Values that are shared across environments so they don't need to be repeated.
        /// </summary>
        [JsonPropertyName("shared")]
        public Dictionary<string, string> SharedKeyValuePairs { get; set; } = new Dictionary<string, string>();
    }
}

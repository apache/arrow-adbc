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
using System.Linq;
using System.Text;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    internal class SparkTestingUtils
    {
        internal const string SPARK_TEST_CONFIG_VARIABLE = "SPARK_TEST_CONFIG_FILE";

        /// <summary>
        /// Gets a the Spark ADBC driver with settings from the <see cref="SparkTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"><see cref="SparkTestConfiguration"/></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcConnection GetSparkAdbcConnection(
            SparkTestConfiguration testConfiguration
           )
        {
            Dictionary<string, string> parameters = GetSparkParameters(testConfiguration);
            AdbcDatabase database = new SparkDriver().Open(parameters);
            AdbcConnection connection = database.Connect(new Dictionary<string, string>());

            return connection;
        }

        /// <summary>
        /// Gets the parameters for connecting to Spark.
        /// </summary>
        /// <param name="testConfiguration"><see cref="SparkTestConfiguration"/></param>
        /// <returns></returns>
        internal static Dictionary<string, string> GetSparkParameters(SparkTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // TODO: make these parameters that are passed in

            if (!string.IsNullOrEmpty(testConfiguration.HostName))
            {
                parameters.Add("HostName", testConfiguration.HostName);
            }

            if (!string.IsNullOrEmpty(testConfiguration.Path))
            {
                parameters.Add("Path", testConfiguration.Path);
            }

            if (!string.IsNullOrEmpty(testConfiguration.Token))
            {
                parameters.Add("Token", testConfiguration.Token);
            }

            return parameters;
        }

        /// <summary>
        /// Parses the queries from resources/SparkData.sql
        /// </summary>
        /// <param name="testConfiguration"><see cref="SparkTestConfiguration"/></param>
        internal static string[] GetQueries(SparkTestConfiguration testConfiguration)
        {
            const string placeholder = "{ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}";
            string[] sql = File.ReadAllLines("Spark/Resources/SparkData.sql");

            StringBuilder content = new StringBuilder();
            foreach (string line in sql)
            {
                if (line.TrimStart().StartsWith("--")) { continue; }
                if (line.Contains(placeholder))
                {
                    // TODO: Try 3-level name
                    string modifiedLine = line.Replace(placeholder, $"{testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{testConfiguration.Metadata.Table}");
                    content.AppendLine(modifiedLine);
                }
                else
                {
                    content.AppendLine(line);
                }
            }

            string[] queries = content.ToString().Split(";".ToCharArray()).Where(x => x.Trim().Length > 0).ToArray();

            return queries;
        }
    }
}

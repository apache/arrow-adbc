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

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Apache.Arrow.Adbc.C;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    internal class SnowflakeTestingUtils
    {
        internal const string SNOWFLAKE_TEST_CONFIG_VARIABLE = "SNOWFLAKE_TEST_CONFIG_FILE";

        /// <summary>
        /// Gets a the Snowflake ADBC driver with settings from the
        /// <see cref="SnowflakeTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcDriver GetSnowflakeAdbcDriver(
            SnowflakeTestConfiguration testConfiguration,
            out Dictionary<string, string> parameters
           )
        {
            // see https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html

            parameters = new Dictionary<string, string>
            {
                { "adbc.snowflake.sql.account", testConfiguration.Account },
                { "username", testConfiguration.User },
                { "password", testConfiguration.Password },
                { "adbc.snowflake.sql.warehouse", testConfiguration.Warehouse },
                { "adbc.snowflake.sql.auth_type", testConfiguration.AuthenticationType }
            };

            Dictionary<string, string> options = new Dictionary<string, string>() { };
            AdbcDriver snowflakeDriver = CAdbcDriverImporter.Load(testConfiguration.DriverPath, testConfiguration.DriverEntryPoint);

            return snowflakeDriver;
        }

        /// <summary>
        /// Gets a the Snowflake ADBC driver with settings from the
        /// <see cref="SnowflakeTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcDriver GetSnowflakeAdbcDriver(
            SnowflakeTestConfiguration testConfiguration
           )
        {
            Dictionary<string, string> options = new Dictionary<string, string>() { };
            AdbcDriver snowflakeDriver = CAdbcDriverImporter.Load(testConfiguration.DriverPath, testConfiguration.DriverEntryPoint);

            return snowflakeDriver;
        }

        /// <summary>
        /// Parses the queries from resources/SnowflakeData.sql
        /// </summary>
        /// <param name="testConfiguration"><see cref="SnowflakeTestConfiguration"/></param>
        internal static string[] GetQueries(SnowflakeTestConfiguration testConfiguration)
        {
            StringBuilder content = new StringBuilder();

            string[] sql = File.ReadAllLines("resources/SnowflakeData.sql");

            string placeholder = "{ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}";

            foreach (string line in sql)
            {
                if (!line.TrimStart().StartsWith("--"))
                {
                    if (line.Contains(placeholder))
                    {
                        string modifiedLine = line.Replace(placeholder, $"{testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{testConfiguration.Metadata.Table}");

                        content.AppendLine(modifiedLine);
                    }
                    else
                    {
                        content.AppendLine(line);
                    }
                }
            }

            string[] queries = content.ToString().Split(";".ToCharArray()).Where(x => x.Trim().Length > 0).ToArray();

            return queries;
        }
    }
}

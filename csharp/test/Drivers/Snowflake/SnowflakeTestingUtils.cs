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
using Apache.Arrow.Adbc.C;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    internal class SnowflakeParameters
    {
        public const string DATABASE = "adbc.snowflake.sql.db";
        public const string SCHEMA = "adbc.snowflake.sql.schema";
        public const string ACCOUNT = "adbc.snowflake.sql.account";
        public const string USERNAME = "username";
        public const string PASSWORD = "password";
        public const string WAREHOUSE = "adbc.snowflake.sql.warehouse";
        public const string AUTH_TYPE = "adbc.snowflake.sql.auth_type";
        public const string AUTH_TOKEN = "adbc.snowflake.sql.client_option.auth_token";
        public const string HOST = "adbc.snowflake.sql.uri.host";
        public const string PKCS8_VALUE = "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value";
        public const string PKCS8_PASS = "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password";
        public const string USE_HIGH_PRECISION = "adbc.snowflake.sql.client_option.use_high_precision";
    }

    internal class SnowflakeTestingUtils
    {
        internal static readonly SnowflakeTestConfiguration TestConfiguration;

        internal const string SNOWFLAKE_TEST_CONFIG_VARIABLE = "SNOWFLAKE_TEST_CONFIG_FILE";

        static SnowflakeTestingUtils()
        {
            try
            {
                TestConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

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
                { SnowflakeParameters.ACCOUNT, testConfiguration.Account },
                { SnowflakeParameters.USERNAME, testConfiguration.User },
                { SnowflakeParameters.PASSWORD, testConfiguration.Password },
                { SnowflakeParameters.WAREHOUSE, testConfiguration.Warehouse },
                { SnowflakeParameters.USE_HIGH_PRECISION, testConfiguration.UseHighPrecision.ToString().ToLowerInvariant() }
            };

            if(testConfiguration.Authentication.Default is not null)
            {
                parameters[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthSnowflake;
                parameters[SnowflakeParameters.USERNAME] = testConfiguration.Authentication.Default.User;
                parameters[SnowflakeParameters.PASSWORD] = testConfiguration.Authentication.Default.Password;
            }

            if(!string.IsNullOrWhiteSpace(testConfiguration.Host))
            {
                parameters[SnowflakeParameters.HOST] = testConfiguration.Host;
            }

            if (!string.IsNullOrWhiteSpace(testConfiguration.Database))
            {
                parameters[SnowflakeParameters.DATABASE] = testConfiguration.Database;
            }

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

            Dictionary<string, string> placeholderValues = new Dictionary<string, string>() {
                {"{ADBC_CATALOG}", testConfiguration.Metadata.Catalog },
                {"{ADBC_SCHEMA}", testConfiguration.Metadata.Schema },
                {"{ADBC_TABLE}", testConfiguration.Metadata.Table }
            };

            foreach (string line in sql)
            {
                if (!line.TrimStart().StartsWith("--"))
                {
                    string modifiedLine = line;

                    foreach (string key in placeholderValues.Keys)
                    {
                        if (modifiedLine.Contains(key))
                            modifiedLine = modifiedLine.Replace(key, placeholderValues[key]);
                    }

                    content.AppendLine(modifiedLine);
                }
            }

            string[] queries = content.ToString().Split(";".ToCharArray()).Where(x => x.Trim().Length > 0).ToArray();

            return queries;
        }
    }
}

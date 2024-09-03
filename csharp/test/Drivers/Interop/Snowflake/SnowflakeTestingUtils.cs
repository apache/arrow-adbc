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
using System.Reflection;
using System.Text;
using Apache.Arrow.Adbc.Drivers.Interop.Snowflake;
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
        private static readonly Assembly CurrentAssembly;

        internal const string SNOWFLAKE_TEST_CONFIG_VARIABLE = "SNOWFLAKE_TEST_CONFIG_FILE";
        private const string SNOWFLAKE_DATA_RESOURCE = "Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake.Resources.SnowflakeData.sql";

        static SnowflakeTestingUtils()
        {
            try
            {
                TestConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine(ex.Message);
                TestConfiguration = new SnowflakeTestConfiguration();
            }

            CurrentAssembly = Assembly.GetExecutingAssembly();
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
                { SnowflakeParameters.ACCOUNT, Parameter(testConfiguration.Account, "account") },
                { SnowflakeParameters.USERNAME, Parameter(testConfiguration.User, "userName") },
                { SnowflakeParameters.PASSWORD, Parameter(testConfiguration.Password, "password") },
                { SnowflakeParameters.WAREHOUSE, Parameter(testConfiguration.Warehouse, "warehouse") },
                { SnowflakeParameters.USE_HIGH_PRECISION, testConfiguration.UseHighPrecision.ToString().ToLowerInvariant() }
            };

            if (testConfiguration.Authentication.Default is not null)
            {
                parameters[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthSnowflake;
                parameters[SnowflakeParameters.USERNAME] = Parameter(testConfiguration.Authentication.Default.User, "username");
                parameters[SnowflakeParameters.PASSWORD] = Parameter(testConfiguration.Authentication.Default.Password, "password");
            }

            if (!string.IsNullOrWhiteSpace(testConfiguration.Host))
            {
                parameters[SnowflakeParameters.HOST] = testConfiguration.Host;
            }

            if (!string.IsNullOrWhiteSpace(testConfiguration.Database))
            {
                parameters[SnowflakeParameters.DATABASE] = testConfiguration.Database;
            }

            Dictionary<string, string> options = new Dictionary<string, string>() { };
            AdbcDriver snowflakeDriver = GetSnowflakeAdbcDriver(testConfiguration);

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
            AdbcDriver snowflakeDriver;

            if (testConfiguration == null || string.IsNullOrEmpty(testConfiguration.DriverPath) || string.IsNullOrEmpty(testConfiguration.DriverEntryPoint))
            {
                snowflakeDriver = SnowflakeDriverLoader.LoadDriver();
            }
            else
            {
                snowflakeDriver = SnowflakeDriverLoader.LoadDriver(testConfiguration.DriverPath, testConfiguration.DriverEntryPoint);
            }

            return snowflakeDriver;
        }

        /// <summary>
        /// Parses the queries from resources/SnowflakeData.sql
        /// </summary>
        /// <param name="testConfiguration"><see cref="SnowflakeTestConfiguration"/></param>
        internal static string[] GetQueries(SnowflakeTestConfiguration testConfiguration, string resourceName = SNOWFLAKE_DATA_RESOURCE)
        {
            StringBuilder content = new StringBuilder();

            string[]? sql = null;

            try
            {
                using (Stream? stream = CurrentAssembly.GetManifestResourceStream(resourceName))
                {
                    if (stream != null)
                    {
                        using (StreamReader sr = new StreamReader(stream))
                        {
                            sql = sr.ReadToEnd().Split(new[] { Environment.NewLine }, StringSplitOptions.None);
                        }
                    }
                    else
                    {
                        throw new FileNotFoundException("Embedded resource not found", resourceName);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occured while reading the resouce: {resourceName}");
                Console.WriteLine(ex.Message);
                throw;
            }

            Dictionary<string, string> placeholderValues = new Dictionary<string, string>() {
                {"{ADBC_CATALOG}", Parameter(testConfiguration.Metadata.Catalog, "catalog") },
                {"{ADBC_SCHEMA}", Parameter(testConfiguration.Metadata.Schema, "schema") },
                {"{ADBC_TABLE}", Parameter(testConfiguration.Metadata.Table, "table") }
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

        /// <summary>
        /// Assert that all expected texts in collection appear in the value.
        /// </summary>
        /// <param name="expectedTexts"></param>
        /// <param name="value"></param>
        internal static void AssertContainsAll(string[]? expectedTexts, string value)
        {
            if (expectedTexts == null) { return; };
            foreach (string text in expectedTexts)
            {
                Assert.Contains(text, value);
            }
        }

        private static string Parameter(string? value, string parameterName)
        {
            if (value == null) throw new ArgumentNullException(parameterName);
            return value;
        }
    }
}

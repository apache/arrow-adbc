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
using System.Text.Json;
using Apache.Arrow.Adbc.Drivers.Interop.FlightSql;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.FlightSql
{
    public class FlightSqlParameters
    {
        public const string Uri = "uri";
        public const string OptionAuthorizationHeader = "adbc.flight.sql.authorization_header";
        public const string OptionRPCCallHeaderPrefix = "adbc.flight.sql.rpc.call_header.";
        public const string OptionTimeoutFetch = "adbc.flight.sql.rpc.timeout_seconds.fetch";
        public const string OptionTimeoutQuery = "adbc.flight.sql.rpc.timeout_seconds.query";
        public const string OptionTimeoutUpdate = "adbc.flight.sql.rpc.timeout_seconds.update";
        public const string OptionSSLSkipVerify = "adbc.flight.sql.client_option.tls_skip_verify";
        public const string OptionAuthority = "adbc.flight.sql.client_option.authority";
        public const string Username = "username";
        public const string Password = "password";

        // not used, but also available:
        //public const string OptionMTLSCertChain = "adbc.flight.sql.client_option.mtls_cert_chain";
        //public const string OptionMTLSPrivateKey = "adbc.flight.sql.client_option.mtls_private_key";
        //public const string OptionSSLOverrideHostname = "adbc.flight.sql.client_option.tls_override_hostname";
        //public const string OptionSSLRootCerts = "adbc.flight.sql.client_option.tls_root_certs";
        //public const string OptionWithBlock = "adbc.flight.sql.client_option.with_block";
        //public const string OptionWithMaxMsgSize = "adbc.flight.sql.client_option.with_max_msg_size";
        //public const string OptionCookieMiddleware = "adbc.flight.sql.rpc.with_cookie_middleware";
    }

    internal class FlightSqlTestingUtils
    {
        internal const string FLIGHTSQL_TEST_CONFIG_VARIABLE = "FLIGHTSQL_TEST_CONFIG_FILE";
        internal const string FLIGHTSQL_TEST_ENV_NAME = "FLIGHTSQL_TEST_ENV_NAME";

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

        public static FlightSqlTestConfiguration LoadFlightSqlTestConfiguration(string? environmentVariable = null)
        {
            if(string.IsNullOrEmpty(environmentVariable))
                environmentVariable = FLIGHTSQL_TEST_CONFIG_VARIABLE;

            FlightSqlTestConfiguration? testConfiguration = null;

            if (!string.IsNullOrWhiteSpace(environmentVariable))
            {
                string? environmentValue = Environment.GetEnvironmentVariable(environmentVariable);

                if (!string.IsNullOrWhiteSpace(environmentValue))
                {
                    if (File.Exists(environmentValue))
                    {
                        // use a JSON file for the various settings
                        string json = File.ReadAllText(environmentValue);

                        testConfiguration = JsonSerializer.Deserialize<FlightSqlTestConfiguration>(json)!;
                    }
                }
            }

            if (testConfiguration == null)
                throw new InvalidOperationException($"Cannot execute test configuration from environment variable `{environmentVariable}`");

            return testConfiguration;
        }

        internal static FlightSqlTestEnvironment GetTestEnvironment(FlightSqlTestConfiguration testConfiguration)
        {
            if (testConfiguration == null)
                throw new ArgumentNullException(nameof(testConfiguration));

            if (testConfiguration.Environments == null || testConfiguration.Environments.Count == 0)
                throw new InvalidOperationException("There are no environments configured");

            FlightSqlTestEnvironment? environment = null;

            // the user can specify a test environment:
            // - in the config,
            // - in the environment variable
            // - attempt to just use the first one from the config
            if (string.IsNullOrEmpty(testConfiguration.TestEnvironmentName))
            {
                string? testEnvNameFromEnvVariable = Environment.GetEnvironmentVariable(FlightSqlTestingUtils.FLIGHTSQL_TEST_ENV_NAME);

                if (string.IsNullOrEmpty(testEnvNameFromEnvVariable))
                {
                    if (testConfiguration.Environments.Count > 0)
                    {
                        environment = testConfiguration.Environments.Values.First();
                    }
                }
                else
                {
                    if (testConfiguration.Environments.TryGetValue(testEnvNameFromEnvVariable!, out FlightSqlTestEnvironment? flightSqlTestEnvironment))
                        environment = flightSqlTestEnvironment!;
                }
            }
            else
            {
                if (testConfiguration.Environments.TryGetValue(testConfiguration.TestEnvironmentName!, out FlightSqlTestEnvironment? flightSqlTestEnvironment))
                    environment = flightSqlTestEnvironment!;
            }

           if (environment == null)
                throw new InvalidOperationException("Could not find a configured Flight SQL environment");

            return environment;
        }

        private void TrySetFlightSqlEnvironment(string? environmentName)
        {
            if (string.IsNullOrEmpty(environmentName))
                return;


        }

        /// <summary>
        /// Gets a the Snowflake ADBC driver with settings from the
        /// <see cref="FlightSqlTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcDriver GetAdbcDriver(
            FlightSqlTestConfiguration testConfiguration,
            FlightSqlTestEnvironment environment,
            out Dictionary<string, string> parameters
           )
        {
            // see https://arrow.apache.org/adbc/main/driver/flight_sql.html

            parameters = new Dictionary<string, string>{};

            if(!string.IsNullOrEmpty(environment.Uri))
            {
                parameters.Add(FlightSqlParameters.Uri, environment.Uri!);
            }

            foreach(string key in environment.RPCCallHeaders.Keys)
            {
                parameters.Add(FlightSqlParameters.OptionRPCCallHeaderPrefix + key, environment.RPCCallHeaders[key]);
            }

            if (!string.IsNullOrEmpty(environment.AuthorizationHeader))
            {
                parameters.Add(FlightSqlParameters.OptionAuthorizationHeader, environment.AuthorizationHeader!);
            }
            else
            {
                if (!string.IsNullOrEmpty(environment.Username) && !string.IsNullOrEmpty(environment.Password))
                {
                    parameters.Add(FlightSqlParameters.Username, environment.Username!);
                    parameters.Add(FlightSqlParameters.Password, environment.Password!);
                }
            }

            if (!string.IsNullOrEmpty(environment.TimeoutQuery))
                parameters.Add(FlightSqlParameters.OptionTimeoutQuery, environment.TimeoutQuery!);

            if (!string.IsNullOrEmpty(environment.TimeoutFetch))
                parameters.Add(FlightSqlParameters.OptionTimeoutFetch, environment.TimeoutFetch!);

            if (!string.IsNullOrEmpty(environment.TimeoutUpdate))
                parameters.Add(FlightSqlParameters.OptionTimeoutUpdate, environment.TimeoutUpdate!);

            if (environment.SSLSkipVerify)
                parameters.Add(FlightSqlParameters.OptionSSLSkipVerify, Convert.ToString(environment.SSLSkipVerify).ToLowerInvariant());

            if (!string.IsNullOrEmpty(environment.Authority))
                parameters.Add(FlightSqlParameters.OptionAuthority, environment.Authority!);

            Dictionary<string, string> options = new Dictionary<string, string>() { };
            AdbcDriver driver = GetFlightSqlAdbcDriver(testConfiguration);

            return driver;
        }

        /// <summary>
        /// Gets a the Flight SQL ADBC driver with settings from the
        /// <see cref="FlightSqlTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcDriver GetFlightSqlAdbcDriver(
            FlightSqlTestConfiguration testConfiguration
           )
        {
            AdbcDriver driver;

            if (testConfiguration == null || string.IsNullOrEmpty(testConfiguration.DriverPath) || string.IsNullOrEmpty(testConfiguration.DriverEntryPoint))
            {
                driver = FlightSqlDriverLoader.LoadDriver();
            }
            else
            {
                driver = FlightSqlDriverLoader.LoadDriver(testConfiguration.DriverPath!, testConfiguration.DriverEntryPoint!);
            }

            return driver;
        }

        /// <summary>
        /// Parses the queries from resources/FlightSqlData.sql
        /// </summary>
        /// <param name="environment"><see cref="FlightSqlTestEnvironment"/></param>
        internal static string[] GetQueries(FlightSqlTestEnvironment environment)
        {
            StringBuilder content = new StringBuilder();

            string[] sql = File.ReadAllLines("resources/FlightSqlData.sql");

            Dictionary<string, string> placeholderValues = new Dictionary<string, string>() {
                {"{ADBC_CATALOG}", environment.Metadata.Catalog },
                {"{ADBC_SCHEMA}", environment.Metadata.Schema },
                {"{ADBC_TABLE}", environment.Metadata.Table }
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

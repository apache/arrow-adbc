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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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
        internal static readonly FlightSqlTestConfiguration TestConfiguration;

        internal const string FLIGHTSQL_TEST_CONFIG_VARIABLE = "FLIGHTSQL_TEST_CONFIG_FILE";

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        static FlightSqlTestingUtils()
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        {
            try
            {
                TestConfiguration = Utils.LoadTestConfiguration<FlightSqlTestConfiguration>(FlightSqlTestingUtils.FLIGHTSQL_TEST_CONFIG_VARIABLE);
            }
            catch (InvalidOperationException ex)
            {
                Console.WriteLine(ex.Message);
            }
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
            out Dictionary<string, string> parameters
           )
        {
            // see https://arrow.apache.org/adbc/main/driver/flight_sql.html

            parameters = new Dictionary<string, string>{};

            if(!string.IsNullOrEmpty(testConfiguration.Uri))
            {
                parameters.Add(FlightSqlParameters.Uri, testConfiguration.Uri!);
            }

            foreach(string key in testConfiguration.RPCCallHeaders.Keys)
            {
                parameters.Add(FlightSqlParameters.OptionRPCCallHeaderPrefix + key, testConfiguration.RPCCallHeaders[key]);
            }

            if (!string.IsNullOrEmpty(testConfiguration.AuthorizationHeader))
            {
                parameters.Add(FlightSqlParameters.OptionAuthorizationHeader, testConfiguration.AuthorizationHeader!);
            }
            else
            {
                if (!string.IsNullOrEmpty(testConfiguration.Username) && !string.IsNullOrEmpty(testConfiguration.Password))
                {
                    parameters.Add(FlightSqlParameters.Username, testConfiguration.Username!);
                    parameters.Add(FlightSqlParameters.Password, testConfiguration.Password!);
                }
            }

            if (!string.IsNullOrEmpty(testConfiguration.TimeoutQuery))
                parameters.Add(FlightSqlParameters.OptionTimeoutQuery, testConfiguration.TimeoutQuery!);

            if (!string.IsNullOrEmpty(testConfiguration.TimeoutFetch))
                parameters.Add(FlightSqlParameters.OptionTimeoutFetch, testConfiguration.TimeoutFetch!);

            if (!string.IsNullOrEmpty(testConfiguration.TimeoutUpdate))
                parameters.Add(FlightSqlParameters.OptionTimeoutUpdate, testConfiguration.TimeoutUpdate!);

            if (!string.IsNullOrEmpty(testConfiguration.SSLSkipVerify))
                parameters.Add(FlightSqlParameters.OptionSSLSkipVerify, testConfiguration.SSLSkipVerify!);

            if (!string.IsNullOrEmpty(testConfiguration.Authority))
                parameters.Add(FlightSqlParameters.OptionAuthority, testConfiguration.Authority!);

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
        /// <param name="testConfiguration"><see cref="FlightSqlTestConfiguration"/></param>
        internal static string[] GetQueries(FlightSqlTestConfiguration testConfiguration)
        {
            StringBuilder content = new StringBuilder();

            string[] sql = File.ReadAllLines("resources/FlightSqlData.sql");

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

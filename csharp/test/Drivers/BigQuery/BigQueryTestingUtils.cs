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
using Apache.Arrow.Adbc.Drivers.BigQuery;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    internal class BigQueryTestingUtils
    {
        internal const string BIGQUERY_TEST_CONFIG_VARIABLE = "BIGQUERY_TEST_CONFIG_FILE";

        /// <summary>
        /// Gets a the BigQuery ADBC driver with settings from the <see cref="BigQueryTestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"><see cref="BigQueryTestConfiguration"/></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcConnection GetBigQueryAdbcConnection(
            BigQueryTestConfiguration testConfiguration
           )
        {
            Dictionary<string, string> parameters = GetBigQueryParameters(testConfiguration);
            AdbcDatabase database = new BigQueryDriver().Open(parameters);
            AdbcConnection connection = database.Connect(new Dictionary<string, string>());

            return connection;
        }

        /// <summary>
        /// Gets the parameters for connecting to BigQuery.
        /// </summary>
        /// <param name="testConfiguration"><see cref="BigQueryTestConfiguration"/></param>
        /// <returns></returns>
        internal static Dictionary<string, string> GetBigQueryParameters(BigQueryTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
               { BigQueryParameters.ProjectId, testConfiguration.ProjectId },
            };

            if (!string.IsNullOrEmpty(testConfiguration.JsonCredential))
            {
                parameters.Add(BigQueryParameters.AuthenticationType, BigQueryConstants.ServiceAccountAuthenticationType);
                parameters.Add(BigQueryParameters.JsonCredential, testConfiguration.JsonCredential);
            }
            else
            {
                parameters.Add(BigQueryParameters.AuthenticationType, BigQueryConstants.UserAuthenticationType);
                parameters.Add(BigQueryParameters.ClientId, testConfiguration.ClientId);
                parameters.Add(BigQueryParameters.ClientSecret, testConfiguration.ClientSecret);
                parameters.Add(BigQueryParameters.RefreshToken, testConfiguration.RefreshToken);
            }

            if (!string.IsNullOrEmpty(testConfiguration.Scopes))
            {
                parameters.Add(BigQueryParameters.Scopes, testConfiguration.Scopes);
            }

            if (testConfiguration.AllowLargeResults)
            {
                parameters.Add(BigQueryParameters.AllowLargeResults, testConfiguration.AllowLargeResults.ToString());
            }

            parameters.Add(BigQueryParameters.IncludeConstraintsWithGetObjects, testConfiguration.IncludeTableConstraints.ToString());

            if (!string.IsNullOrEmpty(testConfiguration.LargeResultsDestinationTable))
            {
                parameters.Add(BigQueryParameters.LargeResultsDestinationTable, testConfiguration.LargeResultsDestinationTable);
            }

            if (testConfiguration.TimeoutMinutes.HasValue)
            {
                parameters.Add(BigQueryParameters.GetQueryResultsOptionsTimeoutMinutes, testConfiguration.TimeoutMinutes.Value.ToString());
            }

            if (testConfiguration.MaxStreamCount.HasValue)
            {
                parameters.Add(BigQueryParameters.MaxFetchConcurrency, testConfiguration.MaxStreamCount.Value.ToString());
            }

            return parameters;
        }

        /// <summary>
        /// Parses the queries from resources/BigQueryData.sql
        /// </summary>
        /// <param name="testConfiguration"><see cref="BigQueryTestConfiguration"/></param>
        internal static string[] GetQueries(BigQueryTestConfiguration testConfiguration)
        {
            // get past the license header
            StringBuilder content = new StringBuilder();

            string placeholder = "{ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}";

            string[] sql = File.ReadAllLines("resources/BigQueryData.sql");

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

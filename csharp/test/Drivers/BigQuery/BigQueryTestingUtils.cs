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
        /// Gets a the BigQuery ADBC driver with settings from the <see cref="BigQueryTestEnvironment"/>.
        /// </summary>
        /// <param name="testEnvironment"><see cref="BigQueryTestEnvironment"/></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        internal static AdbcConnection GetBigQueryAdbcConnection(
            BigQueryTestEnvironment testEnvironment
           )
        {
            Dictionary<string, string> parameters = GetBigQueryParameters(testEnvironment);
            AdbcDatabase database = new BigQueryDriver().Open(parameters);
            AdbcConnection connection = database.Connect(new Dictionary<string, string>());

            return connection;
        }

        /// <summary>
        /// Gets the parameters for connecting to BigQuery.
        /// </summary>
        /// <param name="testEnvironment"><see cref="BigQueryTestEnvironment"/></param>
        /// <returns></returns>
        internal static Dictionary<string, string> GetBigQueryParameters(BigQueryTestEnvironment testEnvironment)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>{};

            if (!string.IsNullOrEmpty(testEnvironment.ProjectId))
            {
                parameters.Add(BigQueryParameters.ProjectId, testEnvironment.ProjectId!);
            }

            if (!string.IsNullOrEmpty(testEnvironment.BillingProjectId))
            {
                parameters.Add(BigQueryParameters.BillingProjectId, testEnvironment.BillingProjectId!);
            }

            if (!string.IsNullOrEmpty(testEnvironment.JsonCredential))
            {
                parameters.Add(BigQueryParameters.AuthenticationType, BigQueryConstants.ServiceAccountAuthenticationType);
                parameters.Add(BigQueryParameters.JsonCredential, testEnvironment.JsonCredential);
            }
            else
            {
                parameters.Add(BigQueryParameters.AuthenticationType, BigQueryConstants.UserAuthenticationType);
                parameters.Add(BigQueryParameters.ClientId, testEnvironment.ClientId);
                parameters.Add(BigQueryParameters.ClientSecret, testEnvironment.ClientSecret);
                parameters.Add(BigQueryParameters.RefreshToken, testEnvironment.RefreshToken);
            }

            if (!string.IsNullOrEmpty(testEnvironment.Scopes))
            {
                parameters.Add(BigQueryParameters.Scopes, testEnvironment.Scopes);
            }

            if (testEnvironment.AllowLargeResults)
            {
                parameters.Add(BigQueryParameters.AllowLargeResults, testEnvironment.AllowLargeResults.ToString());
            }

            parameters.Add(BigQueryParameters.IncludeConstraintsWithGetObjects, testEnvironment.IncludeTableConstraints.ToString());

            parameters.Add(BigQueryParameters.IncludePublicProjectId, testEnvironment.IncludePublicProjectId.ToString());

            if (!string.IsNullOrEmpty(testEnvironment.LargeResultsDestinationTable))
            {
                parameters.Add(BigQueryParameters.LargeResultsDestinationTable, testEnvironment.LargeResultsDestinationTable);
            }

            if (testEnvironment.TimeoutMinutes.HasValue)
            {
                int seconds = testEnvironment.TimeoutMinutes.Value * 60;
                parameters.Add(BigQueryParameters.GetQueryResultsOptionsTimeout, seconds.ToString());
            }
            else if (testEnvironment.QueryTimeout.HasValue)
            {
                parameters.Add(BigQueryParameters.GetQueryResultsOptionsTimeout, testEnvironment.QueryTimeout.Value.ToString());
            }

            if (testEnvironment.ClientTimeout.HasValue)
            {
                parameters.Add(BigQueryParameters.ClientTimeout, testEnvironment.ClientTimeout.Value.ToString());
            }

            if (testEnvironment.MaxStreamCount.HasValue)
            {
                parameters.Add(BigQueryParameters.MaxFetchConcurrency, testEnvironment.MaxStreamCount.Value.ToString());
            }

            if (!string.IsNullOrEmpty(testEnvironment.StatementType))
            {
                parameters.Add(BigQueryParameters.StatementType, testEnvironment.StatementType!);
            }

            if (testEnvironment.StatementIndex.HasValue)
            {
                parameters.Add(BigQueryParameters.StatementIndex, testEnvironment.StatementIndex.Value.ToString());
            }

            return parameters;
        }

        /// <summary>
        /// Parses the queries from resources/BigQueryData.sql
        /// </summary>
        /// <param name="testEnvironment"><see cref="BigQueryTestEnvironment"/></param>
        internal static string[] GetQueries(BigQueryTestEnvironment testEnvironment)
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
                        string modifiedLine = line.Replace(placeholder, $"{testEnvironment.Metadata.Catalog}.{testEnvironment.Metadata.Schema}.{testEnvironment.Metadata.Table}");

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

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

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    /// <summary>
    /// Parameters used for connecting to BigQuery data sources.
    /// </summary>
    internal class BigQueryParameters
    {
        public const string AccessToken = "adbc.bigquery.access_token";
        public const string AllowLargeResults = "adbc.bigquery.allow_large_results";
        public const string AudienceUri = "adbc.bigquery.audience_uri";
        public const string AuthenticationType = "adbc.bigquery.auth_type";
        public const string BillingProjectId = "adbc.bigquery.billing_project_id";
        public const string ClientId = "adbc.bigquery.client_id";
        public const string ClientSecret = "adbc.bigquery.client_secret";
        public const string ClientTimeout = "adbc.bigquery.client.timeout";
        public const string EvaluationKind = "adbc.bigquery.multiple_statement.evaluation_kind";
        public const string GetQueryResultsOptionsTimeout = "adbc.bigquery.get_query_results_options.timeout";
        public const string IncludeConstraintsWithGetObjects = "adbc.bigquery.include_constraints_getobjects";
        public const string IncludePublicProjectId = "adbc.bigquery.include_public_project_id";
        public const string JsonCredential = "adbc.bigquery.auth_json_credential";
        public const string LargeDecimalsAsString = "adbc.bigquery.large_decimals_as_string";
        public const string LargeResultsDataset = "adbc.bigquery.large_results_dataset";
        public const string LargeResultsDestinationTable = "adbc.bigquery.large_results_destination_table";
        public const string MaxFetchConcurrency = "adbc.bigquery.max_fetch_concurrency";
        public const string MaximumRetryAttempts = "adbc.bigquery.maximum_retries";
        public const string ProjectId = "adbc.bigquery.project_id";
        public const string RefreshToken = "adbc.bigquery.refresh_token";
        public const string RetryDelayMs = "adbc.bigquery.retry_delay_ms";
        public const string Scopes = "adbc.bigquery.scopes";
        public const string StatementIndex = "adbc.bigquery.multiple_statement.statement_index";
        public const string StatementType = "adbc.bigquery.multiple_statement.statement_type";
        public const string UseLegacySQL = "adbc.bigquery.use_legacy_sql";

        // these values are safe to log any time
        private static HashSet<string> safeToLog = new HashSet<string>()
        {
            AllowLargeResults, AuthenticationType, BillingProjectId, ClientId, ClientTimeout,
            EvaluationKind, GetQueryResultsOptionsTimeout, IncludeConstraintsWithGetObjects,
            IncludePublicProjectId, LargeDecimalsAsString, LargeResultsDataset, LargeResultsDestinationTable,
            MaxFetchConcurrency, MaximumRetryAttempts, ProjectId, RetryDelayMs, StatementIndex,
            StatementType, UseLegacySQL
        };

        public static bool IsSafeToLog(string name)
        {
            if (safeToLog.Contains(name.ToLower()))
                return true;

            return false;
        }
    }

    /// <summary>
    /// Constants used for default parameter values.
    /// </summary>
    internal class BigQueryConstants
    {
        public const string UserAuthenticationType = "user";
        public const string EntraIdAuthenticationType = "aad";
        public const string ServiceAccountAuthenticationType = "service";
        public const string TokenEndpoint = "https://accounts.google.com/o/oauth2/token";
        public const string TreatLargeDecimalAsString = "true";

        // Entra ID / Azure AD constants
        public const string EntraGrantType = "urn:ietf:params:oauth:grant-type:token-exchange";
        public const string EntraSubjectTokenType = "urn:ietf:params:oauth:token-type:id_token";
        public const string EntraRequestedTokenType = "urn:ietf:params:oauth:token-type:access_token";
        public const string EntraIdScope = "https://www.googleapis.com/auth/cloud-platform";
        public const string EntraStsTokenEndpoint = "https://sts.googleapis.com/v1/token";

        // default value per https://pkg.go.dev/cloud.google.com/go/bigquery#section-readme
        public const string DetectProjectId = "*detect-project-id*";

        // Reuse what the ODBC driver already has in place, in case a caller
        // has permission issues trying to create a new dataset
        public const string DefaultLargeDatasetId = "_bqodbc_temp_tables";

        public const string PublicProjectId = "bigquery-public-data";
    }
}

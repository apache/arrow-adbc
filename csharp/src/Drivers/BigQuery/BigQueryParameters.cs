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
using System.Xml.Linq;

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
        public const string DefaultClientLocation = "adbc.bigquery.default_client_location";
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
        private static HashSet<string> safeToLog = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            AllowLargeResults, AuthenticationType, BillingProjectId, ClientId, ClientTimeout, DefaultClientLocation, EvaluationKind, GetQueryResultsOptionsTimeout,
            EvaluationKind, GetQueryResultsOptionsTimeout, IncludeConstraintsWithGetObjects,
            IncludePublicProjectId, LargeDecimalsAsString, LargeResultsDataset, LargeResultsDestinationTable,
            MaxFetchConcurrency, MaximumRetryAttempts, ProjectId, RetryDelayMs, StatementIndex,
            StatementType, UseLegacySQL
        };

        public static bool IsSafeToLog(string name)
        {
            if (safeToLog.Contains(name))
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

        // matches the pattern for odbc, but for adbc
        public const string DefaultLargeDatasetId = "_bqadbc_temp_tables";

        public const string PublicProjectId = "bigquery-public-data";

        // this is what the BigQuery API uses as the default location
        public const string DefaultClientLocation = "US";

        // from https://cloud.google.com/bigquery/docs/locations#locations_and_regions as of Sept 28, 2025
        public static IReadOnlyList<string> ValidLocations = new List<string>()
        {
            //multi-regions
            "US",
            "EU",
            // Americas
            "us-east5",
            "us-south1",
            "us-central1",
            "us-west4",
            "us-west2",
            "northamerica-south1",
            "northamerica-northeast1",
            "us-east4",
            "us-west1",
            "us-west3",
            "southamerica-east1",
            "southamerica-west1",
            "us-east1",
            "northamerica-northeast2",
            // Asia Pacific
            "asia-south2",
            "asia-east2",
            "asia-southeast2",
            "australia-southeast2",
            "asia-south1",
            "asia-northeast2",
            "asia-northeast3",
            "asia-southeast1",
            "australia-southeast1",
            "asia-east1",
            "asia-northeast1",
            // Europe
            "europe-west1",
            "europe-west10",
            "europe-north1",
            "europe-west3",
            "europe-west2",
            "europe-southwest1",
            "europe-west8",
            "europe-west4",
            "europe-west9",
            "europe-north2",
            "europe-west12",
            "europe-central2",
            "europe-west6",
            // Middle East
            "me-central2",
            "me-central1",
            "me-west1",
            // Africa
            "africa-south1"
        };
    }
}

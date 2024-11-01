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

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    /// <summary>
    /// Parameters used for connecting to BigQuery data sources.
    /// </summary>
    public class BigQueryParameters
    {
        public const string ProjectId = "adbc.bigquery.project_id";
        public const string ClientId = "adbc.bigquery.client_id";
        public const string ClientSecret = "adbc.bigquery.client_secret";
        public const string RefreshToken = "adbc.bigquery.refresh_token";
        public const string AuthenticationType = "adbc.bigquery.auth_type";
        public const string JsonCredential = "adbc.bigquery.auth_json_credential";
        public const string AllowLargeResults = "adbc.bigquery.allow_large_results";
        public const string LargeResultsDestinationTable = "adbc.bigquery.large_results_destination_table";
        public const string UseLegacySQL = "adbc.bigquery.use_legacy_sql";
        public const string LargeDecimalsAsString = "adbc.bigquery.large_decimals_as_string";
        public const string Scopes = "adbc.bigquery.scopes";
        public const string IncludeConstraintsWithGetObjects = "adbc.bigquery.include_constraints_getobjects";
        public const string GetQueryResultsOptionsTimeoutMinutes = "adbc.bigquery.get_query_results_options.timeout";
        public const string MaxFetchConcurrency = "adbc.bigquery.max_fetch_concurrency";
    }

    /// <summary>
    /// Constants used for default parameter values.
    /// </summary>
    public class BigQueryConstants
    {
        public const string UserAuthenticationType = "user";
        public const string ServiceAccountAuthenticationType = "service";
        public const string TokenEndpoint = "https://accounts.google.com/o/oauth2/token";
        public const string TreatLargeDecimalAsString = "true";
    }
}

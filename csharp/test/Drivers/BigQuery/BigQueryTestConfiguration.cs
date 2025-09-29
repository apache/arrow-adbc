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
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    /// <summary>
    /// Configuration settings for working with BigQuery.
    /// </summary>
    internal class BigQueryTestConfiguration : MultiEnvironmentTestConfiguration<BigQueryTestEnvironment>
    {
    }

    /// <summary>
    /// Configuration environments for working with BigQuery.
    /// </summary>
    internal class BigQueryTestEnvironment : TestConfiguration
    {
        public BigQueryTestEnvironment()
        {
            AllowLargeResults = false;
            IncludeTableConstraints = true;
            ParallelQueries = new List<ParallelQuery>();
        }

        [JsonPropertyName("projectId")]
        public string? ProjectId { get; set; }

        [JsonPropertyName("authenticationType")]
        public string AuthenticationType { get; set; } = string.Empty;

        [JsonPropertyName("accessToken")]
        public string AccessToken { get; set; } = string.Empty;

        [JsonPropertyName("audience")]
        public string Audience { get; set; } = string.Empty;

        [JsonPropertyName("billingProjectId")]
        public string? BillingProjectId { get; set; }

        [JsonPropertyName("clientId")]
        public string ClientId { get; set; } = string.Empty;

        [JsonPropertyName("clientSecret")]
        public string ClientSecret { get; set; } = string.Empty;

        [JsonPropertyName("refreshToken")]
        public string RefreshToken { get; set; } = string.Empty;

        [JsonPropertyName("scopes")]
        public string Scopes { get; set; } = string.Empty;

        [JsonPropertyName("jsonCredential")]
        public string JsonCredential { get; set; } = string.Empty;

        [JsonPropertyName("allowLargeResults")]
        public bool AllowLargeResults { get; set; }

        [JsonPropertyName("largeResultsDataset")]
        public string LargeResultsDataset { get; set; } = string.Empty;

        [JsonPropertyName("largeResultsDestinationTable")]
        public string LargeResultsDestinationTable { get; set; } = string.Empty;

        [JsonPropertyName("includeTableConstraints")]
        public bool IncludeTableConstraints { get; set; }

        [JsonPropertyName("includePublicProjectId")]
        public bool IncludePublicProjectId { get; set; } = false;

        /// <summary>
        /// Sets the query timeout (in minutes).
        /// </summary>
        [JsonPropertyName("timeoutMinutes")]
        public int? TimeoutMinutes { get; set; }

        /// <summary>
        /// Sets the query timeout (in seconds).
        /// </summary>
        [JsonPropertyName("queryTimeout")]
        public int? QueryTimeout { get; set; }

        /// <summary>
        /// The number of seconds to allow for the HttpClient timeout.
        /// </summary>
        [JsonPropertyName("clientTimeout")]
        public int? ClientTimeout { get; set; }

        /// <summary>
        /// Indicates if timeout tests should run during this execution.
        /// </summary>
        [JsonPropertyName("runTimeoutTests")]
        public bool RunTimeoutTests { get; set; } = false;

        [JsonPropertyName("maxStreamCount")]
        public int? MaxStreamCount { get; set; }

        [JsonPropertyName("statementType")]
        public string StatementType { get; set; } = string.Empty;

        [JsonPropertyName("statementIndex")]
        public int? StatementIndex { get; set; }

        [JsonPropertyName("evaluationKind")]
        public string EvaluationKind { get; set; } = string.Empty;

        [JsonPropertyName("location")]
        public string ClientLocation { get; set; } = "US";


        /// <summary>
        /// How structs should be handled by the ADO.NET client for this environment.
        /// </summary>
        /// <remarks>
        /// JsonString or Strict
        /// </remarks>
        [JsonPropertyName("structBehavior")]
        public string? StructBehavior { get; set; }

        [JsonPropertyName("entraConfiguration")]
        public EntraConfiguration? EntraConfiguration { get; set; }

        /// <summary>
        /// The number of times to repeat the parallel runs.
        /// </summary>
        [JsonPropertyName("numberOfParallelRuns")]
        public int NumberOfParallelRuns { get; set; }

        [JsonPropertyName("queries")]
        public List<ParallelQuery> ParallelQueries { get; set; }
    }

    class ParallelQuery
    {
        /// <summary>
        /// The query to run.
        /// </summary>
        [JsonPropertyName("query")]
        public string Query { get; set; } = string.Empty;

        /// <summary>
        /// The number of expected results from the query.
        /// </summary>
        [JsonPropertyName("expectedResults")]
        public long ExpectedResultsCount { get; set; }
    }

    class EntraConfiguration
    {
        [JsonPropertyName("scopes")]
        public string[]? Scopes { get; set; }

        [JsonPropertyName("claims")]
        public Dictionary<string, string>? Claims { get; set; }
    }
}

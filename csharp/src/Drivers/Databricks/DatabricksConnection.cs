﻿/*
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
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    internal class DatabricksConnection : SparkHttpConnection
    {
        private bool _applySSPWithQueries = false;
        private bool _enableDirectResults = true;

        internal static TSparkGetDirectResults defaultGetDirectResults = new()
        {
            MaxRows = 2000000,
            MaxBytes = 404857600
        };

        // CloudFetch configuration
        private const long DefaultMaxBytesPerFile = 20 * 1024 * 1024; // 20MB

        private bool _useCloudFetch = true;
        private bool _canDecompressLz4 = true;
        private long _maxBytesPerFile = DefaultMaxBytesPerFile;
        private const bool DefaultRetryOnUnavailable= true;
        private const int DefaultTemporarilyUnavailableRetryTimeout = 500;

        // Default namespace
        private TNamespace? _defaultNamespace;

        public DatabricksConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
            ValidateProperties();
        }

        private void ValidateProperties()
        {
            if (Properties.TryGetValue(DatabricksParameters.ApplySSPWithQueries, out string? applySSPWithQueriesStr))
            {
                if (bool.TryParse(applySSPWithQueriesStr, out bool applySSPWithQueriesValue))
                {
                    _applySSPWithQueries = applySSPWithQueriesValue;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.ApplySSPWithQueries}' value '{applySSPWithQueriesStr}' could not be parsed. Valid values are 'true' and 'false'.");
                }
            }

            if (Properties.TryGetValue(DatabricksParameters.EnableDirectResults, out string? enableDirectResultsStr))
            {
                if (bool.TryParse(enableDirectResultsStr, out bool enableDirectResultsValue))
                {
                    _enableDirectResults = enableDirectResultsValue;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.EnableDirectResults}' value '{enableDirectResultsStr}' could not be parsed. Valid values are 'true' and 'false'.");
                }
            }

            // Parse CloudFetch options from connection properties
            if (Properties.TryGetValue(DatabricksParameters.UseCloudFetch, out string? useCloudFetchStr))
            {
                if (bool.TryParse(useCloudFetchStr, out bool useCloudFetchValue))
                {
                    _useCloudFetch = useCloudFetchValue;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.UseCloudFetch}' value '{useCloudFetchStr}' could not be parsed. Valid values are 'true' and 'false'.");
                }
            }

            if (Properties.TryGetValue(DatabricksParameters.CanDecompressLz4, out string? canDecompressLz4Str))
            {
                if (bool.TryParse(canDecompressLz4Str, out bool canDecompressLz4Value))
                {
                    _canDecompressLz4 = canDecompressLz4Value;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.CanDecompressLz4}' value '{canDecompressLz4Str}' could not be parsed. Valid values are 'true' and 'false'.");
                }
            }

            if (Properties.TryGetValue(DatabricksParameters.MaxBytesPerFile, out string? maxBytesPerFileStr))
            {
                if (!long.TryParse(maxBytesPerFileStr, out long maxBytesPerFileValue))
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.MaxBytesPerFile}' value '{maxBytesPerFileStr}' could not be parsed. Valid values are positive integers.");
                }

                if (maxBytesPerFileValue <= 0)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(Properties),
                        maxBytesPerFileValue,
                        $"Parameter '{DatabricksParameters.MaxBytesPerFile}' value must be a positive integer.");
                }
                _maxBytesPerFile = maxBytesPerFileValue;
            }

            // Parse default namespace
            string? defaultCatalog = null;
            string? defaultSchema = null;
            Properties.TryGetValue(DatabricksParameters.DefaultCatalog, out defaultCatalog);
            Properties.TryGetValue(DatabricksParameters.DefaultSchema, out defaultSchema);

            if (!string.IsNullOrEmpty(defaultCatalog))
            {
                _defaultNamespace = new TNamespace
                {
                    CatalogName = defaultCatalog,
                    SchemaName = defaultSchema
                };
            }
            else if (!string.IsNullOrEmpty(defaultSchema))
            {
                throw new ArgumentException($"Parameter '{DatabricksParameters.DefaultCatalog}' is not set but '{DatabricksParameters.DefaultSchema}' is set. Please provide a value for '{DatabricksParameters.DefaultCatalog}'.");
            }
        }

        /// <summary>
        /// Gets whether server side properties should be applied using queries.
        /// </summary>
        internal bool ApplySSPWithQueries => _applySSPWithQueries;

        /// <summary>
        /// Gets whether direct results are enabled.
        /// </summary>
        internal bool EnableDirectResults => _enableDirectResults;

        /// <summary>
        /// Gets whether CloudFetch is enabled.
        /// </summary>
        internal bool UseCloudFetch => _useCloudFetch;

        /// <summary>
        /// Gets whether LZ4 decompression is enabled.
        /// </summary>
        internal bool CanDecompressLz4 => _canDecompressLz4;

        /// <summary>
        /// Gets the maximum bytes per file for CloudFetch.
        /// </summary>
        internal long MaxBytesPerFile => _maxBytesPerFile;

        /// <summary>
        /// Gets the default namespace to use for SQL queries.
        /// </summary>
        internal TNamespace? DefaultNamespace => _defaultNamespace;

        /// <summary>
        /// Gets a value indicating whether to retry requests that receive a 503 response with a Retry-After header.
        /// </summary>
        protected bool TemporarilyUnavailableRetry { get; private set; } = DefaultRetryOnUnavailable;

        /// <summary>
        /// Gets the maximum total time in seconds to retry 503 responses before failing.
        /// </summary>
        protected int TemporarilyUnavailableRetryTimeout { get; private set; } = DefaultTemporarilyUnavailableRetryTimeout;

        protected override HttpMessageHandler CreateHttpHandler()
        {
            HttpMessageHandler baseHandler = base.CreateHttpHandler();
            if (TemporarilyUnavailableRetry)
            {
                // Add OAuth handler if OAuth authentication is being used
                baseHandler = new RetryHttpHandler(baseHandler, TemporarilyUnavailableRetryTimeout);
            }

            // Add OAuth handler if OAuth authentication is being used
            if (Properties.TryGetValue(SparkParameters.AuthType, out string? authType) &&
                SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue) &&
                authTypeValue == SparkAuthType.OAuth &&
                Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr) &&
                DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out DatabricksOAuthGrantType grantType) &&
                grantType == DatabricksOAuthGrantType.ClientCredentials)
            {
                // Note: We assume that properties have already been validated
                if (Properties.TryGetValue(SparkParameters.HostName, out string? host) && !string.IsNullOrEmpty(host))
                {
                    // Use hostname directly if provided
                }
                else if (Properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri))
                {
                    // Extract hostname from URI if URI is provided
                    if (Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri))
                    {
                        host = parsedUri.Host;
                    }
                }

                Properties.TryGetValue(DatabricksParameters.OAuthClientId, out string? clientId);
                Properties.TryGetValue(DatabricksParameters.OAuthClientSecret, out string? clientSecret);
                Properties.TryGetValue(DatabricksParameters.OAuthScope, out string? scope);

                HttpClient OauthHttpClient = new HttpClient(HiveServer2TlsImpl.NewHttpClientHandler(TlsOptions, _proxyConfigurator));

                var tokenProvider = new OAuthClientCredentialsProvider(
                    OauthHttpClient,
                    clientId!,
                    clientSecret!,
                    host!,
                    scope: scope ?? "sql",
                    timeoutMinutes: 1
                );

                return new OAuthDelegatingHandler(baseHandler, tokenProvider);
            }

            return baseHandler;
        }

        protected internal override bool AreResultsAvailableDirectly => _enableDirectResults;

        protected override void SetDirectResults(TGetColumnsReq request) => request.GetDirectResults = defaultGetDirectResults;

        protected override void SetDirectResults(TGetCatalogsReq request) => request.GetDirectResults = defaultGetDirectResults;

        protected override void SetDirectResults(TGetSchemasReq request) => request.GetDirectResults = defaultGetDirectResults;

        protected override void SetDirectResults(TGetTablesReq request) => request.GetDirectResults = defaultGetDirectResults;

        protected override void SetDirectResults(TGetTableTypesReq request) => request.GetDirectResults = defaultGetDirectResults;

        protected override void SetDirectResults(TGetPrimaryKeysReq request) => request.GetDirectResults = defaultGetDirectResults;

        protected override void SetDirectResults(TGetCrossReferenceReq request) => request.GetDirectResults = defaultGetDirectResults;

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, TGetResultSetMetadataResp? metadataResp = null)
        {
            // Get result format from metadata response if available
            TSparkRowSetType resultFormat = TSparkRowSetType.ARROW_BASED_SET;
            bool isLz4Compressed = false;

            DatabricksStatement? databricksStatement = statement as DatabricksStatement;

            if (databricksStatement == null)
            {
                throw new InvalidOperationException("Cannot obtain a reader for Databricks");
            }

            if (metadataResp != null)
            {
                if (metadataResp.__isset.resultFormat)
                {
                    resultFormat = metadataResp.ResultFormat;
                }

                if (metadataResp.__isset.lz4Compressed)
                {
                    isLz4Compressed = metadataResp.Lz4Compressed;
                }
            }

            // Choose the appropriate reader based on the result format
            if (resultFormat == TSparkRowSetType.URL_BASED_SET)
            {
                HttpClient cloudFetchHttpClient = new HttpClient(HiveServer2TlsImpl.NewHttpClientHandler(TlsOptions, _proxyConfigurator));
                return new CloudFetchReader(databricksStatement, schema, isLz4Compressed, cloudFetchHttpClient);
            }
            else
            {
                return new DatabricksReader(databricksStatement, schema, isLz4Compressed);
            }
        }

        internal override SchemaParser SchemaParser => new DatabricksSchemaParser();

        public override AdbcStatement CreateStatement()
        {
            DatabricksStatement statement = new DatabricksStatement(this);
            return statement;
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            var req = new TOpenSessionReq
            {
                Client_protocol = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                Client_protocol_i64 = (long)TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                CanUseMultipleCatalogs = true,
            };

            // Set default namespace if available
            if (_defaultNamespace != null)
            {
                req.InitialNamespace = _defaultNamespace;
            }

            // If not using queries to set server-side properties, include them in Configuration
            if (!_applySSPWithQueries)
            {
                req.Configuration = new Dictionary<string, string>();
                var serverSideProperties = GetServerSideProperties();
                foreach (var property in serverSideProperties)
                {
                    req.Configuration[property.Key] = property.Value;
                }
            }
            return req;
        }

        /// <summary>
        /// Gets a dictionary of server-side properties extracted from connection properties.
        /// </summary>
        /// <returns>Dictionary of server-side properties with prefix removed from keys.</returns>
        private Dictionary<string, string> GetServerSideProperties()
        {
            return Properties
                .Where(p => p.Key.StartsWith(DatabricksParameters.ServerSidePropertyPrefix))
                .ToDictionary(
                    p => p.Key.Substring(DatabricksParameters.ServerSidePropertyPrefix.Length),
                    p => p.Value
                );
        }

        /// <summary>
        /// Applies server-side properties by executing "set key=value" queries.
        /// </summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        public async Task ApplyServerSidePropertiesAsync()
        {
            if (!_applySSPWithQueries)
            {
                return;
            }

            var serverSideProperties = GetServerSideProperties();

            if (serverSideProperties.Count == 0)
            {
                return;
            }

            using var statement = new DatabricksStatement(this);

            foreach (var property in serverSideProperties)
            {
                if (!IsValidPropertyName(property.Key))
                {
                    Debug.WriteLine($"Skipping invalid property name: {property.Key}");
                    continue;
                }

                string escapedValue = EscapeSqlString(property.Value);
                string query = $"SET {property.Key}={escapedValue}";
                statement.SqlQuery = query;

                try
                {
                    await statement.ExecuteUpdateAsync();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Error setting server-side property '{property.Key}': {ex.Message}");
                }
            }
        }

        private bool IsValidPropertyName(string propertyName)
        {
            // Allow only letters and underscores in property names
            return System.Text.RegularExpressions.Regex.IsMatch(
                propertyName,
                @"^[a-zA-Z_]+$");
        }

        private string EscapeSqlString(string value)
        {
            return "`" + value.Replace("`", "``") + "`";
        }

        protected override void ValidateOptions()
        {
             base.ValidateOptions();

            if (Properties.TryGetValue(DatabricksParameters.TemporarilyUnavailableRetry, out string? tempUnavailableRetryStr))
            {
                if (!bool.TryParse(tempUnavailableRetryStr, out bool tempUnavailableRetryValue))
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.TemporarilyUnavailableRetry, tempUnavailableRetryStr,
                        $"must be a value of false (disabled) or true (enabled). Default is true.");
                }

                TemporarilyUnavailableRetry = tempUnavailableRetryValue;
            }


            if(Properties.TryGetValue(DatabricksParameters.TemporarilyUnavailableRetryTimeout, out string? tempUnavailableRetryTimeoutStr))
            {
                if (!int.TryParse(tempUnavailableRetryTimeoutStr, out int tempUnavailableRetryTimeoutValue) ||
                    tempUnavailableRetryTimeoutValue < 0)
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.TemporarilyUnavailableRetryTimeout, tempUnavailableRetryTimeoutStr,
                        $"must be a value of 0 (retry indefinitely) or a positive integer representing seconds. Default is 900 seconds (15 minutes).");
                }
                TemporarilyUnavailableRetryTimeout = tempUnavailableRetryTimeoutValue;
            }
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected internal override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetPrimaryKeysResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);

        protected override Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected internal override Task<TRowSet> GetRowSetAsync(TGetPrimaryKeysResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);

        protected override AuthenticationHeaderValue? GetAuthenticationHeaderValue(SparkAuthType authType)
        {
            if (authType == SparkAuthType.OAuth)
            {
                Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr);
                if (DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out DatabricksOAuthGrantType grantType) &&
                    grantType == DatabricksOAuthGrantType.ClientCredentials)
                {
                    // Return null for client credentials flow since OAuth handler will handle authentication
                    return null;
                }
            }
            return base.GetAuthenticationHeaderValue(authType);
        }

        protected override void ValidateOAuthParameters()
        {
            Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr);
            DatabricksOAuthGrantType grantType;

            if (!DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out grantType))
            {
                throw new ArgumentOutOfRangeException(
                    DatabricksParameters.OAuthGrantType,
                    grantTypeStr,
                    $"Unsupported {DatabricksParameters.OAuthGrantType} value. Refer to the Databricks documentation for valid values."
                );
            }

            // If we have a valid grant type, validate the required parameters
            if (grantType == DatabricksOAuthGrantType.ClientCredentials)
            {
                Properties.TryGetValue(DatabricksParameters.OAuthClientId, out string? clientId);
                Properties.TryGetValue(DatabricksParameters.OAuthClientSecret, out string? clientSecret);

                if (string.IsNullOrEmpty(clientId))
                {
                    throw new ArgumentException(
                        $"Parameter '{DatabricksParameters.OAuthGrantType}' is set to '{DatabricksConstants.OAuthGrantTypes.ClientCredentials}' but parameter '{DatabricksParameters.OAuthClientId}' is not set. Please provide a value for '{DatabricksParameters.OAuthClientId}'.",
                        nameof(Properties));
                }
                if (string.IsNullOrEmpty(clientSecret))
                {
                    throw new ArgumentException(
                        $"Parameter '{DatabricksParameters.OAuthGrantType}' is set to '{DatabricksConstants.OAuthGrantTypes.ClientCredentials}' but parameter '{DatabricksParameters.OAuthClientSecret}' is not set. Please provide a value for '{DatabricksParameters.OAuthClientSecret}'.",
                        nameof(Properties));
                }
            }
            else
            {
                // For other auth flows, use default OAuth validation
                base.ValidateOAuthParameters();
            }
        }
    }
}

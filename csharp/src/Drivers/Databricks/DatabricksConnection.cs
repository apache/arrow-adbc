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
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2.Client;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    internal class DatabricksConnection : SparkHttpConnection
    {
        internal static new readonly string s_assemblyName = ApacheUtility.GetAssemblyName(typeof(DatabricksConnection));
        internal static new readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(DatabricksConnection));

        private bool _applySSPWithQueries = false;
        private bool _enableDirectResults = true;
        private bool _enableMultipleCatalogSupport = true;
        private bool _enablePKFK = true;
        private bool _runAsyncInThrift = true;

        internal static TSparkGetDirectResults defaultGetDirectResults = new()
        {
            MaxRows = 2000000,
            MaxBytes = 404857600
        };

        // CloudFetch configuration
        private const long DefaultMaxBytesPerFile = 20 * 1024 * 1024; // 20MB
        private const int DefaultQueryTimeSeconds = 3 * 60 * 60; // 3 hours
        private bool _useCloudFetch = true;
        private bool _canDecompressLz4 = true;
        private long _maxBytesPerFile = DefaultMaxBytesPerFile;
        private const bool DefaultRetryOnUnavailable = true;
        private const int DefaultTemporarilyUnavailableRetryTimeout = 900;
        private bool _useDescTableExtended = true;

        // Trace propagation configuration
        private bool _tracePropagationEnabled = true;
        private string _traceParentHeaderName = "traceparent";
        private bool _traceStateEnabled = false;

        // Identity federation client ID for token exchange
        private string? _identityFederationClientId;

        // Default namespace
        private TNamespace? _defaultNamespace;

        private HttpClient? _authHttpClient;

        public DatabricksConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
            ValidateProperties();
        }

        public override IEnumerable<KeyValuePair<string, object?>>? GetActivitySourceTags(IReadOnlyDictionary<string, string> properties)
        {
            IEnumerable<KeyValuePair<string, object?>>? tags = base.GetActivitySourceTags(properties);
            // TODO: Add any additional tags specific to Databricks connection
            //tags ??= [];
            //tags.Concat([new("key", "value")]);
            return tags;
        }

        protected override TCLIService.IAsync CreateTCLIServiceClient(TProtocol protocol)
        {
            return new ThreadSafeClient(new TCLIService.Client(protocol));
        }

        private void ValidateProperties()
        {
            if (Properties.TryGetValue(DatabricksParameters.EnablePKFK, out string? enablePKFKStr))
            {
                if (bool.TryParse(enablePKFKStr, out bool enablePKFKValue))
                {
                    _enablePKFK = enablePKFKValue;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.EnablePKFK}' value '{enablePKFKStr}' could not be parsed. Valid values are 'true', 'false'.");
                }
            }

            if (Properties.TryGetValue(DatabricksParameters.EnableMultipleCatalogSupport, out string? enableMultipleCatalogSupportStr))
            {
                if (bool.TryParse(enableMultipleCatalogSupportStr, out bool enableMultipleCatalogSupportValue))
                {
                    _enableMultipleCatalogSupport = enableMultipleCatalogSupportValue;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.EnableMultipleCatalogSupport}' value '{enableMultipleCatalogSupportStr}' could not be parsed. Valid values are 'true', 'false'.");
                }
            }

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

            if (Properties.TryGetValue(DatabricksParameters.UseDescTableExtended, out string? useDescTableExtendedStr))
            {
                if (bool.TryParse(useDescTableExtendedStr, out bool useDescTableExtended))
                {
                    _useDescTableExtended = useDescTableExtended;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.UseDescTableExtended}' value '{useDescTableExtendedStr}' could not be parsed. Valid values are 'true' and 'false'.");
                }
            }

            if (Properties.TryGetValue(DatabricksParameters.EnableRunAsyncInThriftOp, out string? enableRunAsyncInThriftStr))
            {
                if (bool.TryParse(enableRunAsyncInThriftStr, out bool enableRunAsyncInThrift))
                {
                    _runAsyncInThrift = enableRunAsyncInThrift;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.EnableRunAsyncInThriftOp}' value '{enableRunAsyncInThriftStr}' could not be parsed. Valid values are 'true' and 'false'.");
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
            // only if enableMultipleCatalogSupport is true, do we supply catalog from connection properties
            if (_enableMultipleCatalogSupport)
            {
                Properties.TryGetValue(AdbcOptions.Connection.CurrentCatalog, out defaultCatalog);
            }
            Properties.TryGetValue(AdbcOptions.Connection.CurrentDbSchema, out defaultSchema);

            // This maintains backward compatibility with older workspaces, where the Hive metastore was accessed via the spark catalog name.
            // In newer DBR versions with Unity Catalog, the default catalog is typically hive_metastore.
            // Passing null here allows the runtime to fall back to the workspace-defined default catalog for the session.
            defaultCatalog = HandleSparkCatalog(defaultCatalog);

            if (!string.IsNullOrWhiteSpace(defaultCatalog) || !string.IsNullOrWhiteSpace(defaultSchema))
            {
                var ns = new TNamespace();
                if (!string.IsNullOrWhiteSpace(defaultCatalog))
                    ns.CatalogName = defaultCatalog!;
                if (!string.IsNullOrWhiteSpace(defaultSchema))
                    ns.SchemaName = defaultSchema;
                _defaultNamespace = ns;
            }

            // Parse trace propagation options
            if (Properties.TryGetValue(DatabricksParameters.TracePropagationEnabled, out string? tracePropagationEnabledStr))
            {
                if (bool.TryParse(tracePropagationEnabledStr, out bool tracePropagationEnabled))
                {
                    _tracePropagationEnabled = tracePropagationEnabled;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.TracePropagationEnabled}' value '{tracePropagationEnabledStr}' could not be parsed. Valid values are 'true' and 'false'.");
                }
            }

            if (Properties.TryGetValue(DatabricksParameters.TraceParentHeaderName, out string? traceParentHeaderName))
            {
                if (!string.IsNullOrWhiteSpace(traceParentHeaderName))
                {
                    _traceParentHeaderName = traceParentHeaderName;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.TraceParentHeaderName}' cannot be empty.");
                }
            }

            if (Properties.TryGetValue(DatabricksParameters.TraceStateEnabled, out string? traceStateEnabledStr))
            {
                if (bool.TryParse(traceStateEnabledStr, out bool traceStateEnabled))
                {
                    _traceStateEnabled = traceStateEnabled;
                }
                else
                {
                    throw new ArgumentException($"Parameter '{DatabricksParameters.TraceStateEnabled}' value '{traceStateEnabledStr}' could not be parsed. Valid values are 'true' and 'false'.");
                }
            }

            if (!Properties.ContainsKey(ApacheParameters.QueryTimeoutSeconds))
            {
                // Default QueryTimeSeconds in Hive2Connection is only 60s, which is too small for lots of long running query
                QueryTimeoutSeconds = DefaultQueryTimeSeconds;
            }

            if (Properties.TryGetValue(DatabricksParameters.IdentityFederationClientId, out string? identityFederationClientId))
            {
                _identityFederationClientId = identityFederationClientId;
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
        /// Gets whether multiple catalog is supported
        /// </summary>
        internal bool EnableMultipleCatalogSupport => _enableMultipleCatalogSupport;

        /// <summary>
        /// Check if current connection can use `DESC TABLE EXTENDED` query
        /// </summary>
        internal bool CanUseDescTableExtended => _useDescTableExtended && ServerProtocolVersion != null && FeatureVersionNegotiator.SupportsDESCTableExtended(ServerProtocolVersion.Value);

        /// <summary>
        /// Gets whether PK/FK metadata call is enabled
        /// </summary>
        public bool EnablePKFK => _enablePKFK;

        /// <summary>
        /// Enable RunAsync flag in Thrift Operation
        /// </summary>
        public bool RunAsyncInThrift => _runAsyncInThrift;

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
            HttpMessageHandler baseAuthHandler = HiveServer2TlsImpl.NewHttpClientHandler(TlsOptions, _proxyConfigurator);

            // Add tracing handler to propagate W3C trace context if enabled
            if (_tracePropagationEnabled)
            {
                baseHandler = new TracingDelegatingHandler(baseHandler, this, _traceParentHeaderName, _traceStateEnabled);
                baseAuthHandler = new TracingDelegatingHandler(baseAuthHandler, this, _traceParentHeaderName, _traceStateEnabled);
            }

            if (TemporarilyUnavailableRetry)
            {
                // Add retry handler for 503 responses
                baseHandler = new RetryHttpHandler(baseHandler, TemporarilyUnavailableRetryTimeout);
                baseAuthHandler = new RetryHttpHandler(baseAuthHandler, TemporarilyUnavailableRetryTimeout);
            }

            if (Properties.TryGetValue(SparkParameters.AuthType, out string? authType) &&
                SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue) &&
                authTypeValue == SparkAuthType.OAuth)
            {
                Debug.Assert(_authHttpClient == null, "Auth HttpClient should not be initialized yet.");
                _authHttpClient = new HttpClient(baseAuthHandler);

                string host = GetHost();
                ITokenExchangeClient tokenExchangeClient = new TokenExchangeClient(_authHttpClient, host);

                // Mandatory token exchange should be the inner handler so that it happens
                // AFTER the OAuth handlers (e.g. after M2M sets the access token)
                baseHandler = new MandatoryTokenExchangeDelegatingHandler(
                    baseHandler,
                    tokenExchangeClient,
                    _identityFederationClientId);

                // Add OAuth client credentials handler if OAuth M2M authentication is being used
                if (Properties.TryGetValue(DatabricksParameters.OAuthGrantType, out string? grantTypeStr) &&
                    DatabricksOAuthGrantTypeParser.TryParse(grantTypeStr, out DatabricksOAuthGrantType grantType) &&
                    grantType == DatabricksOAuthGrantType.ClientCredentials)
                {
                    Properties.TryGetValue(DatabricksParameters.OAuthClientId, out string? clientId);
                    Properties.TryGetValue(DatabricksParameters.OAuthClientSecret, out string? clientSecret);
                    Properties.TryGetValue(DatabricksParameters.OAuthScope, out string? scope);

                    var tokenProvider = new OAuthClientCredentialsProvider(
                        _authHttpClient,
                        clientId!,
                        clientSecret!,
                        host!,
                        scope: scope ?? "sql",
                        timeoutMinutes: 1
                    );

                    baseHandler = new OAuthDelegatingHandler(baseHandler, tokenProvider);
                }
                // Add token renewal handler for OAuth access token
                else if (Properties.TryGetValue(DatabricksParameters.TokenRenewLimit, out string? tokenRenewLimitStr) &&
                    int.TryParse(tokenRenewLimitStr, out int tokenRenewLimit) &&
                    tokenRenewLimit > 0 &&
                    Properties.TryGetValue(SparkParameters.AccessToken, out string? accessToken))
                {
                    if (string.IsNullOrEmpty(accessToken))
                    {
                        throw new ArgumentException("Access token is required for OAuth authentication with token renewal.");
                    }

                    // Check if token is a JWT token by trying to decode it
                    if (JwtTokenDecoder.TryGetExpirationTime(accessToken, out DateTime expiryTime))
                    {
                        baseHandler = new TokenRefreshDelegatingHandler(
                            baseHandler,
                            tokenExchangeClient,
                            accessToken,
                            expiryTime,
                            tokenRenewLimit);
                    }
                }
            }

            return baseHandler;
        }

        protected override bool GetObjectsPatternsRequireLowerCase => true;

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, IResponse response, TGetResultSetMetadataResp? metadataResp = null)
        {
            bool isLz4Compressed = false;

            DatabricksStatement? databricksStatement = statement as DatabricksStatement;

            if (databricksStatement == null)
            {
                throw new InvalidOperationException("Cannot obtain a reader for Databricks");
            }

            if (metadataResp != null && metadataResp.__isset.lz4Compressed)
            {
                isLz4Compressed = metadataResp.Lz4Compressed;
            }

            return new DatabricksCompositeReader(databricksStatement, schema, response, isLz4Compressed, TlsOptions, _proxyConfigurator);
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
                CanUseMultipleCatalogs = _enableMultipleCatalogSupport,
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

        protected override async Task HandleOpenSessionResponse(TOpenSessionResp? session, ActivityWithPii? activity = default)
        {
            await base.HandleOpenSessionResponse(session, activity);
            if (session != null)
            {
                var version = session.ServerProtocolVersion;
                if (!FeatureVersionNegotiator.IsDatabricksProtocolVersion(version))
                {
                    throw new DatabricksException("Attempted to use databricks driver with a non-databricks server");
                }
                _enablePKFK = _enablePKFK && FeatureVersionNegotiator.SupportsPKFK(version);
                _enableMultipleCatalogSupport = session.__isset.canUseMultipleCatalogs ? session.CanUseMultipleCatalogs : false;
                if (session.__isset.initialNamespace)
                {
                    _defaultNamespace = session.InitialNamespace;
                }
                else if (_defaultNamespace != null && !string.IsNullOrEmpty(_defaultNamespace.SchemaName))
                {
                    // catalog in namespace is introduced when SET CATALOG is introduced, so we don't need to fallback
                    // server version is too old. Explicitly set the schema using queries
                    await SetSchema(_defaultNamespace.SchemaName);
                }
            }
        }

        // Since Databricks Namespace was introduced in newer versions, we fallback to USE SCHEMA to set default schema
        // in case the server version is too low.
        private async Task SetSchema(string schemaName)
        {
            using var statement = new DatabricksStatement(this);
            statement.SqlQuery = $"USE {schemaName}";
            await statement.ExecuteUpdateAsync();
        }

        /// <summary>
        /// Gets a dictionary of server-side properties extracted from connection properties.
        /// </summary>
        /// <returns>Dictionary of server-side properties with prefix removed from keys.</returns>
        private Dictionary<string, string> GetServerSideProperties()
        {
            return Properties
                .Where(p => p.Key.ToLowerInvariant().StartsWith(DatabricksParameters.ServerSidePropertyPrefix))
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


            if (Properties.TryGetValue(DatabricksParameters.TemporarilyUnavailableRetryTimeout, out string? tempUnavailableRetryTimeoutStr))
            {
                if (!int.TryParse(tempUnavailableRetryTimeoutStr, out int tempUnavailableRetryTimeoutValue) ||
                    tempUnavailableRetryTimeoutValue < 0)
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.TemporarilyUnavailableRetryTimeout, tempUnavailableRetryTimeoutStr,
                        $"must be a value of 0 (retry indefinitely) or a positive integer representing seconds. Default is 900 seconds (15 minutes).");
                }
                TemporarilyUnavailableRetryTimeout = tempUnavailableRetryTimeoutValue;
            }

            // When TemporarilyUnavailableRetry is enabled, we need to make sure connection timeout (which is used to cancel the HttpConnection) is equal
            // or greater than TemporarilyUnavailableRetryTimeout so that it won't timeout before server startup timeout (TemporarilyUnavailableRetryTimeout)
            if (TemporarilyUnavailableRetry && TemporarilyUnavailableRetryTimeout * 1000 > ConnectTimeoutMilliseconds)
            {
                ConnectTimeoutMilliseconds = TemporarilyUnavailableRetryTimeout * 1000;
            }
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(IResponse response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults!.ResultSetMetadata);

        protected override Task<TRowSet> GetRowSetAsync(IResponse response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults!.ResultSet.Results);

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

        /// <summary>
        /// Gets the host from the connection properties.
        /// </summary>
        /// <returns>The host, or empty string if not found.</returns>
        private string GetHost()
        {
            if (Properties.TryGetValue(SparkParameters.HostName, out string? host) && !string.IsNullOrEmpty(host))
            {
                return host;
            }

            if (Properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrEmpty(uri))
            {
                // Parse the URI to extract the host
                if (Uri.TryCreate(uri, UriKind.Absolute, out Uri? parsedUri))
                {
                    return parsedUri.Host;
                }
            }

            throw new ArgumentException("Host not found in connection properties. Please provide a valid host using either 'HostName' or 'Uri' property.");
        }

        public override string AssemblyName => s_assemblyName;

        public override string AssemblyVersion => s_assemblyVersion;

        internal static string? HandleSparkCatalog(string? CatalogName)
        {
            if (CatalogName != null && CatalogName.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }
            return CatalogName;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _authHttpClient?.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}

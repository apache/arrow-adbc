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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Reader
{
    /// <summary>
    /// A composite reader for Databricks that delegates to either CloudFetchReader or DatabricksReader
    /// based on CloudFetch configuration and result set characteristics.
    /// </summary>
    internal sealed class DatabricksCompositeReader : TracingReader
    {
        public override string AssemblyName => DatabricksConnection.s_assemblyName;

        public override string AssemblyVersion => DatabricksConnection.s_assemblyVersion;

        public override Schema Schema { get { return _schema; } }

        private BaseDatabricksReader? _activeReader;
        private readonly IHiveServer2Statement _statement;
        private readonly Schema _schema;
        private readonly IResponse _response;
        private readonly bool _isLz4Compressed;
        private readonly TlsProperties _tlsOptions;
        private readonly HiveServer2ProxyConfigurator _proxyConfigurator;

        private IOperationStatusPoller? operationStatusPoller;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabricksCompositeReader"/> class.
        /// </summary>
        /// <param name="statement">The Databricks statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        /// <param name="httpClient">The HTTP client for CloudFetch operations.</param>
        internal DatabricksCompositeReader(
            IHiveServer2Statement statement,
            Schema schema,
            IResponse response,
            bool isLz4Compressed,
            TlsProperties tlsOptions,
            HiveServer2ProxyConfigurator proxyConfigurator)
            : base(statement)
        {
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _response = response;
            _isLz4Compressed = isLz4Compressed;
            _tlsOptions = tlsOptions;
            _proxyConfigurator = proxyConfigurator;

            // use direct results if available
            if (_statement.TryGetDirectResults(_response, out TSparkDirectResults? directResults)
                && directResults!.__isset.resultSet
                && directResults.ResultSet != null)
            {
                _activeReader = DetermineReader(directResults.ResultSet);
            }
            if (_response.DirectResults?.ResultSet?.HasMoreRows ?? true)
            {
                operationStatusPoller = new DatabricksOperationStatusPoller(statement, _response);
                operationStatusPoller.Start();
            }
        }

        private BaseDatabricksReader DetermineReader(TFetchResultsResp initialResults)
        {
            // if it has links, use cloud fetch
            if (initialResults.__isset.results &&
                initialResults.Results.__isset.resultLinks &&
                initialResults.Results.ResultLinks?.Count > 0)
            {
                HttpClient cloudFetchHttpClient = new HttpClient(HiveServer2TlsImpl.NewHttpClientHandler(_tlsOptions, _proxyConfigurator));
                return new CloudFetchReader(_statement, _schema, _response, initialResults, _isLz4Compressed, cloudFetchHttpClient);
            }
            else
            {
                return new DatabricksReader(_statement, _schema, _response, initialResults, _isLz4Compressed);
            }
        }

        /// <summary>
        /// Reads the next record batch from the active reader.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next record batch, or null if there are no more batches.</returns>
        private async ValueTask<RecordBatch?> ReadNextRecordBatchInternalAsync(CancellationToken cancellationToken = default)
        {
            // Initialize the active reader if not already done
            if (_activeReader == null)
            {
                // if no reader, we did not have direct results
                // Make a FetchResults call to get the initial result set
                // and determine the reader based on the result set
                TFetchResultsReq request = new TFetchResultsReq(_response.OperationHandle!, TFetchOrientation.FETCH_NEXT, this._statement.BatchSize);
                TFetchResultsResp response = await this._statement.Connection.Client!.FetchResults(request, cancellationToken);
                _activeReader = DetermineReader(response);
            }

            return await _activeReader.ReadNextRecordBatchAsync(cancellationToken);
        }

        public override async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            var result = await ReadNextRecordBatchInternalAsync(cancellationToken);
            // Stop the poller when we've reached the end of results
            if (result == null)
            {
                StopOperationStatusPoller();
            }
            return result;
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (!_disposed)
                {
                    if (disposing)
                    {
                        _activeReader?.Dispose();
                        StopOperationStatusPoller();
                        _activeReader = null;

                        TCloseOperationResp resp = HiveServer2Reader.CloseOperation(_statement, _response).Result;
                        HiveServer2Connection.HandleThriftResponse(resp.Status, activity: null);
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
                _disposed = true;
            }
        }

        private void StopOperationStatusPoller()
        {
            operationStatusPoller?.Stop();
            operationStatusPoller?.Dispose();
            operationStatusPoller = null;
        }
    }
}

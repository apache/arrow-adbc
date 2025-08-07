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
using Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch;
using Apache.Arrow.Adbc.Tracing;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// A composite reader for Databricks that delegates to either CloudFetchReader or DatabricksReader
    /// based on CloudFetch configuration and result set characteristics.
    /// </summary>
    internal class DatabricksCompositeReader : TracingReader
    {
        public override string AssemblyName => DatabricksConnection.s_assemblyName;

        public override string AssemblyVersion => DatabricksConnection.s_assemblyVersion;

        public override Schema Schema { get { return _schema; } }

        private BaseDatabricksReader? _activeReader;
        private readonly IHiveServer2Statement _statement;
        private readonly Schema _schema;
        private readonly bool _isLz4Compressed;
        private readonly HttpClient _httpClient;

        private IOperationStatusPoller? operationStatusPoller;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabricksCompositeReader"/> class.
        /// </summary>
        /// <param name="statement">The Databricks statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        /// <param name="httpClient">The HTTP client for CloudFetch operations.</param>
        /// <param name="operationPoller">Optional operation status poller for testing.</param>
        internal DatabricksCompositeReader(IHiveServer2Statement statement, Schema schema, bool isLz4Compressed, HttpClient httpClient, IOperationStatusPoller? operationPoller = null): base(statement)
        {
            _statement = statement;
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _isLz4Compressed = isLz4Compressed;
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

            // use direct results if available
            if (_statement.HasDirectResults && _statement.DirectResults != null && _statement.DirectResults.__isset.resultSet && _statement.DirectResults?.ResultSet != null)
            {
                _activeReader = DetermineReader(_statement.DirectResults.ResultSet);
                if (!_statement.DirectResults.ResultSet.HasMoreRows)
                {
                    return;
                }
            }
            
            // Use injected poller for testing, or create a real one for production
            operationStatusPoller = operationPoller ?? new DatabricksOperationStatusPoller(_statement);
            operationStatusPoller.Start();
        }

        /// <summary>
        /// Determines which reader to use based on the fetch results.
        /// </summary>
        /// <param name="initialResults">The initial fetch results.</param>
        /// <returns>The appropriate reader for the results.</returns>
        protected virtual BaseDatabricksReader DetermineReader(TFetchResultsResp initialResults)
        {
            // if it has links, use cloud fetch
            if (initialResults.__isset.results &&
                initialResults.Results.__isset.resultLinks &&
                initialResults.Results.ResultLinks?.Count > 0)
            {
                return CreateCloudFetchReader(initialResults);
            }
            else
            {
                return CreateDatabricksReader(initialResults);
            }
        }

        /// <summary>
        /// Creates a CloudFetchReader instance. Virtual to allow testing.
        /// </summary>
        /// <param name="initialResults">The initial fetch results.</param>
        /// <returns>A new CloudFetchReader instance.</returns>
        protected virtual BaseDatabricksReader CreateCloudFetchReader(TFetchResultsResp initialResults)
        {
            return new CloudFetchReader(_statement, _schema, initialResults, _isLz4Compressed, _httpClient);
        }

        /// <summary>
        /// Creates a DatabricksReader instance. Virtual to allow testing.
        /// </summary>
        /// <param name="initialResults">The initial fetch results.</param>
        /// <returns>A new DatabricksReader instance.</returns>
        protected virtual BaseDatabricksReader CreateDatabricksReader(TFetchResultsResp initialResults)
        {
            return new DatabricksReader(_statement, _schema, initialResults, _isLz4Compressed);
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
                TFetchResultsReq request = new TFetchResultsReq(this._statement.OperationHandle!, TFetchOrientation.FETCH_NEXT, this._statement.BatchSize);
                TFetchResultsResp response = await this._statement.Client!.FetchResults(request, cancellationToken);
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
            if (disposing)
            {
                _activeReader?.Dispose();
                DisposeOperationStatusPoller();
            }
            _activeReader = null;
            base.Dispose(disposing);
        }

        private void DisposeOperationStatusPoller()
        {
            if (operationStatusPoller != null)
            {
                StopOperationStatusPoller();
                operationStatusPoller.Dispose();
                operationStatusPoller = null;
            }
        }

        private void StopOperationStatusPoller()
        {
            operationStatusPoller?.Stop();
        }
    }
}

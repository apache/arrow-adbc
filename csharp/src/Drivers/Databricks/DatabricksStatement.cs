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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Apache.Databricks.CloudFetch;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Databricks-specific implementation of <see cref="AdbcStatement"/>
    /// </summary>
    internal class DatabricksStatement : SparkStatement, IHiveServer2Statement, IDisposable
    {
        private bool useCloudFetch;
        private bool canDecompressLz4;
        private long maxBytesPerFile;

        // Semaphore lock to ensure that polling and fetching results do not both use transport at the same time
        private readonly SemaphoreSlim _clientSemaphore = new SemaphoreSlim(1, 1);

        public DatabricksStatement(DatabricksConnection connection)
            : base(connection)
        {
            // Inherit CloudFetch settings from connection
            useCloudFetch = connection.UseCloudFetch;
            canDecompressLz4 = connection.CanDecompressLz4;
            maxBytesPerFile = connection.MaxBytesPerFile;
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            base.SetStatementProperties(statement);

            // Set CloudFetch capabilities
            statement.CanDownloadResult = useCloudFetch;
            statement.CanDecompressLZ4Result = canDecompressLz4;
            statement.MaxBytesPerFile = maxBytesPerFile;

            if (Connection.AreResultsAvailableDirectly)
            {
                statement.GetDirectResults = DatabricksConnection.defaultGetDirectResults;
            }
        }

        /// <summary>
        /// Checks if direct results are available.
        /// </summary>
        /// <returns>True if direct results are available and contain result data, false otherwise.</returns>
        public bool HasDirectResults => DirectResults?.ResultSet != null && DirectResults?.ResultSetMetadata != null;

        public TSparkDirectResults? DirectResults
        {
            get { return _directResults; }
        }

        // Cast the Client to IAsync for CloudFetch compatibility
        TCLIService.IAsync IHiveServer2Statement.Client => Connection.Client;

        /// <summary>
        /// Executes a client operation in a thread-safe manner.
        /// </summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the result.</returns>
        public async Task<TResult> ExecuteClientOperationAsync<TResult>(
            Func<TCLIService.IAsync, CancellationToken, Task<TResult>> operation,
            CancellationToken cancellationToken)
        {
            await _clientSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await operation(Connection.Client, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _clientSemaphore.Release();
            }
        }

        /// <summary>
        /// Gets the operation status in a thread-safe manner.
        /// </summary>
        /// <param name="request">The get operation status request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the result.</returns>
        public Task<TGetOperationStatusResp> GetOperationStatusAsync(
            TGetOperationStatusReq request,
            CancellationToken cancellationToken)
        {
            return ExecuteClientOperationAsync(
                (client, token) => client.GetOperationStatus(request, token),
                cancellationToken);
        }

        /// <summary>
        /// Fetches results in a thread-safe manner.
        /// </summary>
        /// <param name="request">The fetch results request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the result.</returns>
        public Task<TFetchResultsResp> FetchResultsAsync(
            TFetchResultsReq request,
            CancellationToken cancellationToken)
        {
            return ExecuteClientOperationAsync(
                (client, token) => client.FetchResults(request, token),
                cancellationToken);
        }

        public override void SetOption(string key, string value)
        {
            switch (key)
            {
                case DatabricksParameters.UseCloudFetch:
                    if (bool.TryParse(value, out bool useCloudFetchValue))
                    {
                        this.useCloudFetch = useCloudFetchValue;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a boolean value.");
                    }
                    break;
                case DatabricksParameters.CanDecompressLz4:
                    if (bool.TryParse(value, out bool canDecompressLz4Value))
                    {
                        this.canDecompressLz4 = canDecompressLz4Value;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a boolean value.");
                    }
                    break;
                case DatabricksParameters.MaxBytesPerFile:
                    if (long.TryParse(value, out long maxBytesPerFileValue))
                    {
                        this.maxBytesPerFile = maxBytesPerFileValue;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a long value.");
                    }
                    break;
                default:
                    base.SetOption(key, value);
                    break;
            }
        }

        /// <summary>
        /// Sets whether to use CloudFetch for retrieving results.
        /// </summary>
        /// <param name="useCloudFetch">Whether to use CloudFetch.</param>
        internal void SetUseCloudFetch(bool useCloudFetch)
        {
            this.useCloudFetch = useCloudFetch;
        }

        /// <summary>
        /// Gets whether CloudFetch is enabled.
        /// </summary>
        public bool UseCloudFetch => useCloudFetch;

        /// <summary>
        /// Gets the maximum bytes per file for CloudFetch.
        /// </summary>
        public long MaxBytesPerFile => maxBytesPerFile;

        /// <summary>
        /// Gets whether LZ4 decompression is enabled.
        /// </summary>
        public bool CanDecompressLz4 => canDecompressLz4;

        /// <summary>
        /// Sets whether the client can decompress LZ4 compressed results.
        /// </summary>
        /// <param name="canDecompressLz4">Whether the client can decompress LZ4.</param>
        internal void SetCanDecompressLz4(bool canDecompressLz4)
        {
            this.canDecompressLz4 = canDecompressLz4;
        }

        /// <summary>
        /// Sets the maximum bytes per file for CloudFetch.
        /// </summary>
        /// <param name="maxBytesPerFile">The maximum bytes per file.</param>
        internal void SetMaxBytesPerFile(long maxBytesPerFile)
        {
            this.maxBytesPerFile = maxBytesPerFile;
        }

        /// <summary>
        /// Disposes the resources used by the statement.
        /// </summary>
        public override void Dispose()
        {
            _clientSemaphore.Dispose();
            base.Dispose();
        }
    }
}

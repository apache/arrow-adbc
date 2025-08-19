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
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2.Client
{
    /// <summary>
    /// A thread-safe wrapper for TCLIService.IAsync client that ensures all operations
    /// are properly synchronized to prevent concurrent access issues.
    /// </summary>
    internal class ThreadSafeClient : IDisposable, TCLIService.IAsync
    {
        private readonly TCLIService.IAsync _client;
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThreadSafeClient"/> class.
        /// </summary>
        /// <param name="client">The TCLIService client to wrap.</param>
        internal ThreadSafeClient(TCLIService.IAsync client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        /// <summary>
        /// Executes a client operation in a thread-safe manner.
        /// </summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the result.</returns>
        private async Task<TResult> ExecuteOperationAsync<TResult>(
            Func<TCLIService.IAsync, CancellationToken, Task<TResult>> operation,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await operation(_client, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        #region TCLIService.IAsync Implementation

        /// <inheritdoc/>
        public Task<TOpenSessionResp> OpenSession(TOpenSessionReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.OpenSession(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TCloseSessionResp> CloseSession(TCloseSessionReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.CloseSession(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetInfoResp> GetInfo(TGetInfoReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetInfo(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TExecuteStatementResp> ExecuteStatement(TExecuteStatementReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.ExecuteStatement(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetTypeInfoResp> GetTypeInfo(TGetTypeInfoReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetTypeInfo(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetCatalogsResp> GetCatalogs(TGetCatalogsReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetCatalogs(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetSchemasResp> GetSchemas(TGetSchemasReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetSchemas(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetTablesResp> GetTables(TGetTablesReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetTables(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetTableTypesResp> GetTableTypes(TGetTableTypesReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetTableTypes(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetColumnsResp> GetColumns(TGetColumnsReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetColumns(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetFunctionsResp> GetFunctions(TGetFunctionsReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetFunctions(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetPrimaryKeysResp> GetPrimaryKeys(TGetPrimaryKeysReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetPrimaryKeys(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetCrossReferenceResp> GetCrossReference(TGetCrossReferenceReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetCrossReference(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetOperationStatusResp> GetOperationStatus(TGetOperationStatusReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetOperationStatus(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TCancelOperationResp> CancelOperation(TCancelOperationReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.CancelOperation(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TCloseOperationResp> CloseOperation(TCloseOperationReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.CloseOperation(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetResultSetMetadataResp> GetResultSetMetadata(TGetResultSetMetadataReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetResultSetMetadata(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TFetchResultsResp> FetchResults(TFetchResultsReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.FetchResults(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetDelegationTokenResp> GetDelegationToken(TGetDelegationTokenReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetDelegationToken(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TCancelDelegationTokenResp> CancelDelegationToken(TCancelDelegationTokenReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.CancelDelegationToken(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TRenewDelegationTokenResp> RenewDelegationToken(TRenewDelegationTokenReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.RenewDelegationToken(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TGetQueryIdResp> GetQueryId(TGetQueryIdReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetQueryId(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TSetClientInfoResp> SetClientInfo(TSetClientInfoReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.SetClientInfo(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TUploadDataResp> UploadData(TUploadDataReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.UploadData(req, token), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<TDownloadDataResp> DownloadData(TDownloadDataReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.DownloadData(req, token), cancellationToken);
        }

        #endregion

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ThreadSafeClient));
            }
        }

        /// <summary>
        /// Disposes the resources used by the client.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _semaphore.Dispose();
                _disposed = true;
            }
        }
    }
}

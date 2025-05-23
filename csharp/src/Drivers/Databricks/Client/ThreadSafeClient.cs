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

namespace Apache.Arrow.Adbc.Drivers.Apache.Databricks.Client
{
    /// <summary>
    /// A thread-safe wrapper for TCLIService.IAsync client that ensures all operations
    /// are properly synchronized to prevent concurrent access issues.
    /// </summary>
    public class ThreadSafeClient : IDisposable
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

        #region Client Operations

        /// <summary>
        /// Gets the operation status in a thread-safe manner.
        /// </summary>
        /// <param name="req">The get operation status request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the result.</returns>
        internal virtual Task<TGetOperationStatusResp> GetOperationStatusAsync(TGetOperationStatusReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.GetOperationStatus(req, token), cancellationToken);
        }

        /// <summary>
        /// Fetches results in a thread-safe manner.
        /// </summary>
        /// <param name="req">The fetch results request.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation with the result.</returns>
        internal virtual Task<TFetchResultsResp> FetchResultsAsync(TFetchResultsReq req, CancellationToken cancellationToken = default)
        {
            return ExecuteOperationAsync((client, token) => client.FetchResults(req, token), cancellationToken);
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

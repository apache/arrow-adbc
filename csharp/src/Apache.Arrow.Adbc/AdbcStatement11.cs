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
using System.Collections;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Statements may represent queries or prepared statements. Statements
    /// may be used multiple times and can be reconfigured (e.g. they can
    /// be reused to execute multiple different queries).
    /// </summary>
    public abstract class AdbcStatement11 : IDisposable
#if NET5_0_OR_GREATER
        , IAsyncDisposable
#endif
    {
        /// <summary>
        /// Gets or sets a SQL query to be executed on this statement.
        /// </summary>
        public virtual string? SqlQuery { get; set; }

        /// <summary>
        /// Gets or sets the Substrait plan.
        /// </summary>
        public virtual byte[]? SubstraitPlan
        {
            get { throw AdbcException.NotImplemented("Statement does not support SubstraitPlan"); }
            set { throw AdbcException.NotImplemented("Statement does not support SubstraitPlan"); }
        }

        /// <summary>
        /// Binds this statement to a <see cref="RecordBatch"/> to provide parameter values or bulk data ingestion.
        /// </summary>
        /// <param name="batch">the RecordBatch to bind</param>
        /// <param name="schema">the schema of the RecordBatch</param>
        public virtual void Bind(RecordBatch batch, Schema schema)
        {
            throw AdbcException.NotImplemented("Statement does not support Bind");
        }

        /// <summary>
        /// Binds this statement to an <see cref="IArrowArrayStream"/> to provide parameter values or bulk data ingestion.
        /// </summary>
        /// <param name="stream"></param>
        public virtual void BindStream(IArrowArrayStream stream)
        {
            throw AdbcException.NotImplemented("Statement does not support BindStream");
        }

        /// <summary>
        /// Executes the statement and returns a structure containing the number
        /// of records and the <see cref="IArrowArrayStream"/>.
        /// </summary>
        /// <returns>A <see cref="QueryResult"/>.</returns>
        public virtual QueryResult ExecuteQuery()
        {
            return Task.Run(() => ExecuteQueryAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes the statement and returns a structure containing the number
        /// of records and the <see cref="IArrowArrayStream"/>.
        /// </summary>
        /// <returns>A <see cref="QueryResult"/>.</returns>
        public abstract Task<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Analyzes the statement and returns the schema of the result set that would
        /// be expected if the statement were to be executed.
        /// </summary>
        /// <returns>An Arrow <see cref="Schema"/> describing the result set.</returns>
        public virtual Schema ExecuteSchema()
        {
            return Task.Run(() => ExecuteSchemaAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Analyzes the statement and returns the schema of the result set that would
        /// be expected if the statement were to be executed.
        /// </summary>
        /// <returns>An Arrow <see cref="Schema"/> describing the result set.</returns>
        public virtual Task<Schema> ExecuteSchemaAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Statement does not support ExecuteSchema");
        }

        /// <summary>
        /// Executes an update command and returns the number of
        /// records effected.
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public virtual UpdateResult ExecuteUpdate()
        {
            return Task.Run(() => ExecuteUpdateAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes an update command and returns the number of
        /// records effected.
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public virtual Task<UpdateResult> ExecuteUpdateAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Statement does not support ExecuteUpdate");
        }

        /// <summary>
        /// Execute a result set-generating query and get a list of
        /// partitions of the result set.
        /// </summary>
        public virtual PartitionedResult ExecutePartitioned()
        {
            return Task.Run(() => ExecutePartitionedAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Execute a result set-generating query and get a list of
        /// partitions of the result set.
        /// </summary>
        public virtual Task<PartitionedResult> ExecutePartitionedAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Statement does not support ExecutePartitioned");
        }

        /// <summary>
        /// Gets an option from a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value</returns>
        public virtual object GetOption(string key)
        {
            throw AdbcException.NotImplemented("Statement does not support getting options");
        }

        /// <summary>
        /// Gets an option from a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value</returns>
        public virtual ValueTask<object> GetOptionAsync(string key, CancellationToken cancellationToken = default)
        {
            return new ValueTask<object>(GetOption(key));
        }

        /// <summary>
        /// Get the schema for bound parameters.
        /// </summary>
        public virtual Schema GetParameterSchema()
        {
            return Task.Run(() => GetParameterSchemaAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get the schema for bound parameters.
        /// </summary>
        public virtual Task<Schema> GetParameterSchemaAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Statement does not support GetParameterSchema");
        }

        /// <summary>
        /// Turn this statement into a prepared statement to be
        /// executed multiple times.
        /// </summary>
        public virtual void Prepare()
        {
            Task.Run(() => PrepareAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Turn this statement into a prepared statement to be
        /// executed multiple times.
        /// </summary>
        public virtual Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Statement does not support Prepare");
        }

        /// <summary>
        /// Set an option on a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, object value)
        {
            throw AdbcException.NotImplemented("Statement does not support setting options");
        }

        /// <summary>
        /// Gets an option from a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual ValueTask SetOptionAsync(string key, object value, CancellationToken cancellationToken = default)
        {
            SetOption(key, value);
            return default;
        }

        /// <summary>
        /// Attempts to cancel an in-progress operation on a connection.
        /// </summary>
        /// <remarks>
        /// This can be called during a method like ExecuteQuery or while consuming an ArrowArrayStream
        /// returned from such. Calling this function should make the other function throw a cancellation exception.
        ///
        /// This must always be thread-safe.
        /// </remarks>
        public virtual void Cancel()
        {
            throw AdbcException.NotImplemented("Statement does not support cancellation");
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        protected virtual void Dispose(bool disposing)
        {
        }

        public ValueTask DisposeAsync()
        {
            return DisposeAsyncCore();
        }

        protected virtual ValueTask DisposeAsyncCore()
        {
            Dispose();
            return default;
        }
    }
}

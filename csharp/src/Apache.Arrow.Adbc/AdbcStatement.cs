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
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Statements may represent queries or prepared statements. Statements
    /// may be used multiple times and can be reconfigured (e.g. they can
    /// be reused to execute multiple different queries).
    /// </summary>
    /// <remarks>
    /// Each asynchronous method has a default implementation that runs the
    /// corresponding synchronous method on the thread pool and responds to its
    /// <see cref="CancellationToken"/> by calling <see cref="Cancel"/>.
    /// </remarks>
    public abstract class AdbcStatement : IDisposable, IAsyncDisposable
    {
        public AdbcStatement()
        {
        }

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
        /// Executes the statement and returns a tuple containing the number
        /// of records and the <see cref="IArrowArrayStream"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="ValueTuple"/> where the first item is the number
        /// of records and the second is the <see cref="IArrowArrayStream"/>.
        /// </returns>
        public abstract QueryResult ExecuteQuery();

        /// <summary>
        /// Executes the statement and returns a tuple containing the number
        /// of records and the <see cref="IArrowArrayStream"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="ValueTuple"/> where the first item is the number
        /// of records and the second is the <see cref="IArrowArrayStream"/>.
        /// </returns>
        /// <remarks>
        /// <para>New code should call <see cref="ExecuteQueryAsync(CancellationToken)"/>.</para>
        /// <para>The default implementation runs <see cref="ExecuteQuery"/> on the thread pool.
        /// It doesn't call <see cref="ExecuteQueryAsync(CancellationToken)"/>, because that
        /// overload's default implementation calls this one. A driver that overrides
        /// <see cref="ExecuteQueryAsync(CancellationToken)"/> should also override this
        /// overload to call it with <see cref="CancellationToken.None"/>, so that callers of
        /// either overload get the driver's asynchronous implementation.</para>
        /// </remarks>
        public virtual ValueTask<QueryResult> ExecuteQueryAsync()
        {
            return new ValueTask<QueryResult>(AdbcAsync.Run(ExecuteQuery, Cancel, CancellationToken.None));
        }

        /// <summary>
        /// Executes the statement and returns a tuple containing the number
        /// of records and the <see cref="IArrowArrayStream"/>.
        /// </summary>
        /// <param name="cancellationToken">
        /// A token used to cancel the operation. Cancellation calls <see cref="Cancel"/>.
        /// </param>
        /// <returns>
        /// A <see cref="ValueTuple"/> where the first item is the number
        /// of records and the second is the <see cref="IArrowArrayStream"/>.
        /// </returns>
        /// <remarks>
        /// The default implementation calls <see cref="ExecuteQueryAsync()"/>, so that drivers
        /// written before this overload existed keep their asynchronous behavior, and calls
        /// <see cref="Cancel"/> if the token is cancelled.
        /// </remarks>
        public virtual ValueTask<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return new ValueTask<QueryResult>(Task.FromCanceled<QueryResult>(cancellationToken));
            }

            ValueTask<QueryResult> result = ExecuteQueryAsync();
            if (!cancellationToken.CanBeCanceled)
            {
                return result;
            }
            return new ValueTask<QueryResult>(AdbcAsync.WithCancellation(result.AsTask(), Cancel, cancellationToken));
        }

        /// <summary>
        /// Analyzes the statement and returns the schema of the result set that would
        /// be expected if the statement were to be executed.
        /// </summary>
        /// <returns>An Arrow <see cref="Schema"/> describing the result set.</returns>
        public virtual Schema ExecuteSchema()
        {
            throw AdbcException.NotImplemented("Statement does not support ExecuteSchema");
        }

        /// <summary>
        /// Asynchronously analyzes the statement and returns the schema of the result set
        /// that would be expected if the statement were to be executed.
        /// </summary>
        /// <param name="cancellationToken">A token used to cancel the operation.</param>
        /// <returns>An Arrow <see cref="Schema"/> describing the result set.</returns>
        public virtual Task<Schema> ExecuteSchemaAsync(CancellationToken cancellationToken = default)
        {
            return AdbcAsync.Run(ExecuteSchema, Cancel, cancellationToken);
        }

        /// <summary>
        /// Executes an update command and returns the number of
        /// records effected.
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public abstract UpdateResult ExecuteUpdate();

        /// <summary>
        /// Executes an update command and returns the number of
        /// records effected.
        /// </summary>
        /// <remarks>
        /// <para>New code should call <see cref="ExecuteUpdateAsync(CancellationToken)"/>.</para>
        /// <para>The default implementation runs <see cref="ExecuteUpdate"/> on the thread pool.
        /// It doesn't call <see cref="ExecuteUpdateAsync(CancellationToken)"/>, because that
        /// overload's default implementation calls this one. A driver that overrides
        /// <see cref="ExecuteUpdateAsync(CancellationToken)"/> should also override this
        /// overload to call it with <see cref="CancellationToken.None"/>, so that callers of
        /// either overload get the driver's asynchronous implementation.</para>
        /// </remarks>
        public virtual Task<UpdateResult> ExecuteUpdateAsync()
        {
            return AdbcAsync.Run(ExecuteUpdate, Cancel, CancellationToken.None);
        }

        /// <summary>
        /// Executes an update command and returns the number of
        /// records effected.
        /// </summary>
        /// <param name="cancellationToken">
        /// A token used to cancel the operation. Cancellation calls <see cref="Cancel"/>.
        /// </param>
        /// <remarks>
        /// The default implementation calls <see cref="ExecuteUpdateAsync()"/>, so that drivers
        /// written before this overload existed keep their asynchronous behavior, and calls
        /// <see cref="Cancel"/> if the token is cancelled.
        /// </remarks>
        public virtual Task<UpdateResult> ExecuteUpdateAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<UpdateResult>(cancellationToken);
            }

            Task<UpdateResult> result = ExecuteUpdateAsync();
            if (!cancellationToken.CanBeCanceled)
            {
                return result;
            }
            return AdbcAsync.WithCancellation(result, Cancel, cancellationToken);
        }

        /// <summary>
        /// Execute a result set-generating query and get a list of
        /// partitions of the result set.
        /// </summary>
        public virtual PartitionedResult ExecutePartitioned()
        {
            throw AdbcException.NotImplemented("Statement does not support ExecutePartitioned");
        }

        /// <summary>
        /// Asynchronously execute a result set-generating query and get a list of
        /// partitions of the result set.
        /// </summary>
        /// <param name="cancellationToken">A token used to cancel the operation.</param>
        public virtual Task<PartitionedResult> ExecutePartitionedAsync(CancellationToken cancellationToken = default)
        {
            return AdbcAsync.Run(ExecutePartitioned, Cancel, cancellationToken);
        }

        /// <summary>
        /// Get the schema for bound parameters.
        /// </summary>
        public virtual Schema GetParameterSchema()
        {
            throw AdbcException.NotImplemented("Statement does not support GetParameterSchema");
        }

        /// <summary>
        /// Asynchronously get the schema for bound parameters.
        /// </summary>
        /// <param name="cancellationToken">A token used to cancel the operation.</param>
        public virtual Task<Schema> GetParameterSchemaAsync(CancellationToken cancellationToken = default)
        {
            return AdbcAsync.Run(GetParameterSchema, Cancel, cancellationToken);
        }

        /// <summary>
        /// Turn this statement into a prepared statement to be
        /// executed multiple times.
        /// </summary>
        public virtual void Prepare()
        {
            throw AdbcException.NotImplemented("Statement does not support Prepare");
        }

        /// <summary>
        /// Asynchronously turn this statement into a prepared statement to be
        /// executed multiple times.
        /// </summary>
        /// <param name="cancellationToken">A token used to cancel the operation.</param>
        public virtual Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            return AdbcAsync.Run(Prepare, Cancel, cancellationToken);
        }

        /// <summary>
        /// Set a string option on a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, string value)
        {
            throw AdbcException.NotImplemented("Statement does not support setting options");
        }

        /// <summary>
        /// Sets an integer-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation converts the value to
        /// a string and calls <see cref="SetOption(string, string)"/>.
        /// </remarks>
        public virtual void SetOptionInt64(string key, long value)
        {
            SetOption(key, AdbcOptionValue.FormatInt64(value));
        }

        /// <summary>
        /// Sets a floating-point-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation converts the value to
        /// a string and calls <see cref="SetOption(string, string)"/>.
        /// </remarks>
        public virtual void SetOptionDouble(string key, double value)
        {
            SetOption(key, AdbcOptionValue.FormatDouble(value));
        }

        /// <summary>
        /// Sets a binary-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value. Implementations must copy the data if they retain it.</param>
        /// <remarks>Added in ADBC 1.1.0.</remarks>
        public virtual void SetOptionBytes(string key, ReadOnlySpan<byte> value)
        {
            throw AdbcException.NotImplemented("Statement does not support binary options");
        }

        /// <summary>
        /// Gets the value of a string-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <exception cref="AdbcException">
        /// With <see cref="AdbcStatusCode.NotFound"/> if the option is not recognized or has no value.
        /// </exception>
        /// <remarks>Added in ADBC 1.1.0.</remarks>
        public virtual string GetOption(string key)
        {
            throw AdbcOptionValue.NotFound(key);
        }

        /// <summary>
        /// Gets the value of an integer-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation parses the result of
        /// <see cref="GetOption(string)"/>.
        /// </remarks>
        public virtual long GetOptionInt64(string key)
        {
            return AdbcOptionValue.ParseInt64(key, GetOption(key));
        }

        /// <summary>
        /// Gets the value of a floating-point-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation parses the result of
        /// <see cref="GetOption(string)"/>. Drivers use this to report
        /// <see cref="AdbcOptions.Statement.Progress"/> and
        /// <see cref="AdbcOptions.Statement.MaxProgress"/>.
        /// </remarks>
        public virtual double GetOptionDouble(string key)
        {
            return AdbcOptionValue.ParseDouble(key, GetOption(key));
        }

        /// <summary>
        /// Gets the value of a binary-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <remarks>Added in ADBC 1.1.0.</remarks>
        public virtual byte[] GetOptionBytes(string key)
        {
            throw AdbcOptionValue.NotFound(key);
        }

        public virtual void Dispose()
        {
        }

        /// <summary>
        /// Asynchronously releases the resources used by the statement.
        /// </summary>
        /// <remarks>
        /// The default implementation calls <see cref="Dispose"/>.
        /// </remarks>
        public virtual ValueTask DisposeAsync()
        {
            Dispose();
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
    }
}

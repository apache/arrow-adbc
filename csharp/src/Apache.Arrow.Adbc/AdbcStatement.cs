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
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Statements may represent queries or prepared statements. Statements
    /// may be used multiple times and can be reconfigured (e.g. they can
    /// be reused to execute multiple different queries).
    /// </summary>
    public abstract class AdbcStatement : IDisposable
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
        public virtual async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            return await Task.Run(() => ExecuteQuery());
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
        /// Executes an update command and returns the number of
        /// records effected.
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public abstract UpdateResult ExecuteUpdate();

        /// <summary>
        /// Executes an update command and returns the number of
        /// records effected.
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public virtual async Task<UpdateResult> ExecuteUpdateAsync()
        {
            return await Task.Run(() => ExecuteUpdate());
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
        /// Get the schema for bound parameters.
        /// </summary>
        public virtual Schema GetParameterSchema()
        {
            throw AdbcException.NotImplemented("Statement does not support GetParameterSchema");
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
        /// Set a string option on a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, string value)
        {
            throw AdbcException.NotImplemented("Statement does not support setting options");
        }

        public virtual void Dispose()
        {
        }

        /// <summary>
        /// Gets a value from the Arrow array at the specified index,
        /// using the Field metadata for information.
        /// </summary>
        /// <param name="arrowArray">
        /// The Arrow array.
        /// </param>
        /// <param name="field">
        /// The <see cref="Field"/> from the <see cref="Schema"/> that can
        /// be used for metadata inspection.
        /// </param>
        /// <param name="index">
        /// The index in the array to get the value from.
        /// </param>
        public virtual object? GetValue(IArrowArray arrowArray, int index)
        {
            if (arrowArray == null) throw new ArgumentNullException(nameof(arrowArray));
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            switch (arrowArray)
            {
                case BooleanArray booleanArray:
                    return booleanArray.GetValue(index);
                case Date32Array date32Array:
                    return date32Array.GetDateTime(index);
                case Date64Array date64Array:
                    return date64Array.GetDateTime(index);
                case Decimal128Array decimal128Array:
                    return decimal128Array.GetSqlDecimal(index);
                case Decimal256Array decimal256Array:
                    return decimal256Array.GetString(index);
                case DoubleArray doubleArray:
                    return doubleArray.GetValue(index);
                case FloatArray floatArray:
                    return floatArray.GetValue(index);
#if NET5_0_OR_GREATER
                case PrimitiveArray<Half> halfFloatArray:
                    return halfFloatArray.GetValue(index);
#endif
                case Int8Array int8Array:
                    return int8Array.GetValue(index);
                case Int16Array int16Array:
                    return int16Array.GetValue(index);
                case Int32Array int32Array:
                    return int32Array.GetValue(index);
                case Int64Array int64Array:
                    return int64Array.GetValue(index);
                case StringArray stringArray:
                    return stringArray.GetString(index);
#if NET6_0_OR_GREATER
                case Time32Array time32Array:
                    return time32Array.GetTime(index);
                case Time64Array time64Array:
                    return time64Array.GetTime(index);
#else
                case Time32Array time32Array:
                    int? time32 = time32Array.GetValue(index);
                    if (time32 == null) { return null; }
                    return ((Time32Type)time32Array.Data.DataType).Unit switch
                    {
                        TimeUnit.Second => TimeSpan.FromSeconds(time32.Value),
                        TimeUnit.Millisecond => TimeSpan.FromMilliseconds(time32.Value),
                        _ => throw new InvalidDataException("Unsupported time unit for Time32Type")
                    };
                case Time64Array time64Array:
                    long? time64 = time64Array.GetValue(index);
                    if (time64 == null) { return null; }
                    return ((Time64Type)time64Array.Data.DataType).Unit switch
                    {
                        TimeUnit.Microsecond => TimeSpan.FromTicks(time64.Value * 10),
                        TimeUnit.Nanosecond => TimeSpan.FromTicks(time64.Value / 100),
                        _ => throw new InvalidDataException("Unsupported time unit for Time64Type")
                    };
#endif
                case TimestampArray timestampArray:
                    return timestampArray.GetTimestamp(index);
                case UInt8Array uInt8Array:
                    return uInt8Array.GetValue(index);
                case UInt16Array uInt16Array:
                    return uInt16Array.GetValue(index);
                case UInt32Array uInt32Array:
                    return uInt32Array.GetValue(index);
                case UInt64Array uInt64Array:
                    return uInt64Array.GetValue(index);

                case BinaryArray binaryArray:
                    if (!binaryArray.IsNull(index))
                        return binaryArray.GetBytes(index).ToArray();

                    return null;

                    // not covered:
                    // -- struct array
                    // -- dictionary array
                    // -- fixed size binary
                    // -- list array
                    // -- union array
            }

            return null;
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

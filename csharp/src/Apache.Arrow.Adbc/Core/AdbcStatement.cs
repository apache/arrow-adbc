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
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Core
{
    /// <summary>
    /// Statements may represent queries or prepared statements. Statements may be used multiple times and can be reconfigured (e.g. they can be reused to execute multiple different queries).
    /// </summary>
    public abstract class AdbcStatement : IDisposable
    {
        public AdbcStatement()
        {
            Timeout = 30;
        }

        /// <summary>
        /// Gets or sets a SQL query to be executed on this statement.
        /// </summary>
        public virtual string SqlQuery { get; set; }

        /// <summary>
        /// Gets or sets the Substrait plan.
        /// </summary>
        public virtual byte[] SubstraitPlan
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        public virtual void Bind()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Executes the statement and returns a tuple containing the number of records and the <see cref="IArrowArrayStream"/>..
        /// </summary>
        /// <returns>A <see cref="ValueTuple"/> where the first item is the number of records and the second is the <see cref="IArrowArrayStream"/>.</returns>
        public abstract QueryResult ExecuteQuery();

        /// <summary>
        /// Executes the statement and returns a tuple containing the number of records and the <see cref="IArrowArrayStream"/>..
        /// </summary>
        /// <returns>A <see cref="ValueTuple"/> where the first item is the number of records and the second is the <see cref="IArrowArrayStream"/>.</returns>
        public virtual async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            return await Task.Run(() => ExecuteQuery());
        }

        /// <summary>
        /// Executes an update command and returns the number of records effected.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public abstract UpdateResult ExecuteUpdate();

        // <summary>
        /// Executes an update command and returns the number of records effected.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public virtual async Task<UpdateResult> ExecuteUpdateAsync()
        {
            return await Task.Run(() => ExecuteUpdate());
        }

        /// <summary>
        /// Timeout (in seconds) for statement execution.
        /// </summary>
        /// <remarks>The default is 30 seconds.</remarks>
        public virtual int Timeout { get; set; }

        /// <summary>
        /// Execute a result set-generating query and get a list of partitions of the result set.
        /// </summary>
        /// <returns><see cref="PartitionedResult"/></returns>
        public virtual PartitionedResult ExecutePartitioned()
        {
            throw AdbcException.NotImplemented("Statement does not support executePartitioned");
        }

        /// <summary>
        /// Get the schema for bound parameters.
        /// </summary>
        /// <returns><see cref="Schema"/></returns>
        public virtual Schema GetParameterSchema()
        {
            throw AdbcException.NotImplemented("Statement does not support GetParameterSchema");
        }

        /// <summary>
        ///  Turn this statement into a prepared statement to be
        ///  executed multiple times.
        /// </summary>
        public virtual void Prepare()
        {
            throw AdbcException.NotImplemented("Statement does not support Prepare");
        }

        public virtual void Dispose()
        {
        }

        /// <summary>
        /// Gets the .NET type based on the Arrow field metadata
        /// </summary>
        /// <param name="f">Field from Arrow Schema</param>
        /// <returns></returns>
        public virtual Type ConvertArrowType(Field f)
        {
            switch (f.DataType.TypeId)
            {
                case ArrowTypeId.Binary:
                    return typeof(byte[]);

                case ArrowTypeId.Boolean:
                    return typeof(bool);

                case ArrowTypeId.Decimal128:
                case ArrowTypeId.Decimal256:
                    return typeof(decimal);

                case ArrowTypeId.Date32:
                case ArrowTypeId.Date64:
                    return typeof(DateTime);

                case ArrowTypeId.Double:
                    return typeof(double);

#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    return typeof(Half);
#else
                case ArrowTypeId.HalfFloat:
                    return typeof(float);
#endif
                case ArrowTypeId.Float:
                    return typeof(float);

                case ArrowTypeId.Int8:
                    return typeof(sbyte);
                case ArrowTypeId.Int16:
                    return typeof(short);
                case ArrowTypeId.Int32:
                    return typeof(int);
                case ArrowTypeId.Int64:
                    return typeof(long);

                case ArrowTypeId.String:
                    return typeof(string);

                case ArrowTypeId.Struct:
                    goto default;

                case ArrowTypeId.Timestamp:
                    return typeof(DateTime);

                case ArrowTypeId.UInt8:
                    return typeof(sbyte);
                case ArrowTypeId.UInt16:
                    return typeof(ushort);
                case ArrowTypeId.UInt32:
                    return typeof(uint);
                case ArrowTypeId.UInt64:
                    return typeof(ulong);


                case ArrowTypeId.Null:
                    return null;

                default:
                    return f.DataType.GetType();
            }
        }

        /// <summary>
        /// Converts a .NET type to an IArrowType
        /// </summary>
        /// <param name="type">The current type</param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public virtual IArrowType ConvertNetType(Type type)
        {

            // these will have different options based on the data source -- get it working for now but may need a more pluggable model
            // per https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/data-type-mappings-in-ado-net

            if (type == typeof(sbyte))
                return Int8Type.Default;
            if (type == typeof(short))
                return Int16Type.Default;
            if (type == typeof(int))
                return Int32Type.Default;
            if (type == typeof(int))
                return Int32Type.Default;
            if (type == typeof(long))
                return Int64Type.Default;
#if NET5_0_OR_GREATER
            if (type == typeof(Half))
                return HalfFloatType.Default;
#endif
            if (type == typeof(float))
                return FloatType.Default;

            if (type == typeof(double))
                return DoubleType.Default;

            if (type == typeof(bool))
                return BooleanType.Default;

            if (type == typeof(string))
                return StringType.Default;

            if (type == typeof(byte))
                return BinaryType.Default;

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
                return TimestampType.Default;

            if (type == typeof(byte) || type == typeof(byte[]))
                return BinaryType.Default;

            //if (type == typeof(decimal))
            //    return new Decimal256Type(38,0);

            throw new NotImplementedException($"No implementation exists for {type.FullName}");
        }

        /// <summary>
        /// Gets a value from the Arrow array at the specified index, using the Field metadata for information.
        /// </summary>
        /// <param name="arrowArray">The Arrow array.</param>
        /// <param name="field">The <see cref="Field"/> from the <see cref="Schema"/> that can be used for metadata inspection.</param>
        /// <param name="index">The index in the array to get the value from.</param>
        /// <returns></returns>
        public abstract object GetValue(IArrowArray arrowArray, Field field, int index);

        /// <summary>
        /// For decimals, Arrow throws an OverflowException if a value is < decimal.min or > decimal.max
        /// So parse the numeric value and return it as a string, if possible
        /// </summary>
        /// <param name="oex"></param>
        /// <returns>A string value of the decimal that threw the exception or rethrows the OverflowException.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public virtual string ParseDecimalValueFromOverflowException(OverflowException oex)
        {
            if (oex == null)
                throw new ArgumentNullException(nameof(oex));

            // any decimal value, positive or negative, with or without a decimal in place
            Regex regex = new Regex(" -?\\d*\\.?\\d* ");

            var matches = regex.Matches(oex.Message);

            var nonEmptyMatches = matches.Where(x => !string.IsNullOrEmpty(x.Value));

            if (nonEmptyMatches.Count() > 0)
            {
                var result = nonEmptyMatches.First().Value;
                return result;
            }

            throw oex;
        }
    }
}

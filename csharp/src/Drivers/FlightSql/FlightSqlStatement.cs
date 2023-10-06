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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Apache.Arrow.Flight;
using Grpc.Core;

namespace Apache.Arrow.Adbc.Drivers.FlightSql
{
    /// <summary>
    /// A Flight SQL implementation of <see cref="AdbcStatement"/>.
    /// </summary>
    public class FlightSqlStatement : AdbcStatement
    {
        private FlightSqlConnection _flightSqlConnection;

        public FlightSqlStatement(FlightSqlConnection flightSqlConnection)
        {
            _flightSqlConnection = flightSqlConnection;
        }

        public override async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            FlightInfo info = await GetInfo(SqlQuery, _flightSqlConnection.Metadata);

            return new QueryResult(info.TotalRecords, new FlightSqlResult(_flightSqlConnection, info));
        }

        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryAsync().Result;
        }

        public override UpdateResult ExecuteUpdate()
        {
            throw new NotImplementedException();
        }

        public async ValueTask<FlightInfo> GetInfo(string query, Metadata headers)
        {
            FlightDescriptor commandDescripter = FlightDescriptor.CreateCommandDescriptor(query);

            return await _flightSqlConnection.FlightClient.GetInfo(commandDescripter, headers).ResponseAsync;
        }

        /// <summary>
        /// Gets a value from the Arrow array at the specified index
        /// using the Arrow field for metadata.
        /// </summary>
        /// <param name="arrowArray">
        /// The array containing the value.
        /// </param>
        /// <param name="field">
        /// The Arrow field.
        /// </param>
        /// <param name="index">
        /// The index of the item.
        /// </param>
        /// <returns>
        /// The item at the index position.
        /// </returns>
        public override object GetValue(IArrowArray arrowArray, Field field, int index)
        {
            if (arrowArray is BooleanArray)
            {
                return Convert.ToBoolean(((BooleanArray)arrowArray).Values[index]);
            }
            else if (arrowArray is Date32Array)
            {
                Date32Array date32Array = (Date32Array)arrowArray;

                return date32Array.GetDateTime(index);
            }
            else if (arrowArray is Date64Array)
            {
                Date64Array date64Array = (Date64Array)arrowArray;

                return date64Array.GetDateTime(index);
            }
            else if (arrowArray is Decimal128Array)
            {
                try
                {
                    // the value may be <decimal.min or >decimal.max
                    // then Arrow throws an exception
                    // no good way to check prior to
                    return ((Decimal128Array)arrowArray).GetValue(index);
                }
                catch (OverflowException oex)
                {
                    return ParseDecimalValueFromOverflowException(oex);
                }
            }
            else if (arrowArray is Decimal256Array)
            {
                try
                {
                    return ((Decimal256Array)arrowArray).GetValue(index);
                }
                catch (OverflowException oex)
                {
                    return ParseDecimalValueFromOverflowException(oex);
                }
            }
            else if (arrowArray is DoubleArray)
            {
                return ((DoubleArray)arrowArray).GetValue(index);
            }
            else if (arrowArray is FloatArray)
            {
                return ((FloatArray)arrowArray).GetValue(index);
            }
#if NET5_0_OR_GREATER
            else if (arrowArray is PrimitiveArray<Half>)
            {
                // TODO: HalfFloatArray not present in current library

                return ((PrimitiveArray<Half>)arrowArray).GetValue(index);
            }
#endif
            else if (arrowArray is Int8Array)
            {
                return ((Int8Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is Int16Array)
            {
                return ((Int16Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is Int32Array)
            {
                return ((Int32Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is Int64Array)
            {
                Int64Array array = (Int64Array)arrowArray;
                return array.GetValue(index);
            }
            else if (arrowArray is StringArray)
            {
                return ((StringArray)arrowArray).GetString(index);
            }
            else if (arrowArray is Time32Array)
            {
                return ((Time32Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is Time64Array)
            {
                return ((Time64Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is TimestampArray)
            {
                TimestampArray timestampArray = (TimestampArray)arrowArray;
                DateTimeOffset dateTimeOffset = timestampArray.GetTimestamp(index).Value;
                return dateTimeOffset;
            }
            else if (arrowArray is UInt8Array)
            {
                return ((UInt8Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is UInt16Array)
            {
                return ((UInt16Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is UInt32Array)
            {
                return ((UInt32Array)arrowArray).GetValue(index);
            }
            else if (arrowArray is UInt64Array)
            {
                return ((UInt64Array)arrowArray).GetValue(index);
            }

            // not covered:
            // -- struct array
            // -- binary array
            // -- dictionary array
            // -- fixed size binary
            // -- list array
            // -- union array

            return null;
        }

        /// <summary>
        /// For decimals, Arrow throws an OverflowException if a value
        /// is < decimal.min or > decimal.max
        /// So parse the numeric value and return it as a string,
        /// if possible
        /// </summary>
        /// <param name="oex">The OverflowException</param>
        /// <returns>
        /// A string value of the decimal that threw the exception
        /// or rethrows the OverflowException.
        /// </returns>
        /// <exception cref="ArgumentNullException"></exception>
        private string ParseDecimalValueFromOverflowException(OverflowException oex)
        {
            if (oex == null)
                throw new ArgumentNullException(nameof(oex));

            // any decimal value, positive or negative, with or without a decimal in place
            Regex regex = new Regex(" -?\\d*\\.?\\d* ");

            var matches = regex.Matches(oex.Message);

            foreach (Match match in matches)
            {
                string value = match.Value;

                if (!string.IsNullOrEmpty(value))
                    return value;
            }

            throw oex;
        }
    }
}

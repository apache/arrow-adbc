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
                case Time32Array time32Array:
                    return time32Array.GetValue(index);
                case Time64Array time64Array:
                    return time64Array.GetValue(index);
                case TimestampArray timestampArray:
                    DateTimeOffset dateTimeOffset = timestampArray.GetTimestamp(index).Value;
                    return dateTimeOffset;
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
            }

            // not covered:
            // -- struct array
            // -- dictionary array
            // -- fixed size binary
            // -- list array
            // -- union array

            return null;
        }
    }
}

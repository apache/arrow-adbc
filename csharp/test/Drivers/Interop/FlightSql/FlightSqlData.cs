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
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.FlightSql
{
    /// <summary>
    /// Gets the sample data from Flight SQL.
    /// </summary>
    internal class FlightSqlData
    {
        /// <summary>
        /// Sample data
        /// </summary>
        public static SampleDataBuilder GetSampleData()
        {
            SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();

            // DuckDB
            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT " +
                            "42 AS \"TinyInt\", "  +
                            "12345 AS \"SmallInt\", " +
                            "987654321 AS \"Integer\", "  +
                            "1234567890123 AS \"BigInt\", " +
                            "3.141592 AS \"Real\", " +
                            "123.456789123456 AS \"Double\", " +
                            "DECIMAL '12345.67' AS \"Decimal\",  " +
                            "'DuckDB' AS \"Varchar\", " +
                            "BLOB 'abc' AS \"Blob\", " +
                            "TRUE AS \"Boolean\"," +
                            "DATE '2024-09-10' AS \"Date\", " +
                            "TIME '12:34:56' AS \"Time\", " +
                            "TIMESTAMP '2024-09-10 12:34:56' AS \"Timestamp\", " +
                            "INTERVAL '1 year' AS \"Interval\", " +
                            "'[1, 2, 3]'::JSON AS \"JSON\", " +
                            "'[{\"key\": \"value\"}]'::JSON AS \"JSON_Array\", " +
                            "to_json([true, false, null]) AS \"List_JSON\", " + // need to convert List values to json
                            "to_json(MAP {'key': 'value'}) AS \"Map_JSON\" ", // need to convert Map values to json
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                          new ColumnNetTypeArrowTypeValue("TinyInt", typeof(int), typeof(Int32Type), 42),
                          new ColumnNetTypeArrowTypeValue("SmallInt", typeof(int), typeof(Int32Type), 12345),
                          new ColumnNetTypeArrowTypeValue("Integer", typeof(int), typeof(Int32Type), 987654321),
                          new ColumnNetTypeArrowTypeValue("BigInt", typeof(Int64), typeof(Int64Type), 1234567890123),
                          new ColumnNetTypeArrowTypeValue("Real", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(3.141592m)),
                          new ColumnNetTypeArrowTypeValue("Double", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123.456789123456m)),
                          new ColumnNetTypeArrowTypeValue("Decimal", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(12345.67m)),
                          new ColumnNetTypeArrowTypeValue("Varchar", typeof(string), typeof(StringType), "DuckDB"),
                          new ColumnNetTypeArrowTypeValue("Blob", typeof(byte[]), typeof(BinaryType),  Encoding.UTF8.GetBytes("abc")),
                          new ColumnNetTypeArrowTypeValue("Boolean", typeof(bool), typeof(BooleanType), true),
                          new ColumnNetTypeArrowTypeValue("Date", typeof(DateTime), typeof(Date32Type), new DateTime(2024, 09, 10)),
#if NET6_0_OR_GREATER
                          new ColumnNetTypeArrowTypeValue("Time", typeof(TimeOnly), typeof(Time64Type), new TimeOnly(12, 34, 56)),
#else
                          new ColumnNetTypeArrowTypeValue("Time", typeof(TimeSpan), typeof(Time64Type), new TimeSpan(12, 34, 56)),
#endif
                          new ColumnNetTypeArrowTypeValue("Timestamp", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2024, 9, 10, 12, 34, 56), TimeSpan.Zero)),
                          new ColumnNetTypeArrowTypeValue("Interval", typeof(MonthDayNanosecondInterval), typeof(IntervalType), new MonthDayNanosecondInterval(12, 0, 0)),
                          new ColumnNetTypeArrowTypeValue("JSON", typeof(string), typeof(StringType), "[1, 2, 3]"),
                          new ColumnNetTypeArrowTypeValue("JSON_Array", typeof(string), typeof(StringType), "[{\"key\": \"value\"}]"),
                          new ColumnNetTypeArrowTypeValue("List_JSON", typeof(string), typeof(StringType),"[true,false,null]"),
                          new ColumnNetTypeArrowTypeValue("Map_JSON", typeof(string), typeof(StringType), "{\"key\":\"value\"}"),
                    }
                });

            // TODO: SQLite

            // TODO: Dremio

            return sampleDataBuilder;
        }
    }
}
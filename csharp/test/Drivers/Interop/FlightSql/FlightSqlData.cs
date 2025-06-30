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
using System.Diagnostics;
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
        /// <param name="environmentType">
        /// The type of environment to get the sample data for.
        /// </param>
        public static SampleDataBuilder GetSampleData(
            FlightSqlTestEnvironmentType environmentType
        )
        {
            switch (environmentType)
            {
                case FlightSqlTestEnvironmentType.Denodo:
                    return GetDenodoSampleData();
                case FlightSqlTestEnvironmentType.Dremio:
                    return GetDremioSampleData();
                case FlightSqlTestEnvironmentType.DuckDB:
                    return GetDuckDbSampleData();
                case FlightSqlTestEnvironmentType.SQLite:
                    return GetSQLiteSampleData();
                case FlightSqlTestEnvironmentType.SpiceAI:
                    return GetSpiceAISampleData();
                default:
                    throw new InvalidOperationException("Unknown environment type.");
            }
        }

        private static SampleDataBuilder GetDenodoSampleData()
        {
            return new SampleDataBuilder();
        }

        private static SampleDataBuilder GetDremioSampleData()
        {
            ListArray.Builder labuilder = new ListArray.Builder(Int32Type.Default);
            Int32Array.Builder numbersBuilder = (Int32Array.Builder)labuilder.ValueBuilder;
            labuilder.Append();
            numbersBuilder.AppendRange(new List<int>() { 1, 2, 3 });

            Int32Array numbersArray = numbersBuilder.Build();

            SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();

            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT " +
                            "TRUE AS sample_boolean, " +
                            "CAST(123 AS INTEGER) AS sample_integer, " +
                            "CAST(1234567890 AS BIGINT) AS sample_bigint, " +
                            "CAST(123.45 AS FLOAT) AS sample_float," +
                            "CAST(12345.6789 AS DOUBLE) AS sample_double,  " +
                            "CAST(12345.67 AS DECIMAL(10, 2)) AS sample_decimal, " +
                            "'Sample Text' AS sample_varchar,  " +
                            "DATE '2024-01-01' AS sample_date,  " +
                            "TIME '12:34:56' AS sample_time,  " +
                            "TIMESTAMP '2024-01-01 12:34:56' AS sample_timestamp,  " +
                            "ARRAY[1, 2, 3] AS sample_array, " +
                            "CONVERT_FROM('{\"name\":\"Gnarly\", \"age\":7, \"car\":null}', 'json') as sample_struct",
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                            new ColumnNetTypeArrowTypeValue("sample_boolean", typeof(bool), typeof(BooleanType), true),
                            new ColumnNetTypeArrowTypeValue("sample_integer", typeof(int), typeof(Int32Type), 123),
                            new ColumnNetTypeArrowTypeValue("sample_bigint", typeof(Int64), typeof(Int64Type), 1234567890L),
                            new ColumnNetTypeArrowTypeValue("sample_float", typeof(float), typeof(FloatType), 123.45f),
                            new ColumnNetTypeArrowTypeValue("sample_double", typeof(double), typeof(DoubleType), 12345.6789d),
                            new ColumnNetTypeArrowTypeValue("sample_decimal", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(12345.67m)),
                            new ColumnNetTypeArrowTypeValue("sample_varchar", typeof(string), typeof(StringType), "Sample Text"),
                            new ColumnNetTypeArrowTypeValue("sample_date", typeof(DateTime), typeof(Date64Type), new DateTime(2024, 1, 1)),
#if NET6_0_OR_GREATER
                            new ColumnNetTypeArrowTypeValue("sample_time", typeof(TimeOnly), typeof(Time32Type), new TimeOnly(12, 34, 56)),
#else
                            new ColumnNetTypeArrowTypeValue("sample_time", typeof(TimeSpan), typeof(Time32Type), new TimeSpan(12, 34, 56)),
#endif
                            new ColumnNetTypeArrowTypeValue("sample_timestamp", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2024, 1, 1, 12, 34, 56), TimeSpan.Zero)),
                            new ColumnNetTypeArrowTypeValue("sample_array", typeof(Int32Array), typeof(ListType), numbersArray),
                            new ColumnNetTypeArrowTypeValue("sample_struct", typeof(string), typeof(StructType), "{\"name\":\"Gnarly\",\"age\":7}"),
                    }
                });

            return sampleDataBuilder;
        }

        private static SampleDataBuilder GetDuckDbSampleData()
        {
            SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();

            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT " +
                            "42 AS \"TinyInt\", " +
                            "12345 AS \"SmallInt\", " +
                            "987654321 AS \"Integer\", " +
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
                }
            );

            return sampleDataBuilder;
        }

        private static SampleDataBuilder GetSQLiteSampleData()
        {
            string tempTable = Guid.NewGuid().ToString().Replace("-", "");

            SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();

            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    // for SQLite, we can't just select data without a
                    // table because we get mixed schemas that are returned,
                    // resulting in an error. so create a temp table,
                    // insert data, select data, then remove the table.
                    PreQueryCommands = new List<string>()
                    {
                            $"CREATE TEMP TABLE [{tempTable}] (INTEGER_COLUMN INTEGER, TEXT_COLUMN TEXT, BLOB_COLUMN BLOB, REAL_COLUMN REAL, NULL_COLUMN NULL);",
                            $"INSERT INTO [{tempTable}] (INTEGER_COLUMN, TEXT_COLUMN, BLOB_COLUMN, REAL_COLUMN, NULL_COLUMN) VALUES (42, 'Hello, SQLite', X'426C6F62', 3.14159, NULL);"
                    },
                    Query = $"SELECT INTEGER_COLUMN, TEXT_COLUMN, BLOB_COLUMN, REAL_COLUMN, NULL_COLUMN FROM [{tempTable}];",
                    PostQueryCommands = new List<string>()
                    {
                            $"DROP TABLE [{tempTable}];"
                    },
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                          new ColumnNetTypeArrowTypeValue("INTEGER_COLUMN", typeof(long), typeof(Int64Type), 42L),
                          new ColumnNetTypeArrowTypeValue("TEXT_COLUMN", typeof(string), typeof(StringType), "Hello, SQLite"),
                          new ColumnNetTypeArrowTypeValue("BLOB_COLUMN", typeof(byte[]), typeof(BinaryType),  Encoding.UTF8.GetBytes("Blob")),
                          new ColumnNetTypeArrowTypeValue("REAL_COLUMN", typeof(double), typeof(DoubleType), 3.14159d),
                          new ColumnNetTypeArrowTypeValue("NULL_COLUMN", typeof(UnionType), typeof(UnionType), null),
                    }
                }
            );

            return sampleDataBuilder;
        }

        private static SampleDataBuilder GetSpiceAISampleData()
        {
            SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();

            sampleDataBuilder.Samples.Add(

                // Primitive types
                new SampleData()
                {
                    Query = "SELECT " +
                            "TRUE AS \"Boolean\", " +
                            "32767::SMALLINT AS \"SmallInt\", " +
                            "2147483647::INTEGER AS \"Integer\", " +
                            "9223372036854775807::BIGINT AS \"BigInt\", " +
                            "3.14::REAL AS \"Real\", " +
                            "3.141592653589793::DOUBLE PRECISION AS \"Double\", " +
                            "'12345.67'::NUMERIC(10,2) AS \"Decimal\", " +
                            "'Hello, Arrow!'::TEXT AS \"Varchar\", " +
                            "'2024-09-10'::DATE AS \"Date\", " +
                            "'12:34:56'::TIME AS \"Time\", " +
                            "'2024-09-10 12:34:56'::TIMESTAMP AS \"Timestamp\", " +
                            "INTERVAL '1 year' AS \"Interval\"",
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                        new ColumnNetTypeArrowTypeValue("Boolean", typeof(bool), typeof(BooleanType), true),
                        new ColumnNetTypeArrowTypeValue("SmallInt", typeof(short), typeof(Int16Type), (short)32767),
                        new ColumnNetTypeArrowTypeValue("Integer", typeof(int), typeof(Int32Type), 2147483647),
                        new ColumnNetTypeArrowTypeValue("BigInt", typeof(long), typeof(Int64Type), 9223372036854775807L),
                        new ColumnNetTypeArrowTypeValue("Real", typeof(float), typeof(FloatType), 3.14f),
                        new ColumnNetTypeArrowTypeValue("Double", typeof(double), typeof(DoubleType), 3.141592653589793d),
                        new ColumnNetTypeArrowTypeValue("Decimal", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(12345.67m)),
                        new ColumnNetTypeArrowTypeValue("Varchar", typeof(string), typeof(StringType), "Hello, Arrow!"),
                        new ColumnNetTypeArrowTypeValue("Date", typeof(DateTime), typeof(Date32Type), new DateTime(2024, 9, 10)),
#if NET6_0_OR_GREATER
                        new ColumnNetTypeArrowTypeValue("Time", typeof(TimeOnly), typeof(Time64Type), new TimeOnly(12, 34, 56)),
#else
                        new ColumnNetTypeArrowTypeValue("Time", typeof(TimeSpan), typeof(Time64Type), new TimeSpan(12, 34, 56)),
#endif
                        new ColumnNetTypeArrowTypeValue("Timestamp", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2024, 9, 10, 12, 34, 56), TimeSpan.Zero)),
                        new ColumnNetTypeArrowTypeValue("Interval", typeof(MonthDayNanosecondInterval), typeof(IntervalType), new MonthDayNanosecondInterval(12, 0, 0)),
                    }
                }
            );

            Dictionary<string, object?> struct_record = new Dictionary<string, object?>();
            struct_record["c0"] = 0;
            struct_record["c1"] = "Test Value";

            sampleDataBuilder.Samples.Add(
                // Lists and Structs
                new SampleData()
                {
                    StructBehavior = "Strict",
                    Query = "SELECT " +
                            "ARRAY[n_regionkey, n_nationkey] AS \"List\", " +
                            "struct(n_regionkey, 'Test Value') AS Struct "+
                            "FROM nation WHERE n_regionkey = 0 AND n_nationkey = 5",
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                        new ColumnNetTypeArrowTypeValue("List", typeof(Int32Array), typeof(ListType), new Int32Array.Builder().AppendRange(new[] { 0, 5}).Build()),
                        new ColumnNetTypeArrowTypeValue("Struct", typeof(Dictionary<string, object?>), typeof(StructType), struct_record),
                    }
                }
            );

            return sampleDataBuilder;
        }
    }
}

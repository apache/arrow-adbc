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
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Gets the sample data from Snowflake.
    /// </summary>
    internal class SnowflakeData
    {
        /// <summary>
        /// Sample data
        /// </summary>
        public static SampleDataBuilder GetSampleData()
        {
            SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();

            // standard data
            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT " +
                            "CAST(1 as NUMBER(38,0)) as NUMBERTYPE, " +
                            "CAST(123.1 as NUMBER(18,1)) as DECIMALTYPE, " +
                            "CAST(123.1 as NUMBER(28,1)) as NUMERICTYPE, " +
                            "CAST(123 as NUMBER(38,0)) as INTTYPE, " +
                            "CAST(123 as NUMBER(38,0)) as INTEGERTYPE, " +
                            "CAST(123 as NUMBER(38,0)) as BIGINTTYPE, " +
                            "CAST(123 as NUMBER(38,0)) as SMALLINTTYPE, " +
                            "CAST(123 as NUMBER(38,0)) as TINYINTTYPE, " +
                            "CAST(123 as NUMBER(38,0)) as BYTEINTTYPE, " +
                            "CAST(123.45 as FLOAT) as FLOATTYPE, " +
                            "CAST(123.45 as FLOAT) as FLOAT4TYPE, " +
                            "CAST(123.45 as FLOAT) as FLOAT8TYPE, " +
                            "CAST(123.45 as FLOAT) as DOUBLETYPE, " +
                            "CAST(123.45 as FLOAT) as DOUBLEPRECISIONTYPE, " +
                            "CAST(123.45 as FLOAT) as REALTYPE, " +
                            "CAST('Hello' as VARCHAR(16777216)) as VARCHARTYPE, " +
                            "CAST('H'  as VARCHAR(1)) as CHARTYPE, " +
                            "CAST('H'  as VARCHAR(1)) as CHARACTERTYPE, " +
                            "CAST('H'  as VARCHAR(16777216)) as STRINGTYPE, " +
                            "CAST('Hello World'  as VARCHAR(16777216)) as TEXTTYPE, " +
                            "to_binary('Hello World', 'UTF-8') as BINARYTYPE, " +
                            "to_binary('Hello World', 'UTF-8') as VARBINARYTYPE, " +
                            "CAST(TRUE  as BOOLEAN) as BOOLEANTYPE, " +
                            "CAST('2023-07-28'  as DATE) as DATETYPE, " +
                            "CAST('2023-07-28 12:34:56' as TIMESTAMP_NTZ(9)) as DATETIMETYPE, " +
                            "CAST('12:34:56'  as TIME(9)) as TIMETYPE, " +
                            "CAST('2023-07-28 12:34:56'  as TIMESTAMP_NTZ(9)) as TIMESTAMPTYPE, " +
                            "CAST('2023-07-28 12:34:56'  as TIMESTAMP_LTZ(9)) as TIMESTAMPLTZTYPE, " +
                            "CAST('2023-07-28 12:34:56'  as TIMESTAMP_NTZ(9)) as TIMESTAMPNTZTYPE, " +
                            "CAST('2023-07-28 12:34:56' as TIMESTAMP_TZ(9)) as TIMESTAMPTZTYPE",
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                        new ColumnNetTypeArrowTypeValue("NUMBERTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(1m)),
                        new ColumnNetTypeArrowTypeValue("DECIMALTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123.1m)),
                        new ColumnNetTypeArrowTypeValue("NUMERICTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123.1m)),
                        new ColumnNetTypeArrowTypeValue("INTTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123m)),
                        new ColumnNetTypeArrowTypeValue("INTEGERTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123m)),
                        new ColumnNetTypeArrowTypeValue("BIGINTTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123m)),
                        new ColumnNetTypeArrowTypeValue("SMALLINTTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123m)),
                        new ColumnNetTypeArrowTypeValue("TINYINTTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123m)),
                        new ColumnNetTypeArrowTypeValue("BYTEINTTYPE", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(123m)),
                        new ColumnNetTypeArrowTypeValue("FLOATTYPE", typeof(double), typeof(DoubleType), 123.45d),
                        new ColumnNetTypeArrowTypeValue("FLOAT4TYPE", typeof(double), typeof(DoubleType), 123.45d),
                        new ColumnNetTypeArrowTypeValue("FLOAT8TYPE", typeof(double), typeof(DoubleType), 123.45d),
                        new ColumnNetTypeArrowTypeValue("DOUBLETYPE", typeof(double), typeof(DoubleType), 123.45d),
                        new ColumnNetTypeArrowTypeValue("DOUBLEPRECISIONTYPE", typeof(double), typeof(DoubleType), 123.45d),
                        new ColumnNetTypeArrowTypeValue("REALTYPE", typeof(double), typeof(DoubleType), 123.45d),
                        new ColumnNetTypeArrowTypeValue("VARCHARTYPE", typeof(string), typeof(StringType), "Hello"),
                        new ColumnNetTypeArrowTypeValue("CHARTYPE", typeof(string), typeof(StringType), "H"),
                        new ColumnNetTypeArrowTypeValue("CHARACTERTYPE", typeof(string), typeof(StringType), "H"),
                        new ColumnNetTypeArrowTypeValue("STRINGTYPE", typeof(string), typeof(StringType), "H"),
                        new ColumnNetTypeArrowTypeValue("TEXTTYPE", typeof(string), typeof(StringType), "Hello World"),
                        new ColumnNetTypeArrowTypeValue("BINARYTYPE", typeof(byte[]), typeof(BinaryType),  Encoding.UTF8.GetBytes("Hello World")),
                        new ColumnNetTypeArrowTypeValue("VARBINARYTYPE", typeof(byte[]), typeof(BinaryType),  Encoding.UTF8.GetBytes("Hello World")),
                        new ColumnNetTypeArrowTypeValue("BOOLEANTYPE", typeof(bool), typeof(BooleanType), true),
                        new ColumnNetTypeArrowTypeValue("DATETYPE", typeof(DateTime), typeof(Date32Type), new DateTime(2023, 7, 28)),
                        new ColumnNetTypeArrowTypeValue("DATETIMETYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28, 12,34,56), TimeSpan.Zero)),
#if NET6_0_OR_GREATER
                        new ColumnNetTypeArrowTypeValue("TIMETYPE", typeof(TimeOnly), typeof(Time64Type), new TimeOnly(12, 34, 56)),
#else
                        new ColumnNetTypeArrowTypeValue("TIMETYPE", typeof(TimeSpan), typeof(Time64Type), new TimeSpan(12, 34, 56)),
#endif
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28,12,34,56), TimeSpan.Zero)),
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPLTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28,19,34,56), TimeSpan.Zero)),
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPNTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28, 12,34,56), TimeSpan.Zero)),
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28, 19,34,56), TimeSpan.Zero)),
                    }
                });

            // null data
            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT " +
                            "CAST(NULL as NUMBER(38,0)) as NUMBERTYPE, " +
                            "CAST(NULL as NUMBER(18,1)) as DECIMALTYPE, " +
                            "CAST(NULL as NUMBER(28,1)) as NUMERICTYPE, " +
                            "CAST(NULL as NUMBER(38,0)) as INTTYPE, " +
                            "CAST(NULL as FLOAT) as FLOATTYPE, " +
                            "CAST(NULL as VARCHAR(16777216)) as VARCHARTYPE, " +
                            "CAST(NULL  as VARCHAR(1)) as CHARTYPE, " +
                            "to_binary(NULL) as BINARYTYPE, " +
                            "CAST(NULL as BOOLEAN) as BOOLEANTYPE, " +
                            "CAST(NULL as DATE) as DATETYPE, " +
                            "CAST(NULL as TIME(9)) as TIMETYPE, " +
                            "CAST(NULL as TIMESTAMP_NTZ(9)) as TIMESTAMPTYPE, " +
                            "CAST(NULL as TIMESTAMP_LTZ(9)) as TIMESTAMPLTZTYPE, " +
                            "CAST(NULL as TIMESTAMP_NTZ(9)) as TIMESTAMPNTZTYPE, " +
                            "CAST(NULL as TIMESTAMP_TZ(9)) as TIMESTAMPTZTYPE",
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                        new ColumnNetTypeArrowTypeValue("NUMBERTYPE", typeof(SqlDecimal), typeof(Decimal128Type), null),
                        new ColumnNetTypeArrowTypeValue("DECIMALTYPE", typeof(SqlDecimal), typeof(Decimal128Type), null),
                        new ColumnNetTypeArrowTypeValue("NUMERICTYPE", typeof(SqlDecimal), typeof(Decimal128Type), null),
                        new ColumnNetTypeArrowTypeValue("INTTYPE", typeof(SqlDecimal), typeof(Decimal128Type), null),
                        new ColumnNetTypeArrowTypeValue("FLOATTYPE", typeof(double), typeof(DoubleType), null),
                        new ColumnNetTypeArrowTypeValue("VARCHARTYPE", typeof(string), typeof(StringType), null),
                        new ColumnNetTypeArrowTypeValue("CHARTYPE", typeof(string), typeof(StringType), null),
                        new ColumnNetTypeArrowTypeValue("BINARYTYPE", typeof(byte[]), typeof(BinaryType),  null),
                        new ColumnNetTypeArrowTypeValue("BOOLEANTYPE", typeof(bool), typeof(BooleanType), null),
                        new ColumnNetTypeArrowTypeValue("DATETYPE", typeof(DateTime), typeof(Date32Type), null),
#if NET6_0_OR_GREATER
                        new ColumnNetTypeArrowTypeValue("TIMETYPE", typeof(TimeOnly), typeof(Time64Type), null),
#else
                        new ColumnNetTypeArrowTypeValue("TIMETYPE", typeof(TimeSpan), typeof(Time64Type), null),
#endif
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPTYPE", typeof(DateTimeOffset), typeof(TimestampType), null),
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPLTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), null),
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPNTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), null),
                        new ColumnNetTypeArrowTypeValue("TIMESTAMPTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), null),
                    }
                });

            // large numbers
            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT  " +
                            "CAST(999999999999999999 as NUMBER(18,0)) as COL18, " +
                            "CAST(9999999999999999999 as NUMBER(19,0)) as COL19, " +
                            "CAST(99999999999999999999 as NUMBER(20,0)) as COL20, " +
                            "CAST(999999999999999999999 as NUMBER(21,0)) as COL21, " +
                            "CAST(9999999999999999999999 as NUMBER(22,0)) as COL22, " +
                            "CAST(99999999999999999999999 as NUMBER(23,0)) as COL23, " +
                            "CAST(999999999999999999999999 as NUMBER(24,0)) as COL24, " +
                            "CAST(9999999999999999999999999 as NUMBER(25,0)) as COL25, " +
                            "CAST(99999999999999999999999999 as NUMBER(26,0)) as COL26, " +
                            "CAST(999999999999999999999999999 as NUMBER(27,0)) as COL27, " +
                            "CAST(9999999999999999999999999999 as NUMBER(28,0)) as COL28, " +
                            "CAST(99999999999999999999999999999 as NUMBER(29,0)) as COL29, " +
                            "CAST(999999999999999999999999999999 as NUMBER(30,0)) as COL30, " +
                            "CAST(9999999999999999999999999999999 as NUMBER(31,0)) as COL31, " +
                            "CAST(99999999999999999999999999999999 as NUMBER(32,0)) as COL32, " +
                            "CAST(999999999999999999999999999999999 as NUMBER(33,0)) as COL33, " +
                            "CAST(9999999999999999999999999999999999 as NUMBER(34,0)) as COL34, " +
                            "CAST(99999999999999999999999999999999999 as NUMBER(35,0)) as COL35, " +
                            "CAST(999999999999999999999999999999999999 as NUMBER(36,0)) as COL36, " +
                            "CAST(9999999999999999999999999999999999999 as NUMBER(37,0)) as COL37, " +
                            "CAST(99999999999999999999999999999999999999 as NUMBER(38,0)) as COL38",
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                        new ColumnNetTypeArrowTypeValue("COL18", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL19", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL20", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL21", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL22", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL23", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL24", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL25", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL26", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL27", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL28", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL29", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL30", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL31", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL32", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL33", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL34", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL35", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL36", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL37", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999999999999999")),
                        new ColumnNetTypeArrowTypeValue("COL38", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999999999999999")),
                    }
                });

            // large with decimal
            sampleDataBuilder.Samples.Add(
                new SampleData()
                {
                    Query = "SELECT  " +
                            "CAST(9999999999999999.99 as NUMBER(18,2)) as COL18, " +
                            "CAST(99999999999999999.99 as NUMBER(19,2)) as COL19, " +
                            "CAST(999999999999999999.99 as NUMBER(20,2)) as COL20, " +
                            "CAST(9999999999999999999.99 as NUMBER(21,2)) as COL21, " +
                            "CAST(99999999999999999999.99 as NUMBER(22,2)) as COL22, " +
                            "CAST(999999999999999999999.99 as NUMBER(23,2)) as COL23, " +
                            "CAST(9999999999999999999999.99 as NUMBER(24,2)) as COL24, " +
                            "CAST(99999999999999999999999.99 as NUMBER(25,2)) as COL25, " +
                            "CAST(999999999999999999999999.99 as NUMBER(26,2)) as COL26, " +
                            "CAST(9999999999999999999999999.99 as NUMBER(27,2)) as COL27, " +
                            "CAST(99999999999999999999999999.99 as NUMBER(28,2)) as COL28, " +
                            "CAST(999999999999999999999999999.99 as NUMBER(29,2)) as COL29, " +
                            "CAST(9999999999999999999999999999.99 as NUMBER(30,2)) as COL30, " +
                            "CAST(99999999999999999999999999999.99 as NUMBER(31,2)) as COL31, " +
                            "CAST(999999999999999999999999999999.99 as NUMBER(32,2)) as COL32, " +
                            "CAST(9999999999999999999999999999999.99 as NUMBER(33,2)) as COL33, " +
                            "CAST(99999999999999999999999999999999.99 as NUMBER(34,2)) as COL34, " +
                            "CAST(999999999999999999999999999999999.99 as NUMBER(35,2)) as COL35, " +
                            "CAST(9999999999999999999999999999999999.99 as NUMBER(36,2)) as COL36, " +
                            "CAST(99999999999999999999999999999999999.99 as NUMBER(37,2)) as COL37, " +
                            "CAST(999999999999999999999999999999999999.99 as NUMBER(38,2)) as COL38",
                    ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                    {
                        new ColumnNetTypeArrowTypeValue("COL18", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL19", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL20", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL21", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL22", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL23", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL24", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL25", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL26", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL27", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL28", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL29", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL30", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL31", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL32", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL33", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL34", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL35", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL36", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("9999999999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL37", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("99999999999999999999999999999999999.99")),
                        new ColumnNetTypeArrowTypeValue("COL38", typeof(SqlDecimal), typeof(Decimal128Type), SqlDecimal.Parse("999999999999999999999999999999999999.99")),
                    }
                });

            return sampleDataBuilder;
        }
    }
}

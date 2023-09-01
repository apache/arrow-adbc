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
using System.Text;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Gets the sample data (based on the first record of the resources/SnowflakeData.sql file)
    /// </summary>
    internal class SampleData
    {
        public static List<ColumnNetTypeArrowTypeValue> GetSampleData()
        {
            List<ColumnNetTypeArrowTypeValue> expectedValues = new List<ColumnNetTypeArrowTypeValue>()
            {
                new ColumnNetTypeArrowTypeValue("NUMBERTYPE", typeof(long), typeof(Int64Array), 1L),
                new ColumnNetTypeArrowTypeValue("DECIMALTYPE", typeof(double), typeof(DoubleArray), 123.1d),
                new ColumnNetTypeArrowTypeValue("NUMERICTYPE", typeof(double), typeof(DoubleArray), 123.1d),
                new ColumnNetTypeArrowTypeValue("INTTYPE", typeof(long), typeof(Int64Array), 123L),
                new ColumnNetTypeArrowTypeValue("INTEGERTYPE", typeof(long), typeof(Int64Array), 123L),
                new ColumnNetTypeArrowTypeValue("BIGINTTYPE", typeof(long), typeof(Int64Array), 123L),
                new ColumnNetTypeArrowTypeValue("SMALLINTTYPE", typeof(long), typeof(Int64Array), 123L),
                new ColumnNetTypeArrowTypeValue("TINYINTTYPE", typeof(long), typeof(Int64Array), 123L),
                new ColumnNetTypeArrowTypeValue("BYTEINTTYPE", typeof(long), typeof(Int64Array), 123L),
                new ColumnNetTypeArrowTypeValue("FLOATTYPE", typeof(double), typeof(DoubleArray), 123.45d),
                new ColumnNetTypeArrowTypeValue("FLOAT4TYPE", typeof(double), typeof(DoubleArray), 123.45d),
                new ColumnNetTypeArrowTypeValue("FLOAT8TYPE", typeof(double), typeof(DoubleArray), 123.45d),
                new ColumnNetTypeArrowTypeValue("DOUBLETYPE", typeof(double), typeof(DoubleArray), 123.45d),
                new ColumnNetTypeArrowTypeValue("DOUBLEPRECISIONTYPE", typeof(double), typeof(DoubleArray), 123.45d),
                new ColumnNetTypeArrowTypeValue("REALTYPE", typeof(double), typeof(DoubleArray), 123.45d),
                new ColumnNetTypeArrowTypeValue("VARCHARTYPE", typeof(string), typeof(StringArray), "Hello"),
                new ColumnNetTypeArrowTypeValue("CHARTYPE", typeof(string), typeof(StringArray), "H"),
                new ColumnNetTypeArrowTypeValue("CHARACTERTYPE", typeof(string), typeof(StringArray), "H"),
                new ColumnNetTypeArrowTypeValue("STRINGTYPE", typeof(string), typeof(StringArray), "H"),
                new ColumnNetTypeArrowTypeValue("TEXTTYPE", typeof(string), typeof(StringArray), "Hello World"),
                new ColumnNetTypeArrowTypeValue("BINARYTYPE", typeof(byte[]), typeof(BinaryArray),  Encoding.UTF8.GetBytes("Hello World")),
                new ColumnNetTypeArrowTypeValue("VARBINARYTYPE", typeof(byte[]), typeof(BinaryArray),  Encoding.UTF8.GetBytes("Hello World")),
                new ColumnNetTypeArrowTypeValue("BOOLEANTYPE", typeof(bool), typeof(BooleanArray), true),
                new ColumnNetTypeArrowTypeValue("DATETYPE", typeof(DateTime), typeof(Date32Array), new DateTime(2023, 7, 28)),
                new ColumnNetTypeArrowTypeValue("DATETIMETYPE", typeof(DateTimeOffset), typeof(TimestampArray), new DateTimeOffset(new DateTime(2023,7,28, 12,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPTYPE", typeof(DateTimeOffset), typeof(TimestampArray), new DateTimeOffset(new DateTime(2023,7,28,12,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPLTZTYPE", typeof(DateTimeOffset), typeof(TimestampArray), new DateTimeOffset(new DateTime(2023,7,28,19,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPNTZTYPE", typeof(DateTimeOffset), typeof(TimestampArray), new DateTimeOffset(new DateTime(2023,7,28, 12,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPTZTYPE", typeof(DateTimeOffset), typeof(TimestampArray), new DateTimeOffset(new DateTime(2023,7,28, 19,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMETYPE", typeof(long), typeof(Time64Array), 45296000000000L)
            };

            return expectedValues;
        }
    }
}

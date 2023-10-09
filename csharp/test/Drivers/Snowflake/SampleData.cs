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
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Gets the sample data (based on the first record of the
    /// resources/SnowflakeData.sql file)
    /// </summary>
    internal class SampleData
    {
        /// <summary>
        /// Sample data returned from TestConfiguration.Query.
        /// </summary>
        public static List<ColumnNetTypeArrowTypeValue> GetSampleData()
        {
            List<ColumnNetTypeArrowTypeValue> expectedValues = new List<ColumnNetTypeArrowTypeValue>()
            {
                // https://github.com/apache/arrow-adbc/issues/1020 has Snowflake treat all values as decimal by default
                new ColumnNetTypeArrowTypeValue("NUMBERTYPE", typeof(decimal), typeof(Decimal128Type), 1m),
                new ColumnNetTypeArrowTypeValue("DECIMALTYPE", typeof(decimal), typeof(Decimal128Type), 1231m),
                new ColumnNetTypeArrowTypeValue("NUMERICTYPE", typeof(decimal), typeof(Decimal128Type), 1231m),
                new ColumnNetTypeArrowTypeValue("INTTYPE", typeof(decimal), typeof(Decimal128Type), 123m),
                new ColumnNetTypeArrowTypeValue("INTEGERTYPE", typeof(decimal), typeof(Decimal128Type), 123m),
                new ColumnNetTypeArrowTypeValue("BIGINTTYPE", typeof(decimal), typeof(Decimal128Type), 123m),
                new ColumnNetTypeArrowTypeValue("SMALLINTTYPE", typeof(decimal), typeof(Decimal128Type), 123m),
                new ColumnNetTypeArrowTypeValue("TINYINTTYPE", typeof(decimal), typeof(Decimal128Type), 123m),
                new ColumnNetTypeArrowTypeValue("BYTEINTTYPE", typeof(decimal), typeof(Decimal128Type), 123m),
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
                new ColumnNetTypeArrowTypeValue("TIMETYPE", typeof(long), typeof(Time64Type), 45296000000000L),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28,12,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPLTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28,19,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPNTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28, 12,34,56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("TIMESTAMPTZTYPE", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2023,7,28, 19,34,56), TimeSpan.Zero)),
            };

            return expectedValues;
        }
    }
}

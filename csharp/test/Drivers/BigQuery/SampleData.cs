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
using System.Net.Http;
using System.Text;
using Apache.Arrow.Types;
using Google.Protobuf.WellKnownTypes;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    /// <summary>
    /// Gets the sample data (based on the first record of the resources/BigQueryData.sql file)
    /// </summary>
    internal class SampleData
    {
        /// <summary>
        /// Represnts the first row of data from resources/BigQueryData.sql
        /// </summary>
        public static List<ColumnNetTypeArrowTypeValue> GetSampleData()
        {
            Int64Array.Builder numbersBuilder = new Int64Array.Builder();
            numbersBuilder.AppendRange(new List<long>() { 1, 2, 3 });
            Int64Array numbersArray = numbersBuilder.Build();

            List<ColumnNetTypeArrowTypeValue> expectedValues = new List<ColumnNetTypeArrowTypeValue>()
            {
                new ColumnNetTypeArrowTypeValue("id", typeof(long), typeof(Int64Array), 1L),
                new ColumnNetTypeArrowTypeValue("number", typeof(double), typeof(FloatArray), 1.23d),
                new ColumnNetTypeArrowTypeValue("decimal", typeof(decimal), typeof(Decimal256Array), decimal.Parse("4.56")),
                new ColumnNetTypeArrowTypeValue("big_decimal", typeof(string), typeof(StringArray), " 789000000000000000000000000000000000000 "),
                new ColumnNetTypeArrowTypeValue("is_active", typeof(bool), typeof(BooleanArray), true),
                new ColumnNetTypeArrowTypeValue("name", typeof(string), typeof(StringArray), "John Doe"),
                new ColumnNetTypeArrowTypeValue("data", typeof(byte[]), typeof(BinaryArray), UTF8Encoding.UTF8.GetBytes("abc123")),
                new ColumnNetTypeArrowTypeValue("date", typeof(DateTime), typeof(Date32Array), new DateTime(2023, 9, 8)),
                new ColumnNetTypeArrowTypeValue("time", typeof(long), typeof(Time64Array), 45296000000L), //'12:34:56'
                new ColumnNetTypeArrowTypeValue("datetime", typeof(DateTimeOffset), typeof(TimestampArray), new DateTimeOffset(new DateTime(2023, 9, 8, 12, 34, 56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("timestamp", typeof(DateTimeOffset), typeof(TimestampArray), new DateTimeOffset(new DateTime(2023, 9, 8, 12, 34, 56), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("point", typeof(string), typeof(StringArray), "POINT(1 2)"),
                new ColumnNetTypeArrowTypeValue("numbers", typeof(long), typeof(Int64Array), numbersArray),
                new ColumnNetTypeArrowTypeValue("person", typeof(string), typeof(StringArray), "{\"name\":\"John Doe\",\"age\":30}")
            };

            return expectedValues;
        }
    }
}

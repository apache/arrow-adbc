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
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    /// <summary>
    /// Validates that specific complex structured types can be inserted, retrieved and targeted correctly
    /// </summary>
    public class ComplexTypesValueTests : SparkTestBase
    {
        public ComplexTypesValueTests(ITestOutputHelper output) : base(output) { }

        public static IEnumerable<object[]> IntArrayData(int size)
        {
            var rnd = new Random();
            int[] ints = new int[size];
            for (int i = 0; i < ints.Length; i++)
            {
                ints[i] = rnd.Next();
            }
            yield return new object[] { ints };
        }

        public static IEnumerable<object[]> LongArrayData(int size)
        {
            var rnd = new Random();
            long[] ints = new long[size];
            for (int i = 0; i < ints.Length; i++)
            {
                ints[i] = rnd.Next();
            }
            yield return new object[] { ints };
        }

        /// <summary>
        /// Validates if driver can send and receive specific array of integer values correctly.
        /// </summary>
        [SkippableFact]
        //[MemberData(nameof(IntArrayData), 0)]
        //[MemberData(nameof(LongArrayData), 2)]
        //[MemberData(nameof(IntArrayData), 1024)]
        public async Task TestArrayOfIntData()
        {
            //string columnName = "INTARRAYTYPE";
            //using TemporaryTable table = NewTemporaryTable(Statement, string.Format("{0} {1}", columnName, "ARRAY<BIGINT>"));
            //string formattedValue = $"ARRAY({string.Join(",", value)})";
            //await ValidateInsertSelectDeleteSingleValue(
            //    table.TableName,
            //    columnName,
            //    value,
            //    formattedValue);
            string selectStatement = "SELECT ARRAY(CAST(1 as INT), 2, 3);";
            await SelectAndValidateValues(selectStatement, new[] { 1, 2, 3 }, 1);
        }

        /// <summary>
        /// Validates if driver can send and receive specific map values correctly.
        /// </summary>
        [SkippableFact]
        public async Task TestMapData()
        {
            //string columnName = "STRUCTTYPE";
            //using TemporaryTable table = NewTemporaryTable(Statement, string.Format("{0} {1}", columnName, "MAP < INT, STRING >"));
            //// This works for INSERT but need a different syntax for SELECT and DELETE
            //string formattedValue = $"MAP(30, \'John Doe\')";
            //InsertSingleValue(table.TableName, columnName, formattedValue);
            //string whereClause = GetDeleteFromWhereClause(columnName, value, formattedValue);
            //DeleteFromTable(tableName, whereClause, 1);
            //await ValidateInsertSelectDeleteSingleValue(
            //    table.TableName,
            //    columnName,
            //    "",
            //    formattedValue);
            string selectStatement = "SELECT map(1, 'John Doe', 2, 'Jane Doe', 3, 'Jack Doe');";
            await SelectAndValidateValues(selectStatement, "", 1);
        }
    }
}

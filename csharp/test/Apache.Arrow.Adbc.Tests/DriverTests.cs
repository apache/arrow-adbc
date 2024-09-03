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

using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Performs tests related to connecting with ADBC drivers.
    /// </summary>
    public class DriverTests
    {
        /// <summary>
        /// Validates that a <see cref="QueryResult"/> contains a number
        /// of records.
        /// </summary>
        /// <param name="queryResult">
        /// The query result.
        /// </param>
        /// <param name="expectedNumberOfResults">
        /// The number of records.
        /// </param>
        public static void CanExecuteQuery(QueryResult queryResult, long expectedNumberOfResults)
        {
            long count = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch nextBatch = queryResult.Stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                count += nextBatch.Length;
            }

            Assert.True(expectedNumberOfResults == count, $"The parsed records ({count}) differ from the expected amount ({expectedNumberOfResults})");

            // if the values were set, make sure they are correct
            if (queryResult.RowCount != -1)
            {
                Assert.True(queryResult.RowCount == expectedNumberOfResults, "The RowCount value does not match the expected results");

                Assert.True(queryResult.RowCount == count, "The RowCount value does not match the counted records");
            }
        }

        /// <summary>
        /// Validates that a <see cref="QueryResult"/> contains a number
        /// of records.
        /// </summary>
        /// <param name="queryResult">
        /// The query result.
        /// </param>
        /// <param name="expectedNumberOfResults">
        /// The number of records.
        /// </param>
        public static async Task CanExecuteQueryAsync(QueryResult queryResult, long expectedNumberOfResults)
        {
            long count = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch nextBatch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (nextBatch == null) { break; }
                count += nextBatch.Length;
            }

            Assert.True(expectedNumberOfResults == count, $"The parsed records ({count}) differ from the expected amount ({expectedNumberOfResults})");

            // if the values were set, make sure they are correct
            if (queryResult.RowCount != -1)
            {
                Assert.True(queryResult.RowCount == expectedNumberOfResults, "The RowCount value does not match the expected results");

                Assert.True(queryResult.RowCount == count, "The RowCount value does not match the counted records");
            }
        }
    }
}

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
        /// <param name="environmentName">
        /// Name of the test environment.
        /// </param>
        public static void CanExecuteQuery(
            QueryResult queryResult,
            long expectedNumberOfResults,
            string? environmentName = null)
        {
            long count = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch nextBatch = queryResult.Stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                count += nextBatch.Length;
            }

            Assert.True(expectedNumberOfResults == count, Utils.FormatMessage($"The parsed records ({count}) differ from the expected amount ({expectedNumberOfResults})", environmentName));

            // if the values were set, make sure they are correct
            if (queryResult.RowCount != -1)
            {
                Assert.True(queryResult.RowCount == expectedNumberOfResults, Utils.FormatMessage("The RowCount value does not match the expected results", environmentName));

                Assert.True(queryResult.RowCount == count, Utils.FormatMessage("The RowCount value does not match the counted records", environmentName));
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
        /// <param name="environmentName">
        /// Name of the test environment.
        /// </param>
        public static async Task CanExecuteQueryAsync(
            QueryResult queryResult,
            long expectedNumberOfResults,
            string? environmentName = null)
        {
            long count = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch nextBatch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (expectedNumberOfResults == 0)
                {
                    Assert.Null(nextBatch);
                }
                if (nextBatch == null) { break; }
                count += nextBatch.Length;
            }

            Assert.True(expectedNumberOfResults == count, Utils.FormatMessage($"The parsed records ({count}) differ from the expected amount ({expectedNumberOfResults})", environmentName));

            // if the values were set, make sure they are correct
            if (queryResult.RowCount != -1)
            {
                Assert.True(queryResult.RowCount == expectedNumberOfResults, Utils.FormatMessage("The RowCount value does not match the expected results", environmentName));

                Assert.True(queryResult.RowCount == count, Utils.FormatMessage("The RowCount value does not match the counted records", environmentName));
            }
        }
    }
}

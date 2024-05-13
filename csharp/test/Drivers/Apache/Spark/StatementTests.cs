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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class StatementTests : SparkTestBase
    {
        private static List<string> DefaultTableTypes => new() { "BASE TABLE", "VIEW" };

        public StatementTests(ITestOutputHelper? outputHelper) : base(outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Validates if the SetOption handle valid/invalid data correctly for the PollTime option.
        /// </summary>
        [SkippableTheory]
        [InlineData("-1", true)]
        [InlineData("zero", true)]
        [InlineData("-2147483648", true)]
        [InlineData("2147483648", true)]
        [InlineData("0")]
        [InlineData("1")]
        [InlineData("2147483647")]
        public void CanSetOptionPollTime(string value, bool throws = false)
        {
            AdbcStatement statement = NewConnection().CreateStatement();
            if (throws)
            {
                Assert.Throws<ArgumentException>(() => statement.SetOption(SparkStatement.Options.PollTimeMilliseconds, value));
            }
            else
            {
                statement.SetOption(SparkStatement.Options.PollTimeMilliseconds, value);
            }
        }

        /// <summary>
        /// Validates if the SetOption handle valid/invalid data correctly for the BatchSize option.
        /// </summary>
        [SkippableTheory]
        [InlineData("-1", true)]
        [InlineData("one", true)]
        [InlineData("-2147483648", true)]
        [InlineData("2147483648", true)]
        [InlineData("0", true)]
        [InlineData("1")]
        [InlineData("2147483647")]
        public void CanSetOptionBatchSize(string value, bool throws = false)
        {
            AdbcStatement statement = NewConnection().CreateStatement();
            if (throws)
            {
                Assert.Throws<ArgumentException>(() => statement.SetOption(SparkStatement.Options.BatchSize, value));
            }
            else
            {
                statement.SetOption(SparkStatement.Options.BatchSize, value);
            }
        }

        /// <summary>
        /// Validates if the driver can execute update statements.
        /// </summary>
        [SkippableFact, Order(1)]
        public async Task CanInteractUsingSetOptions()
        {
            const string columnName = "INDEX";
            Statement.SetOption(SparkStatement.Options.PollTimeMilliseconds, "100");
            Statement.SetOption(SparkStatement.Options.BatchSize, "10");
            using TemporaryTable temporaryTable = await NewTemporaryTableAsync(Statement, $"{columnName} INT");
            await ValidateInsertSelectDeleteSingleValueAsync(temporaryTable.TableName, columnName, 1);
        }
    }
}

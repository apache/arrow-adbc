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
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public abstract class StatementTests<TConfig, TEnv> : TestBase<TConfig, TEnv>
        where TConfig : ApacheTestConfiguration
        where TEnv : HiveServer2TestEnvironment<TConfig>
    {
        private static List<string> DefaultTableTypes => ["TABLE", "VIEW"];

        public StatementTests(ITestOutputHelper? outputHelper, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
            : base(outputHelper, testEnvFactory)
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
            var testConfiguration = TestConfiguration.Clone() as TConfig;
            testConfiguration!.PollTimeMilliseconds = value;
            if (throws)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => NewConnection(testConfiguration).CreateStatement());
            }

            AdbcStatement statement = NewConnection().CreateStatement();
            if (throws)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => statement.SetOption(ApacheParameters.PollTimeMilliseconds, value));
            }
            else
            {
                statement.SetOption(ApacheParameters.PollTimeMilliseconds, value);
            }
        }

        /// <summary>
        /// Validates if the SetOption handle valid/invalid data correctly for the BatchSize option.
        /// </summary>
        [SkippableTheory]
        [InlineData("-1", true)]
        [InlineData("one", true)]
        [InlineData("-2147483648", true)]
        [InlineData("2147483648", false)]
        [InlineData("9223372036854775807", false)]
        [InlineData("9223372036854775808", true)]
        [InlineData("0", true)]
        [InlineData("1")]
        [InlineData("2147483647")]
        public void CanSetOptionBatchSize(string value, bool throws = false)
        {
            var testConfiguration = TestConfiguration.Clone() as TConfig;
            testConfiguration!.BatchSize = value;
            if (throws)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => NewConnection(testConfiguration).CreateStatement());
            }

            AdbcStatement statement = NewConnection().CreateStatement();
            if (throws)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => statement!.SetOption(ApacheParameters.BatchSize, value));
            }
            else
            {
                statement.SetOption(ApacheParameters.BatchSize, value);
            }
        }

        /// <summary>
        /// Validates if the SetOption handle valid/invalid data correctly for the QueryTimeout option.
        /// </summary>
        [SkippableTheory]
        [InlineData("zero", true)]
        [InlineData("-2147483648", true)]
        [InlineData("2147483648", true)]
        [InlineData("0", false)]
        [InlineData("-1", true)]
        [InlineData("1")]
        [InlineData("2147483647")]
        public void CanSetOptionQueryTimeout(string value, bool throws = false)
        {
            var testConfiguration = TestConfiguration.Clone() as TConfig;
            testConfiguration!.QueryTimeoutSeconds = value;
            if (throws)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => NewConnection(testConfiguration).CreateStatement());
            }

            AdbcStatement statement = NewConnection().CreateStatement();
            if (throws)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => statement.SetOption(ApacheParameters.QueryTimeoutSeconds, value));
            }
            else
            {
                statement.SetOption(ApacheParameters.QueryTimeoutSeconds, value);
            }
        }

        /// <summary>
        /// Queries the backend with various timeouts.
        /// </summary>
        /// <param name="statementWithExceptions"></param>
        [SkippableTheory]
        [ClassData(typeof(StatementTimeoutTestData))]
        internal void StatementTimeoutTest(StatementWithExceptions statementWithExceptions)
        {
            TConfig testConfiguration = (TConfig)TestConfiguration.Clone();

            if (statementWithExceptions.QueryTimeoutSeconds.HasValue)
                testConfiguration.QueryTimeoutSeconds = statementWithExceptions.QueryTimeoutSeconds.Value.ToString();

            if (!string.IsNullOrEmpty(statementWithExceptions.Query))
                testConfiguration.Query = statementWithExceptions.Query!;

            OutputHelper?.WriteLine($"QueryTimeoutSeconds: {testConfiguration.QueryTimeoutSeconds}. ShouldSucceed: {statementWithExceptions.ExceptionType == null}. Query: [{testConfiguration.Query}]");

            try
            {
                AdbcStatement st = NewConnection(testConfiguration).CreateStatement();
                st.SqlQuery = testConfiguration.Query;
                QueryResult qr = st.ExecuteQuery();

                OutputHelper?.WriteLine($"QueryResultRowCount: {qr.RowCount}");
            }
            catch (Exception ex) when (ApacheUtility.ContainsException(ex, statementWithExceptions.ExceptionType, out Exception? containedException))
            {
                Assert.IsType(statementWithExceptions.ExceptionType!, containedException!);
            }
        }

        /// <summary>
        /// Validates if the driver can execute update statements.
        /// </summary>
        [SkippableFact, Order(1)]
        public async Task CanInteractUsingSetOptions()
        {
            const string columnName = "INDEX";
            Statement.SetOption(ApacheParameters.PollTimeMilliseconds, "100");
            Statement.SetOption(ApacheParameters.BatchSize, "10");
            using TemporaryTable temporaryTable = await NewTemporaryTableAsync(Statement, $"{columnName} INT");
            await ValidateInsertSelectDeleteSingleValueAsync(temporaryTable.TableName, columnName, 1);
        }
    }

    /// <summary>
    /// Data type used for metadata timeout tests.
    /// </summary>
    internal class StatementWithExceptions
    {
        public StatementWithExceptions(int? queryTimeoutSeconds, string? query, Type? exceptionType)
        {
            QueryTimeoutSeconds = queryTimeoutSeconds;
            Query = query;
            ExceptionType = exceptionType;
        }

        /// <summary>
        /// If null, uses the default timeout.
        /// </summary>
        public int? QueryTimeoutSeconds { get; }

        /// <summary>
        /// If null, expected to succeed.
        /// </summary>
        public Type? ExceptionType { get; }

        /// <summary>
        /// If null, uses the default TestConfiguration.
        /// </summary>
        public string? Query { get; }
    }

    /// <summary>
    /// Collection of <see cref="StatementWithExceptions"/> for testing statement timeouts."/>
    /// </summary>
    internal class StatementTimeoutTestData : TheoryData<StatementWithExceptions>
    {
        public StatementTimeoutTestData()
        {
            string longRunningQuery = "SELECT COUNT(*) AS total_count\nFROM (\n  SELECT t1.id AS id1, t2.id AS id2\n  FROM RANGE(1000000) t1\n  CROSS JOIN RANGE(10000) t2\n) subquery\nWHERE MOD(id1 + id2, 2) = 0";

            Add(new(0, null, null));
            Add(new(null, null, null));
            Add(new(1, null, typeof(TimeoutException)));
            Add(new(5, null, null));
            Add(new(30, null, null));
            Add(new(5, longRunningQuery, typeof(TimeoutException)));
            Add(new(null, longRunningQuery, typeof(TimeoutException)));
            Add(new(0, longRunningQuery, null));
        }
    }
}

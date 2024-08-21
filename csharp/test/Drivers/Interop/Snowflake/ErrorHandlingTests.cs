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
using System.IO;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    public class ErrorHandlingTests : IDisposable
    {
        private const string TestTablePrefix = "ADBCERRORSTEST_"; // Make configurable? Also; must be all caps if not double quoted
        private const string DefaultColumnName = "COLUMN_NAME";
        private const string DefaultColumnNameLower = "column_name";
        readonly SnowflakeTestConfiguration _snowflakeTestConfiguration;
        readonly AdbcConnection _connection;
        readonly AdbcStatement _statement;
        readonly string _catalogSchema;
        private readonly ITestOutputHelper _output;
        private bool _disposedValue = false;

        /// <summary>
        /// Validates that specific errors are generated as expected.
        /// </summary>
        public ErrorHandlingTests(ITestOutputHelper output)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
            _snowflakeTestConfiguration = SnowflakeTestingUtils.TestConfiguration;
            Dictionary<string, string> options = new();
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_snowflakeTestConfiguration, out Dictionary<string, string> parameters);
            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            _connection = adbcDatabase.Connect(options);
            _statement = _connection.CreateStatement();
            _catalogSchema = string.Format("{0}.{1}", _snowflakeTestConfiguration.Metadata?.Catalog, _snowflakeTestConfiguration.Metadata?.Schema);
            _output = output;
        }

        /// <summary>
        /// Tests for invalid table and column names
        /// </summary>
        [SkippableTheory]
        [InlineData("NUMERIC", "0", "INVALID_TABLE_NAME", null, new[] { "002003", "42S02" })]
        [InlineData("NUMERIC", "0", null, "INVALID_" + DefaultColumnName, new[] { "000904", "42000" })]
        [InlineData("NUMERIC", "0", null, "\"" + DefaultColumnNameLower + "\"", new[] { "000904", "42000" })]
        public void TestInvalidObjectName(string columnSpecification, string sourceValue, string? overrideTableName, string? overrideColumnName, string[]? expectedExceptionMessage = null)
        {
            InitializeTest(columnSpecification, sourceValue, out string columnName, out string tableName);
            SelectAndValidateException(overrideTableName ?? tableName, overrideColumnName ?? columnName, typeof(AdbcException), expectedExceptionMessage);
        }

        /// <summary>
        /// Tests for invalid syntax.
        /// </summary>
        [SkippableTheory]
        [InlineData("NUMERIC", "0", null, DefaultColumnName + ",", new[] { "001003", "42000" })]
        [InlineData("NUMERIC", "0", null, "," + DefaultColumnName, new[] { "001003", "42000" })]
        [InlineData("NUMERIC", "0", null, "'" + DefaultColumnName, new[] { "001003", "42000" })]
        [InlineData("NUMERIC", "0", null, DefaultColumnName + "'", new[] { "001003", "42000" })]
        [InlineData("NUMERIC", "0", null, "\"" + DefaultColumnName, new[] { "001003", "42000" })]
        [InlineData("NUMERIC", "0", null, DefaultColumnName + "\"", new[] { "001003", "42000" })]
        public void TestInvalidSyntax(string columnSpecification, string sourceValue, string? overrideTableName, string overrideColumnName, string[]? expectedExceptionMessage = null)
        {
            InitializeTest(columnSpecification, sourceValue, out string columnName, out string tableName);
            SelectAndValidateException(overrideTableName ?? tableName, overrideColumnName ?? columnName, typeof(AdbcException), expectedExceptionMessage);
        }

        /// <summary>
        /// Tests for invalid driver path
        /// </summary>
        [SkippableFact]
        public void TestDriverLoadInvalidPath()
        {
            Dictionary<string, string> parameters = new();
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            string driverFilename = Guid.NewGuid().ToString();
            testConfiguration.DriverPath = Path.Combine(Environment.CurrentDirectory, driverFilename + ".dll");
            Exception actualException = Assert.Throws<ArgumentException>(() => SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters));
            SnowflakeTestingUtils.AssertContainsAll(new[] { "file does not exist (Parameter 'file')" }, actualException.Message);
        }

        /// <summary>
        /// Tests for invalid driver entry point
        /// </summary>
        [SkippableFact]
        public void TestDriverLoadInvalidEntryPoint()
        {
            Dictionary<string, string> parameters = new();
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            string entryPoint = Guid.NewGuid().ToString();
            testConfiguration.DriverEntryPoint = entryPoint;
            Exception actualException = Assert.Throws<EntryPointNotFoundException>(() => SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters));
            SnowflakeTestingUtils.AssertContainsAll(new[] { "Unable to find an entry point named", entryPoint }, actualException.Message);
        }

        /// <summary>
        /// Tests for various invalid connection properties.
        /// </summary>
        /// <param name="test">A test for an invalid configuration checking the exception type and keywords in the error message.</param>
        [SkippableTheory]
        [MemberData(nameof(GenerateTestConfigurationData))]
        public void TestDriverConnectionInvalidConnection(SnowflakeTestConfigurationTest test)
        {
            Dictionary<string, string> options = new();
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(
                (SnowflakeTestConfiguration)test.TestConfiguration,
                out Dictionary<string, string> parameters);
            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            Exception actualException = Assert.Throws(test.ExceptionType, () => adbcDatabase.Connect(options));
            SnowflakeTestingUtils.AssertContainsAll(test.ExceptionMessageComponents, actualException.Message);
        }

        /// <summary>
        /// Generates test configuration for testing invalid properties in the connection.
        /// </summary>
        /// <returns>The list of tests.</returns>
        public static IEnumerable<object[]> GenerateTestConfigurationData()
        {
            string property = Guid.NewGuid().ToString();
            SnowflakeTestConfiguration testConfiguration;

            // Note: Providing an invalid Host will hang the test.
            //testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            //testConfiguration.Host = property;
            //yield return new object[] { new SnowflakeTestConfigurationTest(testConfiguration, typeof(AdbcException), ["Unknown"]) };

            testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Authentication!.Default!.User = property;
            yield return new object[] { new SnowflakeTestConfigurationTest(testConfiguration, typeof(AdbcException), new[] { "[Snowflake] 390100 (08004)", "Incorrect username or password was specified" }) };

            testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Authentication!.Default!.Password = property;
            yield return new object[] { new SnowflakeTestConfigurationTest(testConfiguration, typeof(AdbcException), new[] { "[Snowflake] 390100 (08004)", "Incorrect username or password was specified" }) };

            testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Database = property;
            yield return new object[] { new SnowflakeTestConfigurationTest(testConfiguration, typeof(AdbcException), new[] { "[Snowflake] 390201 (08004)", "The requested database does not exist or not authorized." }) };

            testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Warehouse = property;
            yield return new object[] { new SnowflakeTestConfigurationTest(testConfiguration, typeof(AdbcException), new[] { "[Snowflake] 390201 (08004)", "The requested warehouse does not exist or not authorized." }) };
        }

        /// <summary>
        /// A test.
        /// </summary>
        public readonly struct SnowflakeTestConfigurationTest
        {
            public TestConfiguration TestConfiguration { get; }

            public Type ExceptionType { get; }

            public string[] ExceptionMessageComponents { get; }

            /// <param name="testConfiguration">A test configuration.</param>
            /// <param name="exceptionType">Expected exception type.</param>
            /// <param name="exceptionMessageComponents">Expected keywords/phrases to be found in exception message.</param>
            public SnowflakeTestConfigurationTest(
                TestConfiguration testConfiguration,
                Type exceptionType,
                string[] exceptionMessageComponents)
            {
                TestConfiguration = testConfiguration;
                ExceptionType = exceptionType;
                ExceptionMessageComponents = exceptionMessageComponents;
            }
        }

        private void SelectAndValidateException(
            string table,
            string projection,
            Type expectedExceptionType,
            string[]? expectedExceptionTextContains = null)
        {
            Exception actualException = Assert.Throws(expectedExceptionType, () => PerformQuery(table, projection));
            SnowflakeTestingUtils.AssertContainsAll(expectedExceptionTextContains, actualException.Message);
        }

        private QueryResult PerformQuery(string table, string projection)
        {
            string selectStatement = string.Format(
                "SELECT {0} AS CASTRESULT FROM {1};",
                projection,
                table);
            _output.WriteLine(selectStatement);
            _statement.SqlQuery = selectStatement;
            return _statement.ExecuteQuery();
        }

        private void InsertSingleValue(string table, string columnName, string value)
        {
            string insertNumberStatement = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", table, columnName, value);
            Console.WriteLine(insertNumberStatement);
            _statement.SqlQuery = insertNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(1, updateResult.AffectedRows);
        }

        private void InitializeTest(
            string columnSpecification,
            string sourceValue,
            out string columnName,
            out string tableName,
            bool useSelectSyntax = false)
        {
            columnName = DefaultColumnName;
            tableName = CreateTemporaryTable(
                _statement,
                TestTablePrefix,
                _catalogSchema,
                string.Format("{0} {1}", columnName, columnSpecification));
            if (useSelectSyntax)
            {
                InsertIntoFromSelect(tableName, columnName, sourceValue);
            }
            else
            {
                InsertSingleValue(tableName, columnName, sourceValue);
            }
        }

        private static string CreateTemporaryTable(
            AdbcStatement statement,
            string testTablePrefix,
            string catalogSchema,
            string columns)
        {
            string tableName = string.Format("{0}.{1}{2}", catalogSchema, testTablePrefix, Guid.NewGuid().ToString().Replace("-", ""));
            string createTableStatement = string.Format("CREATE TEMPORARY TABLE {0} ({1})", tableName, columns);
            statement.SqlQuery = createTableStatement;
            statement.ExecuteUpdate();
            return tableName;
        }

        private void InsertIntoFromSelect(string table, string columnName, string selectQuery, long expectedAffectedRows = 1)
        {
            string insertStatement = string.Format("INSERT INTO {0} ({1}) {2};", table, columnName, selectQuery);
            _output.WriteLine(insertStatement);
            _statement.SqlQuery = insertStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(expectedAffectedRows, updateResult.AffectedRows);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {

                    _connection?.Dispose();
                    _statement?.Dispose();
                }

                _disposedValue = true;
            }
        }
    }
}

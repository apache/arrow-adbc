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
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Types;
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
        where TEnv : CommonTestEnvironment<TConfig>
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
        internal virtual void StatementTimeoutTest(StatementWithExceptions statementWithExceptions)
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

        [SkippableFact]
        public async Task CanGetCatalogs()
        {
            var statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "GetCatalogs";

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            Assert.Single(queryResult.Stream.Schema.FieldsList);
            int actualBatchLength = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }
                actualBatchLength += batch.Length;
            }
            if (TestEnvironment.SupportCatalogName)
            {
                Assert.True(actualBatchLength > 0);
            }
            else
            {
                Assert.True(actualBatchLength == 0);
            }
        }

        [SkippableFact]
        public async Task CanGetSchemas()
        {
            var statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, TestConfiguration.Metadata.Catalog);
            statement.SetOption(ApacheParameters.SchemaName, TestConfiguration.Metadata.Schema);
            statement.SqlQuery = "GetSchemas";

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            Assert.Equal(2, queryResult.Stream.Schema.FieldsList.Count);
            int actualBatchLength = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }
                actualBatchLength += batch.Length;
            }
            Assert.Equal(1, actualBatchLength);
        }

        [SkippableFact]
        public async Task CanGetTables()
        {
            var statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, TestConfiguration.Metadata.Catalog);
            statement.SetOption(ApacheParameters.SchemaName, TestConfiguration.Metadata.Schema);
            statement.SetOption(ApacheParameters.TableName, TestConfiguration.Metadata.Table);
            statement.SqlQuery = "GetTables";

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            Assert.True(queryResult.Stream.Schema.FieldsList.Count >= 5);
            int actualBatchLength = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }
                actualBatchLength += batch.Length;
            }
            Assert.Equal(1, actualBatchLength);
        }

        [SkippableFact]
        public async Task CanGetColumns()
        {
            var statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, TestConfiguration.Metadata.Catalog);
            statement.SetOption(ApacheParameters.SchemaName, TestConfiguration.Metadata.Schema);
            statement.SetOption(ApacheParameters.TableName, TestConfiguration.Metadata.Table);
            statement.SqlQuery = "GetColumns";

            QueryResult queryResult = await statement.ExecuteQueryAsync();
            Assert.NotNull(queryResult.Stream);

            // 23 original metadata columns and one added for "base type"
            Assert.Equal(24, queryResult.Stream.Schema.FieldsList.Count);
            int actualBatchLength = 0;

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }
                actualBatchLength += batch.Length;
            }
            Assert.Equal(TestConfiguration.Metadata.ExpectedColumnCount, actualBatchLength);
        }

        /// <summary>
        /// Validates if the driver can execute GetPrimaryKeys metadata command.
        /// </summary>
        protected async Task CanGetPrimaryKeys(string? catalogName, string? schemaName)
        {
            PrepareCreateTableWithPrimaryKeys(
                out string sqlUpdate,
                out string tableName,
                out string fullTableName,
                out IReadOnlyList<string> primaryKeys);
            using TemporaryTable temporaryTable = await TemporaryTable.NewTemporaryTableAsync(
                    Statement,
                    fullTableName,
                    sqlUpdate,
                    OutputHelper);

            // Note: create a new statement to do metadata calls so it does not reuse the existing 'this.Statement'
            AdbcStatement statement = Connection.CreateStatement();
            statement.SetOption(ApacheParameters.CatalogName, catalogName ?? string.Empty);
            statement.SetOption(ApacheParameters.SchemaName, schemaName ?? string.Empty);
            statement.SetOption(ApacheParameters.TableName, tableName);
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "GetPrimaryKeys";

            await ValidateGetPrimaryKeys(catalogName, schemaName, tableName, primaryKeys, statement);
        }

        /// <summary>
        /// Validates if the driver can execute GetCrossReference metadata command referencing the child table.
        /// </summary>
        protected async Task CanGetCrossReferenceFromChildTable(string? catalogName, string? schemaName)
        {
            PrepareCreateTableWithPrimaryKeys(
                out string sqlUpdate,
                out string tableNameParent,
                out string fullTableNameParent,
                out IReadOnlyList<string> primaryKeys);
            using TemporaryTable temporaryTableParent = await TemporaryTable.NewTemporaryTableAsync(
                Statement,
                fullTableNameParent,
                sqlUpdate,
                OutputHelper);

            PrepareCreateTableWithForeignKeys(
                fullTableNameParent,
                out sqlUpdate,
                out string tableNameChild,
                out string fullTableNameChild,
                out IReadOnlyList<string> foreignKeys);
            using TemporaryTable temporaryTableChild = await TemporaryTable.NewTemporaryTableAsync(
                Statement,
                fullTableNameChild,
                sqlUpdate,
                OutputHelper);

            // Note: create a new statement to do metadata calls so it does not reuse the existing 'this.Statement'
            AdbcStatement statement = Connection.CreateStatement();
            // Either or both the parent and child namespace can be provided
            statement.SetOption(ApacheParameters.ForeignCatalogName, catalogName ?? string.Empty);
            statement.SetOption(ApacheParameters.ForeignSchemaName, schemaName ?? string.Empty);
            statement.SetOption(ApacheParameters.ForeignTableName, tableNameChild);
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "GetCrossReference";

            await ValidateGetCrossReference(catalogName, schemaName, tableNameParent, primaryKeys, foreignKeys, statement);
        }

        /// <summary>
        /// Validates if the driver can execute GetCrossReference metadata command referencing the parent table.
        /// </summary>
        protected async Task CanGetCrossReferenceFromParentTable(string? catalogName, string? schemaName)
        {
            PrepareCreateTableWithPrimaryKeys(
                out string sqlUpdate,
                out string tableNameParent,
                out string fullTableNameParent,
                out IReadOnlyList<string> primaryKeys);
            using TemporaryTable temporaryTableParent = await TemporaryTable.NewTemporaryTableAsync(
                Statement,
                fullTableNameParent,
                sqlUpdate,
                OutputHelper);

            PrepareCreateTableWithForeignKeys(
                fullTableNameParent,
                out sqlUpdate,
                out string tableNameChild,
                out string fullTableNameChild,
                out IReadOnlyList<string> foreignKeys);
            using TemporaryTable temporaryTableChild = await TemporaryTable.NewTemporaryTableAsync(
                Statement,
                fullTableNameChild,
                sqlUpdate,
                OutputHelper);

            // Note: create a new statement to do metadata calls so it does not reuse the existing 'this.Statement'
            AdbcStatement statement = Connection.CreateStatement();
            // Either or both the parent and child namespace can be provided
            statement.SetOption(ApacheParameters.CatalogName, catalogName ?? string.Empty);
            statement.SetOption(ApacheParameters.SchemaName, schemaName ?? string.Empty);
            statement.SetOption(ApacheParameters.TableName, tableNameParent);
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "GetCrossReference";

            await ValidateGetCrossReference(catalogName, schemaName, tableNameParent, primaryKeys, foreignKeys, statement);
        }

        protected virtual void PrepareCreateTableWithForeignKeys(string fullTableNameParent, out string sqlUpdate, out string tableNameChild, out string fullTableNameChild, out IReadOnlyList<string> foreignKeys)
        {
            CreateNewTableName(out tableNameChild, out fullTableNameChild);
            sqlUpdate = $"CREATE TABLE IF NOT EXISTS {fullTableNameChild} \n"
                + "  (INDEX INT, USERINDEX INT, USERNAME STRING, ADDRESS STRING, \n"
                + "  PRIMARY KEY (INDEX) disable novalidate, \n"
                + $"  FOREIGN KEY (USERINDEX, USERNAME) REFERENCES {fullTableNameParent} (INDEX, NAME) disable novalidate)";
            foreignKeys = ["userindex", "username"];
        }

        protected virtual void PrepareCreateTableWithPrimaryKeys(out string sqlUpdate, out string tableNameParent, out string fullTableNameParent, out IReadOnlyList<string> primaryKeys)
        {
            CreateNewTableName(out tableNameParent, out fullTableNameParent);
            sqlUpdate = $"CREATE TABLE IF NOT EXISTS {fullTableNameParent} (INDEX INT, NAME STRING, PRIMARY KEY (INDEX, NAME) disable novalidate)";
            primaryKeys = ["index", "name"];
        }

        private static async Task ValidateGetPrimaryKeys(string? catalogName, string? schemaName, string tableName, IReadOnlyList<string> primaryKeys, AdbcStatement statement)
        {
            int expectedBatchLength = primaryKeys.Count;
            int actualBatchLength = 0;

            QueryResult queryResult = statement.ExecuteQuery();
            Assert.NotNull(queryResult.Stream);

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }

                Assert.Equal(6, batch.ColumnCount);
                Assert.Equal(6, queryResult.Stream.Schema.FieldsList.Count);
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[0].DataType);
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[1].DataType);
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[2].DataType);
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[3].DataType);
                Assert.Equal(Int32Type.Default, queryResult.Stream.Schema.FieldsList[4].DataType);
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[5].DataType);
                Assert.Equal(expectedBatchLength, batch.Length);
                actualBatchLength += batch.Length;
                for (int i = 0; i < batch.Length; i++)
                {
                    string? catalogNameActual = ((StringArray)batch.Column(0)).GetString(i);
                    Assert.True(string.Equals(catalogName, catalogNameActual)
                        || string.IsNullOrEmpty(catalogName) && string.IsNullOrEmpty(catalogNameActual));
                    string schemaNameActual = ((StringArray)batch.Column(1)).GetString(i);
                    Assert.Equal(schemaName, schemaNameActual);
                    string tableNameActual = ((StringArray)batch.Column(2)).GetString(i);
                    Assert.Equal(tableName, tableNameActual);

                    string? columnName = ((StringArray)batch.Column(3)).GetString(i)?.ToLowerInvariant();
                    int? keyIndex = ((Int32Array)batch.Column(4)).GetValue(i);
                    Assert.True(keyIndex <= primaryKeys.Count);
                    Assert.True(keyIndex.HasValue);
                    Assert.Equal(primaryKeys[keyIndex.Value - 1], columnName);
                }
            }

            Assert.Equal(expectedBatchLength, actualBatchLength);
        }

        private static async Task ValidateGetCrossReference(string? catalogName, string? schemaName, string tableNameParent, IReadOnlyList<string> primaryKeys, IReadOnlyList<string> foreignKeys, AdbcStatement statement)
        {
            int expectedBatchLength = primaryKeys.Count;
            int actualBatchLength = 0;

            QueryResult queryResult = statement.ExecuteQuery();
            Assert.NotNull(queryResult.Stream);

            while (queryResult.Stream != null)
            {
                RecordBatch? batch = await queryResult.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    break;
                }

                int expectedColumnCount = 14;
                Assert.Equal(expectedColumnCount, batch.ColumnCount);
                Assert.Equal(expectedColumnCount, queryResult.Stream.Schema.FieldsList.Count);
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[0].DataType); // PK_CATALOG_NAME
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[1].DataType); // PK_SCHEMA_NAME
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[2].DataType); // PK_TABLE_NAME
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[3].DataType); // PK_COLUMN_NAME
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[4].DataType); // FK_CATALOG_NAME
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[5].DataType); // FK_SCHEMA_NAME
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[6].DataType); // FK_TABLE_NAME
                Assert.Equal(StringType.Default, queryResult.Stream.Schema.FieldsList[7].DataType); // FK_COLUMN_NAME
                // Databricks return Int16(SmallInt)
                Assert.True(queryResult.Stream.Schema.FieldsList[8].DataType is Int32Type or Int16Type, "FK_INDEX should be either Int32 or Int16"); // FK_INDEX
                Assert.Equal(expectedBatchLength, batch.Length);
                actualBatchLength += batch.Length;
                for (int i = 0; i < batch.Length; i++)
                {
                    string? parentCatalogNameActual = ((StringArray)batch.Column(0)).GetString(i);
                    Assert.True(string.Equals(catalogName, parentCatalogNameActual)
                        || string.IsNullOrEmpty(catalogName) && string.IsNullOrEmpty(parentCatalogNameActual));
                    string parentSchemaNameActual = ((StringArray)batch.Column(1)).GetString(i);
                    Assert.Equal(schemaName, parentSchemaNameActual);
                    string tableNameActual = ((StringArray)batch.Column(2)).GetString(i);
                    Assert.Equal(tableNameParent, tableNameActual);

                    int? keyIndex = ((Int32Array)batch.Column(8)).GetValue(i);
                    Assert.True(keyIndex <= primaryKeys.Count);
                    Assert.True(keyIndex.HasValue);

                    // Assume one-indexed key index
                    string? parentColumnNameActual = ((StringArray)batch.Column(3)).GetString(i)?.ToLowerInvariant();
                    Assert.Equal(primaryKeys[keyIndex.Value - 1], parentColumnNameActual);
                    string? foreignColumnNameActual = ((StringArray)batch.Column(7)).GetString(i)?.ToLowerInvariant();
                    Assert.Equal(foreignKeys[keyIndex.Value - 1], foreignColumnNameActual);
                }
            }

            Assert.Equal(expectedBatchLength, actualBatchLength);
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
    internal class ShortRunningStatementTimeoutTestData : TheoryData<StatementWithExceptions>
    {
        public ShortRunningStatementTimeoutTestData()
        {
            Add(new(0, null, null));
            Add(new(null, null, null));
            Add(new(1, null, typeof(TimeoutException)));
            Add(new(5, null, null));
            Add(new(30, null, null));
        }
    }
}

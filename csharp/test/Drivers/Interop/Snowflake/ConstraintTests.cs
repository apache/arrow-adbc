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
using System.Linq;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ConstraintTests : IClassFixture<ConstraintTestsFixutre>
    {
        readonly SnowflakeTestConfiguration _snowflakeTestConfiguration;
        readonly ConstraintTestsFixutre _fixture;
        const string PRIMARY_KEY = "PRIMARY KEY";
        const string UNIQUE = "UNIQUE";
        const string FOREIGN_KEY = "FOREIGN KEY";

        public ConstraintTests(ConstraintTestsFixutre fixture)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
            _snowflakeTestConfiguration = SnowflakeTestingUtils.TestConfiguration;
            _fixture = fixture;
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as All and Column name as a pattern and get the table constraints data
        /// </summary>
        [SkippableTheory, Order(2)]
        [InlineData(PRIMARY_KEY, "SYS_CONSTRAINT_", new string[] { "ID", "NAME" })]
        public void CanGetObjectsTableConstraintsWithColumnNameFilter(string constraintType, string constraintNameStart, string[] columnNames)
        {
            // need to add the database
            string databaseName = _snowflakeTestConfiguration.Metadata.Catalog;
            string schemaName = _snowflakeTestConfiguration.Metadata.Schema;

            using IArrowArrayStream stream = _fixture._connection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: _fixture._tableName1,
                    tableTypes: _fixture._tableTypes,
                    columnNamePattern: columnNames[0]);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

            List<AdbcTable>? tables = catalogs
                .Where(c => string.Equals(c.Name, databaseName))
                .Select(c => c.DbSchemas)
                .FirstOrDefault()
                ?.Where(s => string.Equals(s.Name, schemaName))
                .Select(s => s.Tables)
                .FirstOrDefault();

            AdbcTable? table = tables?.Where((table) => string.Equals(table.Name, _fixture._tableName1, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            Assert.True(table != null, "table should not be null");
            Assert.True(table.Constraints != null, "table constraints should not be null");


            AdbcConstraint? adbcConstraint = table?.Constraints.Where((constraint) => string.Equals(constraint.Type, constraintType)).FirstOrDefault();
            Assert.True(adbcConstraint != null, $"{constraintType} should be present");
            Assert.StartsWith(constraintNameStart, adbcConstraint.Name);
            Assert.True(adbcConstraint.ColumnNames.Count == columnNames.Length, "constraint column count doesn't match");
            Assert.True(adbcConstraint.ColumnUsage.Count == 0, "ColumnUsages is only for foreign key");
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a pattern and get the table constraints data
        /// </summary>
        [SkippableTheory, Order(3)]
        [InlineData(UNIQUE, "SYS_CONSTRAINT_", new string[] { "ID" }, new string[] { }, AdbcConnection.GetObjectsDepth.All)]
        [InlineData(UNIQUE, "SYS_CONSTRAINT_", new string[] { "ID" }, new string[] { }, AdbcConnection.GetObjectsDepth.Tables)]
        [InlineData(PRIMARY_KEY, "SYS_CONSTRAINT_", new string[] { "COMPANY_NAME" }, new string[] { }, AdbcConnection.GetObjectsDepth.All)]
        [InlineData(PRIMARY_KEY, "SYS_CONSTRAINT_", new string[] { "COMPANY_NAME" }, new string[] { }, AdbcConnection.GetObjectsDepth.Tables)]
        [InlineData(FOREIGN_KEY, "ADBC_FKEY", new string[] { "DATABASE_ID", "DATABASE_NAME" }, new string[] { "ID", "NAME" }, AdbcConnection.GetObjectsDepth.All)]
        [InlineData(FOREIGN_KEY, "ADBC_FKEY", new string[] { "DATABASE_ID", "DATABASE_NAME" }, new string[] { "ID", "NAME" }, AdbcConnection.GetObjectsDepth.Tables)]
        public void CanGetObjectsTableConstraints(string constraintType, string constraintNameStart, string[] columnNames, string[] referenceColumnNames, AdbcConnection.GetObjectsDepth depth)
        {
            // need to add the database
            string databaseName = _snowflakeTestConfiguration.Metadata.Catalog;
            string schemaName = _snowflakeTestConfiguration.Metadata.Schema;

            using IArrowArrayStream stream = _fixture._connection.GetObjects(
                    depth: depth,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: _fixture._tableName2,
                    tableTypes: _fixture._tableTypes,
                    columnNamePattern: null);

            using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

            List<AdbcTable>? tables = catalogs
                .Where(c => string.Equals(c.Name, databaseName))
                .Select(c => c.DbSchemas)
                .FirstOrDefault()
                ?.Where(s => string.Equals(s.Name, schemaName))
                .Select(s => s.Tables)
                .FirstOrDefault();

            AdbcTable? table = tables?.Where((table) => string.Equals(table.Name, _fixture._tableName2, StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            Assert.True(table != null, "table should not be null");
            Assert.True(table.Constraints != null, "table constraints should not be null");


            AdbcConstraint? adbcConstraint = table.Constraints?.Where((constraint) => string.Equals(constraint.Type, constraintType)).FirstOrDefault();
            Assert.True(adbcConstraint != null, $"{constraintType} should be present");
            Assert.StartsWith(constraintNameStart, adbcConstraint.Name);
            Assert.True(adbcConstraint.ColumnNames.Count == columnNames.Length, "constraint column count doesn't match");
            foreach (string columnName in columnNames)
            {
                Assert.True(adbcConstraint.ColumnNames.Where((col) => string.Equals(col, columnName)).FirstOrDefault() != null, $"{columnName} is not marked as {constraintType}");
            }
            if (constraintType == FOREIGN_KEY)
            {
                foreach (string referenceColumnName in referenceColumnNames)
                {
                    AdbcUsageSchema usageSchemaExpected = new AdbcUsageSchema()
                    {
                        FkCatalog = databaseName,
                        FkDbSchema = schemaName,
                        FkTable = _fixture._tableName1.ToUpper(),
                        FkColumnName = referenceColumnName
                    };
                    Assert.True(adbcConstraint.ColumnUsage.Where((usageSchema) => usageSchema.Equals(usageSchemaExpected)).FirstOrDefault() != null, $"ColumnUsage should be present for '{referenceColumnName}' column in '{_fixture._tableName1}' table");
                }
            }
            else
            {
                Assert.True(adbcConstraint.ColumnUsage.Count == 0, "ColumnUsages is only for foreign key");
            }
        }
    }

    public class ConstraintTestsFixutre : IDisposable
    {
        public readonly string s_testTablePrefix = "ADBCCONSTRAINTTEST_";
        readonly SnowflakeTestConfiguration _snowflakeTestConfiguration;
        public readonly AdbcConnection _connection;
        public readonly AdbcStatement _statement;
        public readonly List<string> _tableTypes;
        public readonly string _tableName1;
        public readonly string _tableName2;
        private bool _disposed = false;

        private const string SNOWFLAKE_CONSTRAINTS_DATA_RESOURCE = "Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake.Resources.SnowflakeConstraints.sql";


        public ConstraintTestsFixutre()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
            _snowflakeTestConfiguration = SnowflakeTestingUtils.TestConfiguration;
            _tableName1 = s_testTablePrefix + Guid.NewGuid().ToString().Replace("-", "");
            _tableName2 = s_testTablePrefix + Guid.NewGuid().ToString().Replace("-", "");
            _tableTypes = new List<string> { "BASE TABLE", "VIEW" };
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_snowflakeTestConfiguration, out parameters);
            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            _connection = adbcDatabase.Connect(options);
            _statement = _connection.CreateStatement();
            CreateTables();
        }

        private void CreateTables()
        {
            string[] queries = SnowflakeTestingUtils.GetQueries(_snowflakeTestConfiguration, SNOWFLAKE_CONSTRAINTS_DATA_RESOURCE);

            Dictionary<string, string> placeholderValues = new Dictionary<string, string>() {
                {"{ADBC_CONSTRANT_TABLE_1}", _tableName1 },
                {"{ADBC_CONSTRANT_TABLE_2}", _tableName2 }
            };

            for (int i = 0; i < queries.Length; i++)
            {
                string query = queries[i];
                foreach (string key in placeholderValues.Keys)
                {
                    if (query.Contains(key))
                        query = query.Replace(key, placeholderValues[key]);
                }
                UpdateResult updateResult = ExecuteUpdateStatement(query);
            }
        }

        private void DropCreatedTables()
        {
            string[] queries = new string[] {
                $"DROP TABLE IF EXISTS {_snowflakeTestConfiguration.Metadata.Catalog}.{_snowflakeTestConfiguration.Metadata.Schema}.{_tableName1}",
                $"DROP TABLE IF EXISTS {_snowflakeTestConfiguration.Metadata.Catalog}.{_snowflakeTestConfiguration.Metadata.Schema}.{_tableName2}"
            };

            for (int i = 0; i < queries.Length; i++)
            {
                string query = queries[i];
                UpdateResult updateResult = ExecuteUpdateStatement(query);
            }
        }

        private UpdateResult ExecuteUpdateStatement(string query)
        {
            using AdbcStatement statement = _connection.CreateStatement();
            statement.SqlQuery = query;
            UpdateResult updateResult = statement.ExecuteUpdate();
            return updateResult;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                DropCreatedTables();
                _connection?.Dispose();
                _statement?.Dispose();
                _disposed = true;
            }
        }
    }
}

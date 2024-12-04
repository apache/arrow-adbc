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
using System.Diagnostics;
using System.Linq;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.FlightSql
{
    /// <summary>
    /// Class for testing the Flight SQL ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class DriverTests : IDisposable
    {
        readonly FlightSqlTestConfiguration _testConfiguration;
        readonly List<FlightSqlTestEnvironment> _environments;
        readonly Dictionary<string, AdbcConnection> _configuredConnections = new Dictionary<string, AdbcConnection>();
        readonly ITestOutputHelper _outputHelper;

        private List<string> GetPatterns(string? namePattern, bool caseSenstive)
        {
            List<string> patterns = new List<string>();

            string name = namePattern!;
            patterns.Add(name);
            patterns.Add($"{GetPartialNameForPatternMatch(name)}%");
            patterns.Add($"_{GetNameWithoutFirstChatacter(name)}");

            if (!caseSenstive)
            {
                patterns.Add($"{GetPartialNameForPatternMatch(name).ToLower()}%");
                patterns.Add($"{GetPartialNameForPatternMatch(name).ToUpper()}%");
                patterns.Add($"_{GetNameWithoutFirstChatacter(name).ToLower()}");
                patterns.Add($"_{GetNameWithoutFirstChatacter(name).ToUpper()}");
            }

            return patterns;
        }

        public DriverTests(ITestOutputHelper outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(FlightSqlTestingUtils.FLIGHTSQL_INTEROP_TEST_CONFIG_VARIABLE));
            _testConfiguration = FlightSqlTestingUtils.LoadFlightSqlTestConfiguration();
            _environments = FlightSqlTestingUtils.GetTestEnvironments(_testConfiguration);
            _outputHelper = outputHelper;

            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                Dictionary<string, string> parameters = new Dictionary<string, string>();
                Dictionary<string, string> options = new Dictionary<string, string>();
                AdbcDriver driver = FlightSqlTestingUtils.GetAdbcDriver(_testConfiguration, environment, out parameters);
                AdbcDatabase database = driver.Open(parameters);
                AdbcConnection connection = database.Connect(options);

                _configuredConnections.Add(environment.Name!, connection);
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        /// <remarks>
        /// Tests are ordered to ensure data is created
        /// for the other queries to run.
        /// </remarks>
        [SkippableFact, Order(1)]
        public void CanExecuteUpdate()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                // Dremio doesn't have acceptPut implemented by design.
                if (environment.SupportsWriteUpdate)
                {
                    string[] queries = FlightSqlTestingUtils.GetQueries(environment);

                    List<int> expectedResults = new List<int>() { -1, 1, 1 };

                    for (int i = 0; i < queries.Length; i++)
                    {
                        string query = queries[i];
                        UpdateResult updateResult = ExecuteUpdateStatement(environment, query);
                        Assert.Equal(expectedResults[i], updateResult.AffectedRows);
                    }
                }
                else
                {
                    _outputHelper.WriteLine("WriteUpdate is not supported in the [" + environment.Name + "] environment");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetInfo.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanGetInfo()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetInfo(new List<AdbcInfoCode>() { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion, AdbcInfoCode.VendorName });
                using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
                UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");

                List<string> expectedValues = new List<string>() { "DriverName", "DriverVersion", "VendorName" };

                for (int i = 0; i < infoNameArray.Length; i++)
                {
                    uint? uintValue = infoNameArray.GetValue(i);

                    if (uintValue.HasValue)
                    {
                        AdbcInfoCode value = (AdbcInfoCode)uintValue;
                        DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                        Assert.Contains(value.ToString(), expectedValues);

                        StringArray stringArray = (StringArray)valueArray.Fields[0];
                        _outputHelper.WriteLine($"{value}={stringArray.GetString(i)}");
                    }
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Catalogs and CatalogName passed as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsCatalogs()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                // Dremio doesn't use catalogs
                if (environment.SupportsCatalogs)
                {
                    string databaseName = environment.Metadata.Catalog;
                    foreach (string catalogPattern in GetPatterns(databaseName, environment.CaseSensitive))
                    {
                        string schemaName = environment.Metadata.Schema;

                        using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                                depth: AdbcConnection.GetObjectsDepth.Catalogs,
                                catalogPattern: catalogPattern,
                                dbSchemaPattern: null,
                                tableNamePattern: null,
                                tableTypes: environment.TableTypes,
                                columnNamePattern: null);

                        using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                        List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, null);
                        AdbcCatalog? catalog = catalogs.Where((catalog) => string.Equals(catalog.Name, databaseName)).FirstOrDefault();

                        Assert.True(catalog != null, "catalog should not be null in the [" + environment.Name + "] environment");
                    }
                }
                else
                {
                    _outputHelper.WriteLine("Catalogs are not supported in the [" + environment.Name + "] environment");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as DbSchemas with DbSchemaName as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsDbSchemas()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                string databaseName = environment.Metadata.Catalog;
                string schemaName = environment.Metadata.Schema;

                if (schemaName != null)
                {
                    foreach (string dbSchemaPattern in GetPatterns(schemaName, environment.CaseSensitive))
                    {
                        using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                                depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                                catalogPattern: databaseName,
                                dbSchemaPattern: dbSchemaPattern,
                                tableNamePattern: null,
                                tableTypes: environment.TableTypes,
                                columnNamePattern: null);

                        using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                        List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                        List<AdbcDbSchema>? dbSchemas = catalogs
                            .Where(c => environment.SupportsCatalogs ? string.Equals(c.Name, databaseName) : true)
                            .Select(c => c.DbSchemas)
                            .FirstOrDefault();

                        Assert.True(dbSchemas != null, "dbSchemas should not be null in the [" + environment.Name + "] environment");

                        AdbcDbSchema? dbSchema = dbSchemas
                            .Where(dbSchema => schemaName == null ? string.Equals(dbSchema.Name, string.Empty) : string.Equals(dbSchema.Name, schemaName))
                            .FirstOrDefault();

                        Assert.True(dbSchema != null, "dbSchema should not be null in the [" + environment.Name + "] environment");
                    }
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects with GetObjectsDepth as Tables with TableName as a pattern.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsTables()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                string databaseName = environment.Metadata.Catalog;
                string schemaName = environment.Metadata.Schema;
                string tableName = environment.Metadata.Table;

                foreach (string tableNamePattern in GetPatterns(tableName, environment.CaseSensitive))
                {
                    using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.Tables,
                        catalogPattern: databaseName,
                        dbSchemaPattern: schemaName,
                        tableNamePattern: tableNamePattern,
                        tableTypes: environment.TableTypes,
                        columnNamePattern: null);

                    using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                    List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                    List<AdbcDbSchema>? schemas = catalogs
                        .Where(c => environment.SupportsCatalogs ? string.Equals(c.Name, databaseName) : true)
                        .Select(c => c.DbSchemas)
                        .FirstOrDefault();

                    Assert.True(schemas != null, "schemas should not be null in the [" + environment.Name + "] environment");

                    List<AdbcTable>? tables = schemas
                        .Where(s => schemaName == null ? string.Equals(s.Name, string.Empty) : string.Equals(s.Name, schemaName))
                        .Select(s => s.Tables)
                        .FirstOrDefault();

                    Assert.True(tables != null, "schemas should not be null in the [" + environment.Name + "] environment");

                    AdbcTable? table = tables.Where((table) => string.Equals(table.Name, tableName)).FirstOrDefault();
                    Assert.True(table != null, $"could not find the table named [{tableName}] from the [{tableNamePattern}] pattern in the [" + environment.Name + "] environment. Is this environment case sensitive?");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects for GetObjectsDepth as All.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsAll()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                string? databaseName = environment.Metadata.Catalog;
                string? schemaName = environment.Metadata.Schema;
                string tableName = environment.Metadata.Table;
                string? columnName = null;

                using IArrowArrayStream stream = GetAdbcConnection(environment.Name).GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.All,
                        catalogPattern: databaseName,
                        dbSchemaPattern: schemaName,
                        tableNamePattern: tableName,
                        tableTypes: environment.TableTypes,
                        columnNamePattern: columnName);

                using RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                List<AdbcDbSchema>? schemas = catalogs
                    .Where(c => environment.SupportsCatalogs ? string.Equals(c.Name, databaseName) : true)
                    .Select(c => c.DbSchemas)
                    .FirstOrDefault();

                Assert.True(schemas != null, "schemas should not be null in the [" + environment.Name + "] environment");

                List<AdbcTable>? tables;

                if (schemaName == null)
                {
                    tables = schemas
                        .Where(s => string.Equals(s.Name, string.Empty))
                        .Select(s => s.Tables)
                        .FirstOrDefault();
                }
                else
                {
                    tables = schemas
                        .Where(s => string.Equals(s.Name, schemaName))
                        .Select(s => s.Tables)
                        .FirstOrDefault();
                }

                Assert.True(tables != null, "tables should not be null in the [" + environment.Name + "] environment");

                AdbcTable? table = tables
                    .Where(t => string.Equals(t.Name, tableName))
                    .FirstOrDefault();

                Assert.True(table != null, "table should not be null in the [" + environment.Name + "] environment");

                List<AdbcColumn>? columns = table.Columns;

                Assert.True(columns != null, "Columns cannot be null in the [" + environment.Name + "] environment");
                Assert.Equal(environment.Metadata.ExpectedColumnCount, columns.Count);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanGetTableSchema()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                string databaseName = environment.Metadata.Catalog;
                string schemaName = environment.Metadata.Schema;
                string tableName = environment.Metadata.Table;

                Schema schema = GetAdbcConnection(environment.Name).GetTableSchema(databaseName, schemaName, tableName);

                int numberOfFields = schema.FieldsList.Count;

                Assert.True(environment.Metadata.ExpectedColumnCount == numberOfFields, "ExpectedColumnCount not equal in the [" + environment.Name + "] environment");
            }
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanGetTableTypes()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                using IArrowArrayStream arrowArrayStream = GetAdbcConnection(environment.Name).GetTableTypes();

                using RecordBatch recordBatch = arrowArrayStream.ReadNextRecordBatchAsync().Result;

                StringArray stringArray = (StringArray)recordBatch.Column("table_type");

                List<string> known_types = environment.TableTypes;

                int results = 0;

                for (int i = 0; i < stringArray.Length; i++)
                {
                    string value = stringArray.GetString(i);

                    if (known_types.Contains(value))
                    {
                        results++;
                    }
                }

                Assert.True(known_types.Count == results, "TableTypes not equal in the [" + environment.Name + "] environment");
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(6)]
        public void CanExecuteQuery()
        {
            foreach (FlightSqlTestEnvironment environment in _environments)
            {
                using AdbcStatement statement = GetAdbcConnection(environment.Name).CreateStatement();
                statement.SqlQuery = environment.Query;

                QueryResult queryResult = statement.ExecuteQuery();

                Tests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount, environment.Name);
            }
        }

        private UpdateResult ExecuteUpdateStatement(FlightSqlTestEnvironment environment, string query)
        {
            using AdbcStatement statement = GetAdbcConnection(environment.Name).CreateStatement();
            statement.SqlQuery = query;
            UpdateResult updateResult = statement.ExecuteUpdate();
            return updateResult;
        }

        private AdbcConnection GetAdbcConnection(string? environmentName)
        {
            if (string.IsNullOrEmpty(environmentName))
            {
                throw new ArgumentNullException(nameof(environmentName));
            }

            return _configuredConnections[environmentName!];
        }

        private string GetPartialNameForPatternMatch(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length == 1) return name;

            return name.Substring(0, name.Length / 2);
        }

        private string GetNameWithoutFirstChatacter(string name)
        {
            if (string.IsNullOrEmpty(name)) return name;

            return name.Substring(1);
        }

        public void Dispose()
        {
            foreach (AdbcConnection configuredConnection in this._configuredConnections.Values)
            {
                configuredConnection.Dispose();
            }

            _configuredConnections.Clear();
        }
    }
}

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
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class DriverTests
    {
        BigQueryTestConfiguration _testConfiguration;
        readonly List<BigQueryTestEnvironment> _environments;
        readonly Dictionary<string, AdbcConnection> _configuredConnections = new Dictionary<string, AdbcConnection>();

        readonly ITestOutputHelper? _outputHelper;

        public DriverTests(ITestOutputHelper? outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE));

            _testConfiguration = MultiEnvironmentTestUtils.LoadMultiEnvironmentTestConfiguration<BigQueryTestConfiguration>(BigQueryTestingUtils.BIGQUERY_TEST_CONFIG_VARIABLE);
            _environments = MultiEnvironmentTestUtils.GetTestEnvironments<BigQueryTestEnvironment>(_testConfiguration);
            _outputHelper = outputHelper;

            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection connection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                _configuredConnections.Add(environment.Name!, connection);
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanExecuteUpdate()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                string[] queries = BigQueryTestingUtils.GetQueries(environment);

                List<int> expectedResults = new List<int>() { -1, 1, 1 };

                for (int i = 0; i < queries.Length; i++)
                {
                    string query = queries[i];
                    AdbcStatement statement = adbcConnection.CreateStatement();
                    statement.SqlQuery = query;

                    UpdateResult updateResult = statement.ExecuteUpdate();

                    Assert.Equal(expectedResults[i], updateResult.AffectedRows);
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetInfo.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanGetInfo()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                IArrowArrayStream stream = adbcConnection.GetInfo(new List<AdbcInfoCode>() { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion, AdbcInfoCode.VendorName });

                RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
                UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");

                List<string> expectedValues = new List<string>() { "DriverName", "DriverVersion", "VendorName" };

                for (int i = 0; i < infoNameArray.Length; i++)
                {
                    AdbcInfoCode value = (AdbcInfoCode)infoNameArray.GetValue(i)!.Value;
                    DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                    Assert.Contains(value.ToString(), expectedValues);

                    StringArray stringArray = (StringArray)valueArray.Fields[0];
                    Console.WriteLine($"{value}={stringArray.GetString(i)}");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjectsAllCatalogs()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                IArrowArrayStream stream = adbcConnection.GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.Catalogs,
                        catalogPattern: null,
                        dbSchemaPattern: null,
                        tableNamePattern: null,
                        tableTypes: BigQueryTableTypes.TableTypes,
                        columnNamePattern: null);

                RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, null);

                foreach (AdbcCatalog ct in catalogs)
                {
                    this._outputHelper?.WriteLine($"{ct.Name} in [{environment.Name}]");
                }
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanGetObjects()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                // need to add the database
                string? catalogName = environment.Metadata.Catalog;
                string? schemaName = environment.Metadata.Schema;
                string? tableName = environment.Metadata.Table;
                string? columnName = null;

                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                IArrowArrayStream stream = adbcConnection.GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.All,
                        catalogPattern: catalogName,
                        dbSchemaPattern: schemaName,
                        tableNamePattern: tableName,
                        tableTypes: BigQueryTableTypes.TableTypes,
                        columnNamePattern: columnName);

                RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                List<AdbcColumn>? columns = catalogs
                    .Select(s => s.DbSchemas)
                    .FirstOrDefault()
                    ?.Select(t => t.Tables)
                    .FirstOrDefault()
                    ?.Select(c => c.Columns)
                    .FirstOrDefault();

                Assert.Equal(environment.Metadata.ExpectedColumnCount, columns?.Count);
            }
        }

        [SkippableFact, Order(3)]
        public void CanGetObjectsTables()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                string? catalogName = environment.Metadata.Catalog;
                string? schemaName = environment.Metadata.Schema;
                string? tableName = environment.Metadata.Table;

                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                IArrowArrayStream stream = adbcConnection.GetObjects(
                        depth: AdbcConnection.GetObjectsDepth.Tables,
                        catalogPattern: catalogName,
                        dbSchemaPattern: schemaName,
                        tableNamePattern: null,
                        tableTypes: BigQueryTableTypes.TableTypes,
                        columnNamePattern: null);

                RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

                List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, schemaName);

                List<AdbcTable>? tables = catalogs
                    .Where(c => string.Equals(c.Name, catalogName))
                    .Select(c => c.DbSchemas)
                    .FirstOrDefault()
                    ?.Where(s => string.Equals(s.Name, schemaName))
                    .Select(s => s.Tables)
                    .FirstOrDefault();

                AdbcTable? table = tables?.Where((table) => string.Equals(table.Name, tableName)).FirstOrDefault();
                Assert.True(table != null, "table should not be null in the  [" + environment.Name + "] environment");
                Assert.Equal("BASE TABLE", table.Type);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanGetTableSchema()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                string? catalogName = environment.Metadata.Catalog;
                string? schemaName = environment.Metadata.Schema;
                string tableName = environment.Metadata.Table;

                Schema schema = adbcConnection.GetTableSchema(catalogName, schemaName, tableName);

                int numberOfFields = schema.FieldsList.Count;

                Assert.Equal(environment.Metadata.ExpectedColumnCount, numberOfFields);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanGetTableTypes()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                IArrowArrayStream arrowArrayStream = adbcConnection.GetTableTypes();

                RecordBatch recordBatch = arrowArrayStream.ReadNextRecordBatchAsync().Result;

                StringArray stringArray = (StringArray)recordBatch.Column("table_type");

                List<string> known_types = BigQueryTableTypes.TableTypes.ToList();

                int results = 0;

                for (int i = 0; i < stringArray.Length; i++)
                {
                    string value = stringArray.GetString(i);

                    if (known_types.Contains(value))
                    {
                        results++;
                    }
                }

                Assert.Equal(known_types.Count, results);
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(6)]
        public void CanExecuteQuery()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                AdbcStatement statement = adbcConnection.CreateStatement();
                statement.SqlQuery = environment.Query;

                QueryResult queryResult = statement.ExecuteQuery();

                Tests.DriverTests.CanExecuteQuery(queryResult, environment.ExpectedResultsCount, environment.Name);
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(6)]
        public void CanExecuteParallelQueries()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                Parallel.For(0, environment.NumberOfParallelRuns, (i) =>
                {
                    Parallel.ForEach(environment.ParallelQueries, pq =>
                    {
                        using (AdbcStatement statement = adbcConnection.CreateStatement())
                        {
                            statement.SqlQuery = pq.Query;

                            QueryResult queryResult = statement.ExecuteQuery();

                            _outputHelper?.WriteLine($"({i}) {DateTime.Now.Ticks} - {queryResult.RowCount} results for {pq.Query}");

                            Tests.DriverTests.CanExecuteQuery(queryResult, pq.ExpectedResultsCount, environment.Name);
                        }
                    });
                });
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results.
        /// </summary>
        [SkippableFact, Order(7)]
        public void CanExecuteMetadataQuery()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                if (environment.IsMetadataCommand)
                {
                    AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                    AdbcStatement statement = adbcConnection.CreateStatement();
                    statement.SqlQuery = environment.Query;

                    QueryResult queryResult = statement.ExecuteQuery();

                    RecordBatch? recordBatch = queryResult.Stream?.ReadNextRecordBatchAsync().Result;

                    Assert.NotNull(recordBatch);

                    string json = RecordBatchToJson(recordBatch);

                    Assert.True(environment.ExpectedMetadataResultJson?.Equals(json, StringComparison.Ordinal), "Expected results do not match");
                }
            }
        }

        private static string RecordBatchToJson(RecordBatch recordBatch)
        {
            List<Dictionary<string, object?>> rows = new List<Dictionary<string, object?>>();

            for (int rowIndex = 0; rowIndex < recordBatch.Length; rowIndex++)
            {
                Dictionary<string, object?> row = new Dictionary<string, object?>();

                for (int colIndex = 0; colIndex < recordBatch.ColumnCount; colIndex++)
                {
                    IArrowArray column = recordBatch.Column(colIndex);
                    string fieldName = recordBatch.Schema.GetFieldByIndex(colIndex).Name;

                    object? value = column.ValueAt(rowIndex);
                    row[fieldName] = value;
                }

                rows.Add(row);
            }

            return JsonSerializer.Serialize(rows, new JsonSerializerOptions
            {
                WriteIndented = false,
                PropertyNamingPolicy = null
            });
        }

        private AdbcConnection GetAdbcConnection(string? environmentName)
        {
            if (string.IsNullOrEmpty(environmentName))
            {
                throw new ArgumentNullException(nameof(environmentName));
            }

            return _configuredConnections[environmentName!];
        }

        /// <summary>
        /// Validates the ClientTimeout parameter.
        /// </summary>
        [SkippableFact, Order(7)]
        public void ClientTimeoutTest()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                if (environment.RunTimeoutTests && environment.ClientTimeout.HasValue)
                {
                    AdbcConnection adbcConnection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);

                    AdbcStatement statement = adbcConnection.CreateStatement();
                    statement.SqlQuery = environment.Query;

                    Assert.Throws<TaskCanceledException>(() => { statement.ExecuteQuery(); });
                }
            }
        }

        /// <summary>
        /// Validates the GetQueryResultsOptionsTimeoutMinutes parameter.
        /// </summary>
        [SkippableFact, Order(8)]
        public void QueryTimeoutTest()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                if (environment.RunTimeoutTests && (environment.QueryTimeout.HasValue || environment.TimeoutMinutes.HasValue))
                {
                    AdbcConnection adbcConnection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                    AdbcStatement statement = adbcConnection.CreateStatement();
                    statement.SqlQuery = environment.Query;
                    Assert.Throws<TimeoutException>(() => { statement.ExecuteQuery(); });
                }
            }
        }

        /// <summary>
        /// Validates if the driver can connect to a live server and
        /// parse the results of multi-statements.
        /// </summary>
        [SkippableFact, Order(9)]
        public void CanExecuteMultiStatementQuery()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);
                AdbcStatement statement = adbcConnection.CreateStatement();
                string query1 = "SELECT * FROM bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide";
                string query2 = "SELECT " +
                          "CAST(1.7976931348623157e+308 as FLOAT64) as number, " +
                          "PARSE_NUMERIC(\"9.99999999999999999999999999999999E+28\") as decimal, " +
                          "PARSE_BIGNUMERIC(\"5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+37\") as big_decimal";
                string combinedQuery = query1 + ";" + query2 + ";";
                statement.SqlQuery = combinedQuery;
                QueryResult queryResult = statement.ExecuteQuery();
                Tests.DriverTests.CanExecuteQuery(queryResult, 61900, environment.Name);
            }
        }
    }
}

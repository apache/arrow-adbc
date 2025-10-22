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
using System.Reflection;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class StatementTests
    {
        BigQueryTestConfiguration _testConfiguration;
        readonly List<BigQueryTestEnvironment> _environments;
        readonly Dictionary<string, AdbcConnection> _configuredConnections = new Dictionary<string, AdbcConnection>();
        readonly ITestOutputHelper? _outputHelper;

        public StatementTests(ITestOutputHelper? outputHelper)
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

        [Fact]
        public void CanSetStatementOption()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                // ensure the value isn't already set
                Assert.False(environment.AllowLargeResults);

                AdbcConnection adbcConnection = BigQueryTestingUtils.GetBigQueryAdbcConnection(environment);
                AdbcStatement statement = adbcConnection.CreateStatement();
                statement.SetOption(BigQueryParameters.AllowLargeResults, "true");

                // BigQuery is currently on ADBC 1.0, so it doesn't have the GetOption interface. Therefore, use reflection to validate the value is set correctly.
                const BindingFlags bindingAttr = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance;
                IReadOnlyDictionary<string, string>? options = statement.GetType().GetProperty("Options", bindingAttr)!.GetValue(statement) as IReadOnlyDictionary<string, string>;
                Assert.True(options != null);
                Assert.True(options[BigQueryParameters.AllowLargeResults] == "true");
            }
        }

        [Fact]
        public async Task CanCancelStatement()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                AdbcStatement statement = adbcConnection.CreateStatement();

                // Execute the query/cancel multiple times to validate consistent behavior
                const int iterations = 3;
                for (int i = 0; i < iterations; i++)
                {
                    _outputHelper?.WriteLine($"Iteration {i + 1} of {iterations}");
                    // Generate unique column names so query will not be served from cache
                    string columnName1 = Guid.NewGuid().ToString("N");
                    string columnName2 = Guid.NewGuid().ToString("N");
                    statement.SqlQuery = $"SELECT GENERATE_ARRAY(`{columnName2}`, 10000) AS `{columnName1}` FROM UNNEST(GENERATE_ARRAY(0, 100000)) AS `{columnName2}`";
                    _outputHelper?.WriteLine($"Query: {statement.SqlQuery}");

                    // Expect this to take about 10 seconds without cancellation
                    Task<QueryResult> queryTask = Task.Run(statement.ExecuteQuery);

                    await Task.Yield();
                    await Task.Delay(3000);
                    statement.Cancel();

                    try
                    {
                        QueryResult queryResult = await queryTask;
                        Assert.Fail("Expecting OperationCanceledException to be thrown.");
                    }
                    catch (Exception ex) when (ex is OperationCanceledException)
                    {
                        _outputHelper?.WriteLine($"Received expected OperationCanceledException: {ex.Message}");
                    }
                    catch (Exception ex) when (ex is not FailException)
                    {
                        Assert.Fail($"Expecting OperationCanceledException to be thrown. Instead, received {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }
        }

        [Fact]
        public async Task CanCancelStreamAndDisposeStatement()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                AdbcStatement statement = adbcConnection.CreateStatement();

                // Execute the query/cancel multiple times to validate consistent behavior
                const int iterations = 3;
                QueryResult[] results = new QueryResult[iterations];
                for (int i = 0; i < iterations; i++)
                {
                    _outputHelper?.WriteLine($"Iteration {i + 1} of {iterations}");
                    // Generate unique column names so query will not be served from cache
                    string columnName1 = Guid.NewGuid().ToString("N");
                    string columnName2 = Guid.NewGuid().ToString("N");
                    statement.SqlQuery = $"SELECT `{columnName2}` AS `{columnName1}` FROM UNNEST(GENERATE_ARRAY(1, 100)) AS `{columnName2}`";
                    _outputHelper?.WriteLine($"Query: {statement.SqlQuery}");

                    // Expect this to take about 10 seconds without cancellation
                    results[i] = statement.ExecuteQuery();
                }
                statement.Cancel();
                for (int index = 0; index < iterations; index++)
                {
                    try
                    {
                        QueryResult queryResult = results[index];
                        using IArrowArrayStream? stream = queryResult.Stream;
                        Assert.NotNull(stream);
                        RecordBatch batch = await stream.ReadNextRecordBatchAsync();

                        Assert.Fail("Expecting OperationCanceledException to be thrown.");
                    }
                    catch (Exception ex) when (BigQueryUtils.ContainsException(ex, out OperationCanceledException? _))
                    {
                        _outputHelper?.WriteLine($"Received expected OperationCanceledException: {ex.Message}");
                    }
                    catch (Exception ex) when (ex is not FailException)
                    {
                        Assert.Fail($"Expecting OperationCanceledException to be thrown. Instead, received {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }
        }

        [Fact]
        public async Task CanCancelStreamFromStatement()
        {
            foreach (BigQueryTestEnvironment environment in _environments)
            {
                using AdbcConnection adbcConnection = GetAdbcConnection(environment.Name);

                AdbcStatement statement = adbcConnection.CreateStatement();

                // Execute the query/cancel multiple times to validate consistent behavior
                const int iterations = 3;
                QueryResult[] results = new QueryResult[iterations];
                for (int i = 0; i < iterations; i++)
                {
                    _outputHelper?.WriteLine($"Iteration {i + 1} of {iterations}");
                    // Generate unique column names so query will not be served from cache
                    string columnName1 = Guid.NewGuid().ToString("N");
                    string columnName2 = Guid.NewGuid().ToString("N");
                    statement.SqlQuery = $"SELECT `{columnName2}` AS `{columnName1}` FROM UNNEST(GENERATE_ARRAY(1, 100)) AS `{columnName2}`";
                    _outputHelper?.WriteLine($"Query: {statement.SqlQuery}");

                    // Expect this to take about 10 seconds without cancellation
                    results[i] = statement.ExecuteQuery();
                }
                statement.Cancel();
                statement.Dispose();
                for (int index = 0; index < iterations; index++)
                {
                    try
                    {
                        QueryResult queryResult = results[index];
                        using IArrowArrayStream? stream = queryResult.Stream;
                        Assert.NotNull(stream);
                        RecordBatch batch = await stream.ReadNextRecordBatchAsync();

                        Assert.Fail("Expecting OperationCanceledException to be thrown.");
                    }
                    catch (Exception ex) when (BigQueryUtils.ContainsException(ex, out OperationCanceledException? _))
                    {
                        _outputHelper?.WriteLine($"Received expected OperationCanceledException: {ex.Message}");
                    }
                    catch (Exception ex) when (ex is not FailException)
                    {
                        Assert.Fail($"Expecting OperationCanceledException to be thrown. Instead, received {ex.GetType().Name}: {ex.Message}");
                    }
                }
            }
        }

        private AdbcConnection GetAdbcConnection(string? environmentName)
        {
            if (string.IsNullOrEmpty(environmentName))
            {
                throw new ArgumentNullException(nameof(environmentName));
            }

            return _configuredConnections[environmentName!];
        }
    }
}

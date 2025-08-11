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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Provides a base class for ADBC tests.
    /// </summary>
    /// <typeparam name="TConfig">A TestConfiguration type to use when accessing test configuration files.</typeparam>
    public abstract class TestBase<TConfig, TEnv> : IDisposable
        where TConfig : TestConfiguration
        where TEnv : TestEnvironment<TConfig>
    {
        private bool _disposedValue;
        private readonly Lazy<TConfig> _testConfiguration;
        private readonly Lazy<AdbcConnection> _connection;
        private readonly Lazy<AdbcStatement> _statement;
        private readonly TestEnvironment<TConfig>.Factory<TEnv> _testEnvFactory;
        private readonly Lazy<TEnv> _testEnvironment;

        /// <summary>
        /// Constructs a new TestBase object with an output helper.
        /// </summary>
        /// <param name="outputHelper">Test output helper for writing test output.</param>
        public TestBase(ITestOutputHelper? outputHelper, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
        {
            OutputHelper = outputHelper;
            _testEnvFactory = testEnvFactory;
            _testEnvironment = new Lazy<TEnv>(() => _testEnvFactory.Create(() => Connection));
            _testConfiguration = new Lazy<TConfig>(() => Utils.LoadTestConfiguration<TConfig>(TestConfigVariable));
            _connection = new Lazy<AdbcConnection>(() => NewConnection());
            _statement = new Lazy<AdbcStatement>(() => Connection.CreateStatement());
        }

        /// <summary>
        /// Gets the test output helper.
        /// </summary>
        protected ITestOutputHelper? OutputHelper { get; }

        public TEnv TestEnvironment => _testEnvironment.Value;

        /// <summary>
        /// The name of the environment variable that stores the full location of the test configuration file.
        /// </summary>
        protected string TestConfigVariable => TestEnvironment.TestConfigVariable;

        protected string VendorVersion => TestEnvironment.VendorVersion;

        protected Version VendorVersionAsVersion => new Lazy<Version>(() => new Version(VendorVersion)).Value;


        /// <summary>
        /// Creates a temporary table (if possible) using the native SQL dialect.
        /// </summary>
        /// <param name="connection">The ADBC statement to apply the update.</param>
        /// <param name="columns">The columns definition in the native SQL dialect.</param>
        /// <returns>A disposable temporary table object that will drop the table when disposed.</returns>
        protected async Task<TemporaryTable> NewTemporaryTableAsync(AdbcConnection connection, string columns)
        {
            string tableName = NewTableName();
            string sqlUpdate = TestEnvironment.GetCreateTemporaryTableStatement(tableName, columns);
            return await TemporaryTable.NewTemporaryTableAsync(connection, tableName, sqlUpdate, OutputHelper);
        }

        /// <summary>
        /// Creates a new unique table name .
        /// </summary>
        /// <returns>A unique table name.</returns>
        protected string NewTableName() => string.Format(
                        "{0}{1}{2}",
                        string.IsNullOrEmpty(TestConfiguration.Metadata.Catalog) ? string.Empty : DelimitIdentifier(TestConfiguration.Metadata.Catalog) + ".",
                        string.IsNullOrEmpty(TestConfiguration.Metadata.Schema) ? string.Empty : DelimitIdentifier(TestConfiguration.Metadata.Schema) + ".",
                        DelimitIdentifier(Guid.NewGuid().ToString("N"))
                    );

        /// <summary>
        /// Gets the relative resource location of source SQL data used in driver testing.
        /// </summary>
        protected string SqlDataResourceLocation => TestEnvironment.SqlDataResourceLocation;

        protected int ExpectedColumnCount => TestEnvironment.ExpectedColumnCount;

        /// <summary>
        /// Creates a new driver.
        /// </summary>
        protected AdbcDriver NewDriver => TestEnvironment.CreateNewDriver();

        protected bool SupportsDelete => TestEnvironment.SupportsDelete;

        protected bool SupportsUpdate => TestEnvironment.SupportsUpdate;

        protected bool ValidateAffectedRows => TestEnvironment.ValidateAffectedRows;

        /// <summary>
        /// Gets the parameters from the test configuration that are passed to the driver as a dictionary.
        /// </summary>
        /// <param name="testConfiguration">The test configuration as input.</param>
        /// <returns>Dictionary of parameters for the driver.</returns>
        protected virtual Dictionary<string, string> GetDriverParameters(TConfig testConfiguration) => TestEnvironment.GetDriverParameters(testConfiguration);

        /// <summary>
        /// Gets a single ADBC Connection for the object.
        /// </summary>
        protected AdbcConnection Connection => _connection.Value;

        /// <summary>
        /// Gets the test configuration file.
        /// </summary>
        protected TConfig TestConfiguration => _testConfiguration.Value;

        /// <summary>
        /// Parses the queries from internal resource location
        /// </summary>
        protected string[] GetQueries()
        {
            const string placeholder = "{ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}";
            string[] sql = File.ReadAllLines(SqlDataResourceLocation);

            StringBuilder content = new StringBuilder();
            foreach (string line in sql)
            {
                if (line.TrimStart().StartsWith("--")) { continue; }
                if (line.Contains(placeholder))
                {
                    string table = TestConfiguration.Metadata.Table;
                    string catlog = !string.IsNullOrEmpty(TestConfiguration.Metadata.Catalog) ? TestConfiguration.Metadata.Catalog + "." : string.Empty;
                    string schema = !string.IsNullOrEmpty(TestConfiguration.Metadata.Schema) ? TestConfiguration.Metadata.Schema + "." : string.Empty;
                    string modifiedLine = line.Replace(placeholder, $"{catlog}{schema}{table}");
                    content.AppendLine(modifiedLine);
                }
                else
                {
                    content.AppendLine(line);
                }
            }

            string[] queries = content.ToString().Split(";".ToCharArray()).Where(x => x.Trim().Length > 0).ToArray();

            return queries;
        }

        protected SampleDataBuilder GetSampleDataBuilder()
        {
            return TestEnvironment.GetSampleDataBuilder();
        }

        /// <summary>
        /// Gets an ADBC driver with settings from the <see cref="Tests.TestConfiguration"/>.
        /// </summary>
        /// <param name="testConfiguration"><see cref="Tests.TestConfiguration"/></param>
        /// <param name="connectionOptions"></param>
        /// <returns></returns>
        protected AdbcConnection NewConnection(TConfig? testConfiguration = null, IReadOnlyDictionary<string, string>? connectionOptions = null)
        {
            Dictionary<string, string> parameters = GetDriverParameters(testConfiguration ?? TestConfiguration);
            AdbcDatabase database = NewDriver.Open(parameters);
            return database.Connect(connectionOptions ?? new Dictionary<string, string>());
        }

        /// <summary>
        /// Validates that an insert, select and delete statement works with the given value.
        /// </summary>
        /// <param name="selectStatement"></param>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to insert, select and delete.</param>
        /// <param name="formattedValue">The formatted value to insert, select and delete.</param>
        /// <returns></returns>
        protected async Task ValidateInsertSelectDeleteSingleValueAsync(string selectStatement, string tableName, string columnName, object value, string? formattedValue = null)
        {
            await InsertSingleValueAsync(tableName, columnName, formattedValue ?? value?.ToString());
            await SelectAndValidateValuesAsync(selectStatement, value, 1);
            string whereClause = GetWhereClause(columnName, formattedValue ?? value);
            if (SupportsDelete) await DeleteFromTableAsync(tableName, whereClause, 1);
        }

        /// <summary>
        /// Validates that an insert, select and delete statement works with the given value.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to insert, select and delete.</param>
        /// <param name="formattedValue">The formatted value to insert, select and delete.</param>
        /// <returns></returns>
        protected async Task ValidateInsertSelectDeleteSingleValueAsync(string tableName, string columnName, object? value, string? formattedValue = null)
        {
            await InsertSingleValueAsync(tableName, columnName, formattedValue ?? value?.ToString());
            await SelectAndValidateValuesAsync(tableName, columnName, value, 1, formattedValue);
            string whereClause = GetWhereClause(columnName, formattedValue ?? value);
            if (SupportsDelete) await DeleteFromTableAsync(tableName, whereClause, 1);
        }

        /// <summary>
        /// Validates that two inserts, select and delete statement works with the given value.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to insert, select and delete.</param>
        /// <param name="formattedValue">The formatted value to insert, select and delete.</param>
        /// <returns></returns>
        protected async Task ValidateInsertSelectDeleteTwoValuesAsync(string tableName, string columnName, object? value, string? formattedValue = null)
        {
            await InsertSingleValueAsync(tableName, columnName, formattedValue ?? value?.ToString());
            await InsertSingleValueAsync(tableName, columnName, formattedValue ?? value?.ToString());
            await SelectAndValidateValuesAsync(tableName, columnName, [value, value], 2, formattedValue);
            string whereClause = GetWhereClause(columnName, formattedValue ?? value);
            if (SupportsDelete) await DeleteFromTableAsync(tableName, whereClause, 2);
        }

        /// <summary>
        /// Validates "multi-value" scenarios
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnName"></param>
        /// <param name="indexColumnName"></param>
        /// <param name="values"></param>
        /// <param name="formattedValues"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        protected async Task ValidateInsertSelectDeleteMultipleValuesAsync(string tableName, string columnName, string indexColumnName, object?[] values, string?[]? formattedValues = null)
        {
            await InsertMultipleValuesWithIndexColumnAsync(tableName, columnName, indexColumnName, values, formattedValues);

            string selectStatement = $"SELECT {columnName}, {indexColumnName} FROM {tableName}";
            await SelectAndValidateValuesAsync(selectStatement, values, values.Length, hasIndexColumn: true);

            if (SupportsDelete) await DeleteFromTableAsync(tableName, "", values.Length);
        }

        /// <summary>
        /// Inserts multiple values
        /// </summary>
        /// <param name="tableName">Name of the table to perform the insert</param>
        /// <param name="columnName">Name of the value column to insert</param>
        /// <param name="indexColumnName">Name of the index column to insert index values</param>
        /// <param name="values">The array of values to insert</param>
        /// <param name="formattedValues">The array of formatted values to insert</param>
        /// <returns></returns>
        protected async Task InsertMultipleValuesWithIndexColumnAsync(string tableName, string columnName, string indexColumnName, object?[] values, string?[]? formattedValues)
        {
            string insertStatement = GetInsertStatementWithIndexColumn(tableName, columnName, indexColumnName, values, formattedValues);
            OutputHelper?.WriteLine(insertStatement);
            AdbcStatement statement = Connection.CreateStatement();
            statement.SqlQuery = insertStatement;
            UpdateResult updateResult = await statement.ExecuteUpdateAsync();
            if (ValidateAffectedRows) Assert.Equal(values.Length, updateResult.AffectedRows);
        }

        /// <summary>
        /// Gets the SQL INSERT statement for inserting multiple values with an index column
        /// </summary>
        /// <param name="tableName">Name of the table to perform the insert</param>
        /// <param name="columnName">Name of the value column to insert</param>
        /// <param name="indexColumnName">Name of the index column to insert index values</param>
        /// <param name="values">The array of values to insert</param>
        /// <param name="formattedValues">The array of formatted values to insert</param>
        /// <returns></returns>
        protected string GetInsertStatementWithIndexColumn(string tableName, string columnName, string indexColumnName, object?[] values, string?[]? formattedValues) =>
            TestEnvironment.GetInsertStatementWithIndexColumn(tableName, columnName, indexColumnName, values, formattedValues);

        /// <summary>
        /// Inserts a single value into a table.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to insert.</param>
        protected async Task InsertSingleValueAsync(string tableName, string columnName, string? value)
        {
            string insertStatement = GetInsertStatement(tableName, columnName, value);
            OutputHelper?.WriteLine(insertStatement);
            AdbcStatement statement = Connection.CreateStatement();
            statement.SqlQuery = insertStatement;
            UpdateResult updateResult = await statement.ExecuteUpdateAsync();
            if (ValidateAffectedRows) Assert.Equal(1, updateResult.AffectedRows);
        }

        /// <summary>
        /// Gets the native SQL statement to insert a single value.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to insert.</param>
        /// <returns></returns>
        protected string GetInsertStatement(string tableName, string columnName, string? value) =>
            TestEnvironment.GetInsertStatement(tableName, columnName, value);

        /// <summary>
        /// Deletes a (single) value from a table.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="whereClause">The WHERE clause string.</param>
        /// <param name="expectedRowsAffected">The expected number of affected rows.</param>
        protected async Task DeleteFromTableAsync(string tableName, string whereClause, int expectedRowsAffected)
        {
            string deleteNumberStatement = GetDeleteValueStatement(tableName, whereClause);
            OutputHelper?.WriteLine(deleteNumberStatement);
            AdbcStatement statement = Connection.CreateStatement();
            statement.SqlQuery = deleteNumberStatement;
            UpdateResult updateResult = await statement.ExecuteUpdateAsync();
            if (ValidateAffectedRows) Assert.Equal(expectedRowsAffected, updateResult.AffectedRows);
        }

        /// <summary>
        /// Gets the native SQL statement to delete (single) value.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="whereClause">The WHERE clause string.</param>
        /// <returns></returns>
        protected string GetDeleteValueStatement(string tableName, string whereClause) => TestEnvironment.GetDeleteValueStatement(tableName, whereClause);

        /// <summary>
        /// Selects a single value and validates it equality with expected value and number of results.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to select and validate.</param>
        /// <param name="expectedLength">The number of expected results (rows).</param>
        /// <returns></returns>
        protected async Task SelectAndValidateValuesAsync(string table, string columnName, object? value, int expectedLength, string? formattedValue = null)
        {
            string selectNumberStatement = GetSelectSingleValueStatement(table, columnName, formattedValue ?? value);
            await SelectAndValidateValuesAsync(selectNumberStatement, value, expectedLength);
        }

        /// <summary>
        /// Selects a single value and validates it equality with expected value and number of results.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="values">The value to select and validate.</param>
        /// <param name="expectedLength">The number of expected results (rows).</param>
        /// <returns></returns>
        protected async Task SelectAndValidateValuesAsync(string table, string columnName, object?[] values, int expectedLength, string? formattedValue = null)
        {
            string selectNumberStatement = GetSelectSingleValueStatement(table, columnName, formattedValue ?? values[0]);
            await SelectAndValidateValuesAsync(selectNumberStatement, values, expectedLength);
        }

        /// <summary>
        /// Selects a single value and validates it equality with expected value and number of results.
        /// </summary>
        /// <param name="selectStatement">The SQL statement to execute.</param>
        /// <param name="value">The value to select and validate.</param>
        /// <param name="expectedLength">The number of expected results (rows).</param>
        /// <returns></returns>
        protected async Task SelectAndValidateValuesAsync(string selectStatement, object? value, int expectedLength)
        {
            await SelectAndValidateValuesAsync(selectStatement, [value], expectedLength);
        }

        private static T? ArrowArrayAs<T>(IArrowArray arrowArray)
            where T : IArrowArray
        {
            if (arrowArray is T t)
            {
                return t;
            }
            return default;
        }

        /// <summary>
        /// Selects a single value and validates it equality with expected value and number of results.
        /// </summary>
        /// <param name="selectStatement">The SQL statement to execute.</param>
        /// <param name="values">The array of values to select and validate.</param>
        /// <param name="expectedLength">The number of expected results (rows).</param>
        /// <returns></returns>
        protected async Task SelectAndValidateValuesAsync(string selectStatement, object?[] values, int expectedLength, bool hasIndexColumn = false)
        {
            AdbcStatement statement = Connection.CreateStatement();
            statement.SqlQuery = selectStatement;
            OutputHelper?.WriteLine(selectStatement);
            QueryResult queryResult = await statement.ExecuteQueryAsync();
            int actualLength = 0;
            using (IArrowArrayStream stream = queryResult.Stream ?? throw new InvalidOperationException("stream is null"))
            {
                Dictionary<ArrowTypeId, Func<IArrowArray, int, object?>> valueGetters = new()
                {
                    { ArrowTypeId.Decimal128, (a, i) => ArrowArrayAs<Decimal128Array>(a)?.GetSqlDecimal(i) },
                    { ArrowTypeId.Double, (a, i) => ArrowArrayAs<DoubleArray>(a)?.GetValue(i) },
                    { ArrowTypeId.Float, (a, i) => ArrowArrayAs<FloatArray>(a)?.GetValue(i) },
                    { ArrowTypeId.Int64, (a, i) => ArrowArrayAs<Int64Array>(a)?.GetValue(i) },
                    { ArrowTypeId.Int32, (a, i) => ArrowArrayAs<Int32Array>(a)?.GetValue(i) },
                    { ArrowTypeId.Int16, (a, i) => ArrowArrayAs<Int16Array>(a)?.GetValue(i) },
                    { ArrowTypeId.Int8, (a, i) => ArrowArrayAs<Int8Array>(a)?.GetValue(i) },
                    { ArrowTypeId.String, (a, i) => ArrowArrayAs<StringArray>(a)?.GetString(i) },
                    { ArrowTypeId.Timestamp, (a, i) => ArrowArrayAs<TimestampArray>(a)?.GetTimestamp(i) },
                    { ArrowTypeId.Date32, (a, i) => ArrowArrayAs<Date32Array>(a)?.GetDateTimeOffset(i) },
                    { ArrowTypeId.Boolean, (a, i) => ArrowArrayAs<BooleanArray>(a)?.GetValue(i) },
                };
                // Assume first column
                Field field = stream.Schema.GetFieldByIndex(0);
                Int32Array? indexArray = null;
                while (true)
                {
                    using (RecordBatch nextBatch = await stream.ReadNextRecordBatchAsync())
                    {
                        if (nextBatch == null) { break; }
                        switch (field.DataType)
                        {
                            case BinaryType:
                                BinaryArray binaryArray = (BinaryArray)nextBatch.Column(0);
                                actualLength += binaryArray.Length;
                                ValidateBinaryArrayValue((i) => values?[i], binaryArray);
                                break;
                            case NullType:
                                NullArray nullArray = (NullArray)nextBatch.Column(0);
                                actualLength += nullArray.Length;
                                ValidateValue(nullArray.Length, (i) => values?[i] == null, (i) => nullArray.IsNull(i));
                                break;
                            default:
                                if (valueGetters.TryGetValue(field.DataType.TypeId, out Func<IArrowArray, int, object?>? valueGetter))
                                {
                                    IArrowArray array = nextBatch.Column(0);
                                    actualLength += array.Length;
                                    indexArray = hasIndexColumn ? (Int32Array)nextBatch.Column(1) : null;
                                    ValidateValue(array.Length, (i) => values?[i], (i) => valueGetter(array, i), indexArray, array.IsNull);
                                }
                                else
                                {
                                    Assert.Fail($"Unhandled datatype {field.DataType}");
                                }
                                break;

                        }
                    }
                }
                Assert.Equal(expectedLength, actualLength);
            }
        }

        /// <summary>
        /// Validates a single values for all results (in the batch).
        /// </summary>
        /// <param name="expectedValues">The value to validate.</param>
        /// <param name="binaryArray">The binary array to validate</param>
        private static void ValidateBinaryArrayValue(Func<int, object?> expectedValues, BinaryArray binaryArray)
        {
            for (int i = 0; i < binaryArray.Length; i++)
            {
                // Note: null is indicated in output flag 'isNull'.
                byte[] byteArray = binaryArray.GetBytes(i, out bool isNull).ToArray();
                byte[]? nullableByteArray = isNull ? null : byteArray;
                var expectedValue = expectedValues(i);
                Assert.Equal<object>(expectedValue, nullableByteArray);
            }
        }

        /// <summary>
        /// Validates a single values for all results (in the batch).
        /// </summary>
        /// <param name="length">The length of the current batch/array.</param>
        /// <param name="value">The value to validate.</param>
        /// <param name="getter">The getter function to retrieve the actual value.</param>
        /// <param name="indexColumn"></param>
        private static void ValidateValue(int length, Func<int, object?> value, Func<int, object?> getter, Int32Array? indexColumn = null, Func<int, bool>? isNullEvaluator = default)
        {
            for (int i = 0; i < length; i++)
            {
                int valueIndex = indexColumn?.GetValue(i) ?? i;
                object? expected = value(valueIndex);
                if (isNullEvaluator != null)
                {
                    Assert.Equal(expected == null, isNullEvaluator(i));
                }
                object? actual = getter(i);
                Assert.Equal<object>(expected, actual);
            }
        }

        /// <summary>
        /// Gets the native SQL statement to select the (single) value from the table.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to select and validate.</param>
        /// <returns>The native SQL statement.</returns>
        protected string GetSelectSingleValueStatement(string table, string columnName, object? value) =>
            $"SELECT {columnName} FROM {table} {GetWhereClause(columnName, value)}";

        protected string GetWhereClause(string columnName, object? value) =>
            value == null
                ? $"WHERE {columnName} IS NULL"
                : string.Format("WHERE {0} = {1}", columnName, MaybeDoubleToString(value));

        private static object MaybeDoubleToString(object value) =>
            value.GetType() == typeof(float)
                ? ConvertFloatToString((float)value)
                : value.GetType() == typeof(double)
                    ? ConvertDoubleToString((double)value)
                    : value;

        /// <summary>
        /// Converts double values to it's String equivalent.
        /// </summary>
        /// <param name="value">The double value to convert.</param>
        /// <returns>The String representation of the double value.</returns>
        protected static string ConvertDoubleToString(double value)
        {
            switch (value)
            {
                case double.PositiveInfinity:
                    return "CAST('inf' as DOUBLE)";
                case double.NegativeInfinity:
                    return "CAST('-inf' as DOUBLE)";
                case double.NaN:
                    return "CAST('Nan' as DOUBLE)";
#if NET472
                // Double.ToString() rounds max/min values, causing Snowflake to store +/- infinity
                case double.MaxValue:
                    return "1.7976931348623157E+308";
                case double.MinValue:
                    return "-1.7976931348623157E+308";
#endif
                default:
                    return value.ToString(CultureInfo.InvariantCulture);
            }
        }

        /// <summary>
        /// Converts float values to it's String equivalent.
        /// </summary>
        /// <param name="value">The float value to convert.</param>
        /// <returns>The String representation of the float value.</returns>
        protected static string ConvertFloatToString(float value)
        {
            switch (value)
            {
                case float.PositiveInfinity:
                    return "CAST('inf' as FLOAT)";
                case float.NegativeInfinity:
                    return "CAST('-inf' as FLOAT)";
                case float.NaN:
                    return "CAST('NaN' as FLOAT)";
#if NET472
                // Float.ToString() rounds max/min values, causing Snowflake to store +/- infinity
                case float.MaxValue:
                    return "3.40282347E+38";
                case float.MinValue:
                    return "-3.40282347E+38";
#endif
                default:
                    return value.ToString(CultureInfo.InvariantCulture);
            }
        }

        /// <summary>
        /// Disposes managed and unmanaged resources.
        /// </summary>
        /// <param name="disposing">Indicator of whether this method is called explicitly or implicitly.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    if (_statement.IsValueCreated)
                    {
                        _statement.Value.Dispose();
                    }
                    if (_connection.IsValueCreated)
                    {
                        _connection.Value.Dispose();
                    }
                }

                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected static string QuoteValue(string value)
        {
            return $"'{value.Replace("'", "''")}'";
        }

        protected string DelimitIdentifier(string value)
        {
            return $"{Delimiter}{value.Replace(Delimiter, $"{Delimiter}{Delimiter}")}{Delimiter}";
        }

        protected string Delimiter => TestEnvironment.Delimiter;

        protected static void AssertContainsAll(string[] expectedTexts, string value)
        {
            if (expectedTexts == null) { return; };
            foreach (string text in expectedTexts)
            {
                Assert.Contains(text, value);
            }
        }

        /// <summary>
        /// Gets test patterns for GetObject identifier matching.
        /// </summary>
        /// <param name="name">The idenytifier to create a pattern for.</param>
        /// <returns>An enumeration of patterns to match produced from the identifier.</returns>
        protected static IEnumerable<object[]> GetPatterns(string? name)
        {
            if (name == null) yield break;

            yield return new object[] { name! };
            if (!string.IsNullOrEmpty(name))
            {
                yield return new object[] { $"{GetPartialNameForPatternMatch(name!)}%" };
                yield return new object[] { $"{GetPartialNameForPatternMatch(name!).ToLower()}%" };
                yield return new object[] { $"{GetPartialNameForPatternMatch(name!).ToUpper()}%" };
                yield return new object[] { $"_{GetNameWithoutFirstChatacter(name!)}" };
                yield return new object[] { $"_{GetNameWithoutFirstChatacter(name!).ToLower()}" };
                yield return new object[] { $"_{GetNameWithoutFirstChatacter(name!).ToUpper()}" };
            }
        }

        private static string GetPartialNameForPatternMatch(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length == 1) return name;

            return name.Substring(0, name.Length / 2);
        }

        private static string GetNameWithoutFirstChatacter(string name)
        {
            if (string.IsNullOrEmpty(name)) return name;

            return name.Substring(1);
        }

        protected virtual void CreateNewTableName(out string tableName, out string fullTableName)
        {
            string catalogName = TestConfiguration.Metadata.Catalog;
            string schemaName = TestConfiguration.Metadata.Schema;
            tableName = Guid.NewGuid().ToString("N");
            string catalogFormatted = string.IsNullOrEmpty(catalogName) ? string.Empty : DelimitIdentifier(catalogName) + ".";
            fullTableName = $"{catalogFormatted}{DelimitIdentifier(schemaName)}.{DelimitIdentifier(tableName)}";
        }

        /// <summary>
        /// Represents a temporary table that can create and drop the table automatically.
        /// </summary>
        public class TemporaryTable : IDisposable
        {
            private bool _disposedValue;
            private readonly AdbcConnection _connection;

            /// <summary>
            /// Gets the name of the table.
            /// </summary>
            public string TableName { get; }

            private TemporaryTable(AdbcConnection connection, string tableName)
            {
                TableName = tableName;
                _connection = connection;
            }

            /// <summary>
            /// Creates a new instance of a temporary table.
            /// </summary>
            /// <param name="connection">The ADBC statement to run the create table query.</param>
            /// <param name="tableName">The name of temporary table to create.</param>
            /// <param name="sqlUpdate">The SQL query to create the table in the native SQL dialect.</param>
            /// <returns></returns>
            public static async Task<TemporaryTable> NewTemporaryTableAsync(AdbcConnection connection, string tableName, string sqlUpdate, ITestOutputHelper? outputHelper = default)
            {
                AdbcStatement statement = connection.CreateStatement();
                statement.SqlQuery = sqlUpdate;
                outputHelper?.WriteLine(sqlUpdate);
                await statement.ExecuteUpdateAsync();
                return new TemporaryTable(connection, tableName);
            }

            /// <summary>
            /// Drops the tables.
            /// </summary>
            protected virtual async Task DropAsync()
            {
                AdbcStatement statement = _connection.CreateStatement();
                statement.SqlQuery = $"DROP TABLE {TableName}";
                await statement.ExecuteUpdateAsync();
            }

            protected virtual void Dispose(bool disposing)
            {
                if (!_disposedValue)
                {
                    if (disposing)
                    {
                        DropAsync().Wait();
                    }

                    _disposedValue = true;
                }
            }

            public void Dispose()
            {
                // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
                Dispose(disposing: true);
                GC.SuppressFinalize(this);
            }
        }

        protected class TemporarySchema : IDisposable
        {
            private bool _disposedValue;
            private readonly AdbcConnection _connection;

            private TemporarySchema(string catalogName, AdbcConnection connection)
            {
                CatalogName = catalogName;
                SchemaName = Guid.NewGuid().ToString("N");
                _connection = connection;
            }

            public static async ValueTask<TemporarySchema> NewTemporarySchemaAsync(string catalogName, AdbcConnection connection)
            {
                TemporarySchema schema = new TemporarySchema(catalogName, connection);
                string catalog = string.IsNullOrEmpty(schema.CatalogName) ? string.Empty : schema.CatalogName + ".";
                AdbcStatement statement = connection.CreateStatement();
                statement.SqlQuery = $"CREATE SCHEMA IF NOT EXISTS {catalog}{schema.SchemaName}";
                await statement.ExecuteUpdateAsync();
                return schema;
            }

            public string CatalogName { get; }

            public string SchemaName { get; }

            protected virtual void Dispose(bool disposing)
            {
                if (!_disposedValue)
                {
                    if (disposing)
                    {
                        string catalog = string.IsNullOrEmpty(CatalogName) ? string.Empty : CatalogName + ".";
                        AdbcStatement statement = _connection.CreateStatement();
                        statement.SqlQuery = $"DROP SCHEMA IF EXISTS {catalog}{SchemaName}";
                        statement.ExecuteUpdate();
                    }

                    _disposedValue = true;
                }
            }

            public void Dispose()
            {
                // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
                Dispose(disposing: true);
                GC.SuppressFinalize(this);
            }
        }
    }
}

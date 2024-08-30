﻿/*
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
        public TestBase(ITestOutputHelper? outputHelper, TestEnvironment<TConfig>.Factory<TEnv> testEnvFacltory)
        {
            OutputHelper = outputHelper;
            _testEnvFactory = testEnvFacltory;
            _testEnvironment = new Lazy<TEnv>(() => _testEnvFactory.Create(() => Connection));
            _testConfiguration = new Lazy<TConfig>(() => Utils.LoadTestConfiguration<TConfig>(TestConfigVariable));
            _connection = new Lazy<AdbcConnection>(() => NewConnection());
            _statement = new Lazy<AdbcStatement>(() => Connection.CreateStatement());
        }

        /// <summary>
        /// Gets the test ouput helper.
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
        /// <param name="statement">The ADBC statement to apply the update.</param>
        /// <param name="columns">The columns definition in the native SQL dialect.</param>
        /// <returns>A disposable temporary table object that will drop the table when disposed.</returns>
        protected async Task<TemporaryTable> NewTemporaryTableAsync(AdbcStatement statement, string columns)
        {
            string tableName = NewTableName();
            string sqlUpdate = TestEnvironment.GetCreateTemporaryTableStatement(tableName, columns);
            return await TemporaryTable.NewTemporaryTableAsync(statement, tableName, sqlUpdate);
        }

        /// <summary>
        /// Creates a new unique table name .
        /// </summary>
        /// <returns>A unique table name.</returns>
        protected string NewTableName() => string.Format(
                        "{0}{1}{2}",
                        string.IsNullOrEmpty(TestConfiguration.Metadata.Catalog) ? string.Empty : DelimitIdentifier(TestConfiguration.Metadata.Catalog) + ".",
                        string.IsNullOrEmpty(TestConfiguration.Metadata.Schema) ? string.Empty : DelimitIdentifier(TestConfiguration.Metadata.Schema) + ".",
                        DelimitIdentifier(Guid.NewGuid().ToString().Replace("-", ""))
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
        /// <returns>Ditionary of parameters for the driver.</returns>
        protected virtual Dictionary<string, string> GetDriverParameters(TConfig testConfiguration) => TestEnvironment.GetDriverParameters(testConfiguration);

        /// <summary>
        /// Gets a single ADBC Connection for the object.
        /// </summary>
        protected AdbcConnection Connection => _connection.Value;

        /// <summary>
        /// Gets as single ADBC Statement for the object.
        /// </summary>
        protected AdbcStatement Statement => _statement.Value;

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

        /// <summary>
        /// Gets a the Spark ADBC driver with settings from the <see cref="SparkTestConfiguration"/>.
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
        /// <param name="formattedValue">The formated value to insert, select and delete.</param>
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
        /// <param name="formattedValue">The formated value to insert, select and delete.</param>
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
        /// <param name="formattedValue">The formated value to insert, select and delete.</param>
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
            Statement.SqlQuery = insertStatement;
            UpdateResult updateResult = await Statement.ExecuteUpdateAsync();
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
            Statement.SqlQuery = insertStatement;
            UpdateResult updateResult = await Statement.ExecuteUpdateAsync();
            if (ValidateAffectedRows) Assert.Equal(1, updateResult.AffectedRows);
        }

        /// <summary>
        /// Gets the native SQL statment to insert a single value.
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
            Statement.SqlQuery = deleteNumberStatement;
            UpdateResult updateResult = await Statement.ExecuteUpdateAsync();
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

        /// <summary>
        /// Selects a single value and validates it equality with expected value and number of results.
        /// </summary>
        /// <param name="selectStatement">The SQL statement to execute.</param>
        /// <param name="values">The array of values to select and validate.</param>
        /// <param name="expectedLength">The number of expected results (rows).</param>
        /// <returns></returns>
        protected async Task SelectAndValidateValuesAsync(string selectStatement, object?[] values, int expectedLength, bool hasIndexColumn = false)
        {
            Statement.SqlQuery = selectStatement;
            OutputHelper?.WriteLine(selectStatement);
            QueryResult queryResult = await Statement.ExecuteQueryAsync();
            int actualLength = 0;
            using (IArrowArrayStream stream = queryResult.Stream ?? throw new InvalidOperationException("stream is null"))
            {
                // Assume first column
                Field field = stream.Schema.GetFieldByIndex(0);
                while (true)
                {
                    using (RecordBatch nextBatch = await stream.ReadNextRecordBatchAsync())
                    {
                        if (nextBatch == null) { break; }
                        switch (field.DataType)
                        {
                            case Decimal128Type:
                                Decimal128Array decimalArray = (Decimal128Array)nextBatch.Column(0);
                                actualLength += decimalArray.Length;
                                ValidateValue((i) => values?[i], decimalArray.Length, (i) => decimalArray.GetSqlDecimal(i));
                                break;
                            case DoubleType:
                                DoubleArray doubleArray = (DoubleArray)nextBatch.Column(0);
                                actualLength += doubleArray.Length;
                                ValidateValue((i) => values?[i], doubleArray.Length, (i) => doubleArray.GetValue(i));
                                break;
                            case FloatType:
                                FloatArray floatArray = (FloatArray)nextBatch.Column(0);
                                actualLength += floatArray.Length;
                                ValidateValue((i) => values?[i], floatArray.Length, (i) => floatArray.GetValue(i));
                                break;
                            case Int64Type:
                                Int64Array int64Array = (Int64Array)nextBatch.Column(0);
                                actualLength += int64Array.Length;
                                ValidateValue((i) => values?[i], int64Array.Length, (i) => int64Array.GetValue(i));
                                break;
                            case Int32Type:
                                Int32Array intArray = (Int32Array)nextBatch.Column(0);
                                actualLength += intArray.Length;
                                ValidateValue((i) => values?[i], intArray.Length, (i) => intArray.GetValue(i));
                                break;
                            case Int16Type:
                                Int16Array shortArray = (Int16Array)nextBatch.Column(0);
                                actualLength += shortArray.Length;
                                ValidateValue((i) => values?[i], shortArray.Length, (i) => shortArray.GetValue(i));
                                break;
                            case Int8Type:
                                Int8Array tinyIntArray = (Int8Array)nextBatch.Column(0);
                                actualLength += tinyIntArray.Length;
                                ValidateValue((i) => values?[i], tinyIntArray.Length, (i) => tinyIntArray.GetValue(i));
                                break;
                            case StringType:
                                StringArray stringArray = (StringArray)nextBatch.Column(0);
                                actualLength += stringArray.Length;
                                ValidateValue((i) => values?[i], stringArray.Length, (i) => stringArray.GetString(i));
                                break;
                            case TimestampType:
                                TimestampArray timestampArray = (TimestampArray)nextBatch.Column(0);
                                actualLength += timestampArray.Length;
                                ValidateValue((i) => values?[i], timestampArray.Length, (i) => timestampArray.GetTimestamp(i));
                                break;
                            case Date32Type:
                                Date32Array date32Array = (Date32Array)nextBatch.Column(0);
                                actualLength += date32Array.Length;
                                ValidateValue((i) => values?[i], date32Array.Length, (i) => date32Array.GetDateTimeOffset(i));
                                break;
                            case BooleanType:
                                BooleanArray booleanArray = (BooleanArray)nextBatch.Column(0);
                                Int32Array? indexArray = hasIndexColumn ? (Int32Array)nextBatch.Column(1) : null;
                                actualLength += booleanArray.Length;
                                ValidateValue((i) => values?[i], booleanArray.Length, (i) => booleanArray.GetValue(i), indexArray);
                                break;
                            case BinaryType:
                                BinaryArray binaryArray = (BinaryArray)nextBatch.Column(0);
                                actualLength += binaryArray.Length;
                                ValidateBinaryArrayValue((i) => values?[i], binaryArray);
                                break;
                            case NullType:
                                NullArray nullArray = (NullArray)nextBatch.Column(0);
                                actualLength += nullArray.Length;
                                ValidateValue((i) => values?[i] == null, nullArray.Length, (i) => nullArray.IsNull(i));
                                break;
                            default:
                                Assert.Fail($"Unhandled datatype {field.DataType}");
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
        /// <param name="value">The value to validate.</param>
        /// <param name="length">The length of the current batch/array.</param>
        /// <param name="getter">The getter function to retrieve the actual value.</param>
        private static void ValidateValue(Func<int, object?> value, int length, Func<int, object?> getter, Int32Array? indexColumn = null)
        {
            for (int i = 0; i < length; i++)
            {
                int valueIndex = indexColumn?.GetValue(i) ?? i;
                object? expected = value(valueIndex);
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
                    return "double('infinity')";
                case double.NegativeInfinity:
                    return "double('-infinity')";
                case double.NaN:
                    return "double('NaN')";
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
                    return "float('infinity')";
                case float.NegativeInfinity:
                    return "float('-infinity')";
                case float.NaN:
                    return "float('NaN')";
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

        /// <summary>
        /// Represents a temporary table that can create and drop the table automatically.
        /// </summary>
        public class TemporaryTable : IDisposable
        {
            private bool _disposedValue;
            private readonly AdbcStatement _statement;

            /// <summary>
            /// Gets the name of the table.
            /// </summary>
            public string TableName { get; }

            private TemporaryTable(AdbcStatement statement, string tableName)
            {
                TableName = tableName;
                _statement = statement;
            }

            /// <summary>
            /// Creates a new instance of a temporary table.
            /// </summary>
            /// <param name="statement">The ADBC statement to run the create table query.</param>
            /// <param name="tableName">The name of temporary table to create.</param>
            /// <param name="sqlUpdate">The SQL query to create the table in the native SQL dialect.</param>
            /// <returns></returns>
            public static async Task<TemporaryTable> NewTemporaryTableAsync(AdbcStatement statement, string tableName, string sqlUpdate)
            {
                statement.SqlQuery = sqlUpdate;
                await statement.ExecuteUpdateAsync();
                return new TemporaryTable(statement, tableName);
            }

            /// <summary>
            /// Drops the tables.
            /// </summary>
            protected virtual async Task DropAsync()
            {
                _statement.SqlQuery = $"DROP TABLE {TableName}";
                await _statement.ExecuteUpdateAsync();
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
            private readonly AdbcStatement _statement;

            private TemporarySchema(string catalogName, AdbcStatement statement)
            {
                CatalogName = catalogName;
                SchemaName = Guid.NewGuid().ToString().Replace("-", "");
                _statement = statement;
            }

            public static async ValueTask<TemporarySchema> NewTemporarySchemaAsync(string catalogName, AdbcStatement statement)
            {
                TemporarySchema schema = new TemporarySchema(catalogName, statement);
                string catalog = string.IsNullOrEmpty(schema.CatalogName) ? string.Empty : schema.CatalogName + ".";
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
                        _statement.SqlQuery = $"DROP SCHEMA IF EXISTS {catalog}{SchemaName}";
                        _statement.ExecuteUpdate();
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

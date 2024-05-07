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
    /// <typeparam name="T">A TestConfiguration type to use when accessing test configuration files.</typeparam>
    public abstract class TestBase<T> : IDisposable where T : TestConfiguration
    {
        private bool _disposedValue;
        private T? _testConfiguration;
        private AdbcConnection? _connection = null;
        private AdbcStatement? _statement = null;

        /// <summary>
        /// Constructs a new TestBase object with an output helper.
        /// </summary>
        /// <param name="outputHelper">Test output helper for writing test output.</param>
        public TestBase(ITestOutputHelper? outputHelper)
        {
            OutputHelper = outputHelper;
        }

        /// <summary>
        /// Gets the test ouput helper.
        /// </summary>
        protected ITestOutputHelper? OutputHelper { get; }

        /// <summary>
        /// The name of the environment variable that stores the full location of the test configuration file.
        /// </summary>
        protected abstract string TestConfigVariable { get; }

        /// <summary>
        /// Creates a temporary table (if possible) using the native SQL dialect.
        /// </summary>
        /// <param name="statement">The ADBC statement to apply the update.</param>
        /// <param name="columns">The columns definition in the native SQL dialect.</param>
        /// <returns>A disposable temporary table object that will drop the table when disposed.</returns>
        protected virtual async ValueTask<TemporaryTable> NewTemporaryTableAsync(AdbcStatement statement, string columns)
        {
            string tableName = NewTableName();
            string sqlUpdate = string.Format("CREATE TEMPORARY IF NOT EXISTS TABLE {0} ({1})", tableName, columns);
            return await TemporaryTable.NewTemporaryTableAsync(statement, tableName, sqlUpdate);
        }

        /// <summary>
        /// Creates a new unique table name .
        /// </summary>
        /// <returns>A unique table name.</returns>
        protected virtual string NewTableName() => string.Format(
                        "{0}.{1}.{2}",
                        TestConfiguration.Metadata.Catalog,
                        TestConfiguration.Metadata.Schema,
                        Guid.NewGuid().ToString().Replace("-", "")
                    );

        /// <summary>
        /// Gets the relative resource location of source SQL data used in driver testing.
        /// </summary>
        protected abstract string SqlDataResourceLocation { get; }

        /// <summary>
        /// Creates a new driver.
        /// </summary>
        protected abstract AdbcDriver NewDriver { get; }

        /// <summary>
        /// Gets the parameters from the test configuration that are passed to the driver as a dictionary.
        /// </summary>
        /// <param name="testConfiguration">The test configuration as input.</param>
        /// <returns>Ditionary of parameters for the driver.</returns>
        protected abstract Dictionary<string, string> GetDriverParameters(T testConfiguration);

        /// <summary>
        /// Gets a single ADBC Connection for the object.
        /// </summary>
        protected AdbcConnection Connection
        {
            get
            {
                if (_connection == null)
                {
                    _connection = NewConnection();
                }
                return _connection;
            }
        }

        /// <summary>
        /// Gets as single ADBC Statement for the object.
        /// </summary>
        protected AdbcStatement Statement
        {
            get
            {
                if (_statement == null)
                {
                    _statement = Connection.CreateStatement();
                }
                return _statement;
            }
        }

        /// <summary>
        /// Gets the test configuration file.
        /// </summary>
        protected T TestConfiguration
        {
            get
            {
                if (_testConfiguration == null)
                {
                    _testConfiguration = Utils.LoadTestConfiguration<T>(TestConfigVariable);
                }
                return _testConfiguration;
            }
        }

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
                    string modifiedLine = line.Replace(placeholder, $"{TestConfiguration.Metadata.Catalog}.{TestConfiguration.Metadata.Schema}.{TestConfiguration.Metadata.Table}");
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
        protected AdbcConnection NewConnection(T? testConfiguration = null, IReadOnlyDictionary<string, string>? connectionOptions = null)
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
            await DeleteFromTableAsync(tableName, whereClause, 1);
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
            await DeleteFromTableAsync(tableName, whereClause, 1);
        }

        /// <summary>
        /// Inserts a single value into a table.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to insert.</param>
        protected virtual async Task InsertSingleValueAsync(string tableName, string columnName, string? value)
        {
            string insertNumberStatement = GetInsertValueStatement(tableName, columnName, value);
            OutputHelper?.WriteLine(insertNumberStatement);
            Statement.SqlQuery = insertNumberStatement;
            UpdateResult updateResult = await Statement.ExecuteUpdateAsync();
            Assert.Equal(1, updateResult.AffectedRows);
        }

        /// <summary>
        /// Gets the native SQL statment to insert a single value.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to insert.</param>
        /// <returns></returns>
        protected virtual string GetInsertValueStatement(string tableName, string columnName, string? value) =>
            string.Format("INSERT INTO {0} ({1}) VALUES ({2});", tableName, columnName, value ?? "NULL");

        /// <summary>
        /// Deletes a (single) value from a table.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="whereClause">The WHERE clause string.</param>
        /// <param name="expectedRowsAffected">The expected number of affected rows.</param>
        protected virtual async Task DeleteFromTableAsync(string tableName, string whereClause, int expectedRowsAffected)
        {
            string deleteNumberStatement = GetDeleteValueStatement(tableName, whereClause);
            OutputHelper?.WriteLine(deleteNumberStatement);
            Statement.SqlQuery = deleteNumberStatement;
            UpdateResult updateResult = await Statement.ExecuteUpdateAsync();
            Assert.Equal(expectedRowsAffected, updateResult.AffectedRows);
        }

        /// <summary>
        /// Gets the native SQL statement to delete (single) value.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="whereClause">The WHERE clause string.</param>
        /// <returns></returns>
        protected virtual string GetDeleteValueStatement(string tableName, string whereClause) =>
            string.Format("DELETE FROM {0} WHERE {1};", tableName, whereClause);

        /// <summary>
        /// Selects a single value and validates it equality with expected value and number of results.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to select and validate.</param>
        /// <param name="expectedLength">The number of expected results (rows).</param>
        /// <returns></returns>
        protected virtual async Task SelectAndValidateValuesAsync(string table, string columnName, object? value, int expectedLength, string? formattedValue = null)
        {
            string selectNumberStatement = GetSelectSingleValueStatement(table, columnName, formattedValue ?? value);
            await SelectAndValidateValuesAsync(selectNumberStatement, value, expectedLength);
        }

        /// <summary>
        /// Selects a single value and validates it equality with expected value and number of results.
        /// </summary>
        /// <param name="selectStatement">The SQL statement to execute.</param>
        /// <param name="value">The value to select and validate.</param>
        /// <param name="expectedLength">The number of expected results (rows).</param>
        /// <returns></returns>
        protected virtual async Task SelectAndValidateValuesAsync(string selectStatement, object? value, int expectedLength)
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
                                ValidateValue(value, decimalArray.Length, (i) => decimalArray.GetSqlDecimal(i));
                                break;
                            case DoubleType:
                                DoubleArray doubleArray = (DoubleArray)nextBatch.Column(0);
                                actualLength += doubleArray.Length;
                                ValidateValue(value, doubleArray.Length, (i) => doubleArray.GetValue(i));
                                break;
                            case FloatType:
                                FloatArray floatArray = (FloatArray)nextBatch.Column(0);
                                actualLength += floatArray.Length;
                                ValidateValue(value, floatArray.Length, (i) => floatArray.GetValue(i));
                                break;
                            case Int64Type:
                                Int64Array int64Array = (Int64Array)nextBatch.Column(0);
                                actualLength += int64Array.Length;
                                ValidateValue(value, int64Array.Length, (i) => int64Array.GetValue(i));
                                break;
                            case Int32Type:
                                Int32Array intArray = (Int32Array)nextBatch.Column(0);
                                actualLength += intArray.Length;
                                ValidateValue(value, intArray.Length, (i) => intArray.GetValue(i));
                                break;
                            case Int16Type:
                                Int16Array shortArray = (Int16Array)nextBatch.Column(0);
                                actualLength += shortArray.Length;
                                ValidateValue(value, shortArray.Length, (i) => shortArray.GetValue(i));
                                break;
                            case Int8Type:
                                Int8Array tinyIntArray = (Int8Array)nextBatch.Column(0);
                                actualLength += tinyIntArray.Length;
                                ValidateValue(value, tinyIntArray.Length, (i) => tinyIntArray.GetValue(i));
                                break;
                            case StringType:
                                StringArray stringArray = (StringArray)nextBatch.Column(0);
                                actualLength += stringArray.Length;
                                ValidateValue(value, stringArray.Length, (i) => stringArray.GetString(i));
                                break;
                            case TimestampType:
                                TimestampArray timestampArray = (TimestampArray)nextBatch.Column(0);
                                actualLength += timestampArray.Length;
                                ValidateValue(value, timestampArray.Length, (i) => timestampArray.GetTimestamp(i));
                                break;
                            case Date32Type:
                                Date32Array date32Array = (Date32Array)nextBatch.Column(0);
                                actualLength += date32Array.Length;
                                ValidateValue(value, date32Array.Length, (i) => date32Array.GetDateTimeOffset(i));
                                break;
                            case BooleanType:
                                BooleanArray booleanArray = (BooleanArray)nextBatch.Column(0);
                                actualLength += booleanArray.Length;
                                ValidateValue(value, booleanArray.Length, (i) => booleanArray.GetValue(i));
                                break;
                            case BinaryType:
                                BinaryArray binaryArray = (BinaryArray)nextBatch.Column(0);
                                actualLength += binaryArray.Length;
                                ValidateValue(value, binaryArray.Length, (i) => binaryArray.GetBytes(i).ToArray());
                                break;
                            case NullType:
                                NullArray nullArray = (NullArray)nextBatch.Column(0);
                                actualLength += nullArray.Length;
                                ValidateValue(value == null, nullArray.Length, (i) => nullArray.IsNull(i));
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
        /// <param name="value">The value to validate.</param>
        /// <param name="length">The length of the current batch/array.</param>
        /// <param name="getter">The getter function to retrieve the actual value.</param>
        private static void ValidateValue(object? value, int length, Func<int, object?> getter)
        {
            for (int i = 0; i < length; i++)
            {
                Assert.Equal(value, getter(i));
            }
        }

        /// <summary>
        /// Gets the native SQL statement to select the (single) value from the table.
        /// </summary>
        /// <param name="tableName">The name of the table to use.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="value">The value to select and validate.</param>
        /// <returns>The native SQL statement.</returns>
        protected virtual string GetSelectSingleValueStatement(string table, string columnName, object? value) =>
            $"SELECT {columnName} FROM {table} WHERE {GetWhereClause(columnName, value)}";

        protected virtual string GetWhereClause(string columnName, object? value) =>
            value == null
                ? $"{columnName} IS NULL"
                : string.Format("{0} = {1}", columnName, MaybeDoubleToString(value));

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
                    return "'inf'";
                case double.NegativeInfinity:
                    return "'-inf'";
                case double.NaN:
                    return "'NaN'";
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
                    return "'inf'";
                case float.NegativeInfinity:
                    return "'-inf'";
                case float.NaN:
                    return "'NaN'";
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
                    if (_statement != null)
                    {
                        _statement.Dispose();
                        _statement = null;
                    }
                    if (_connection != null)
                    {
                        _connection.Dispose();
                        _connection = null;
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

        protected virtual string DelimitIdentifier(string value)
        {
            return $"{Delimiter}{value.Replace(Delimiter, $"{Delimiter}{Delimiter}")}{Delimiter}";
        }

        protected virtual string Delimiter => "\"";

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
            if (string.IsNullOrEmpty(name)) yield break;

            yield return new object[] { name! };
            yield return new object[] { $"{GetPartialNameForPatternMatch(name!)}%" };
            yield return new object[] { $"{GetPartialNameForPatternMatch(name!).ToLower()}%" };
            yield return new object[] { $"{GetPartialNameForPatternMatch(name!).ToUpper()}%" };
            yield return new object[] { $"_{GetNameWithoutFirstChatacter(name!)}" };
            yield return new object[] { $"_{GetNameWithoutFirstChatacter(name!).ToLower()}" };
            yield return new object[] { $"_{GetNameWithoutFirstChatacter(name!).ToUpper()}" };
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
        protected class TemporaryTable : IDisposable
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
            public static async ValueTask<TemporaryTable> NewTemporaryTableAsync(AdbcStatement statement, string tableName, string sqlUpdate)
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
                statement.SqlQuery = $"CREATE SCHEMA IF NOT EXISTS {schema.CatalogName}.{schema.SchemaName}";
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
                        _statement.SqlQuery = $"DROP SCHEMA IF EXISTS {CatalogName}.{SchemaName}";
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

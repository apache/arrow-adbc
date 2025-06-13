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
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    public class CastTests : IDisposable
    {
        private const string ARRAY = "ARRAY";
        private const string BOOLEAN = "BOOLEAN";
        private const string NUMERIC = "NUMERIC";
        private const string OBJECT = "OBJECT";
        private const string VARCHAR = "VARCHAR";
        private const string VARIANT = "VARIANT";
        private const string TIMESTAMP_TZ = "TIMESTAMP_TZ";
        private const string DOUBLE = "DOUBLE";
        private const string BIGINT = "BIGINT";
        private const string INTEGER = "INTEGER";
        private const string TO_VARCHAR = "TO_VARCHAR";
        private const string TO_DOUBLE = "TO_DOUBLE";
        private const string TO_BOOLEAN = "TO_BOOLEAN";
        private const string TO_NUMERIC = "TO_NUMERIC";
        private const string TO_TIMESTAMP_TZ = "TO_TIMESTAMP_TZ";
        private const string TO_DATE = "TO_DATE";
        private const string TO_TIME = "TO_TIME";
        private const string TO_ARRAY = "TO_ARRAY";
        private const string TO_VARIANT = "TO_VARIANT";
        private const string TO_OBJECT = "TO_OBJECT";
        private const string TRY_TO_NUMERIC = "TRY_TO_NUMERIC";
        private const string TRY_TO_BOOLEAN = "TRY_TO_BOOLEAN";
        private const string TRY_TO_TIMESTAMP_TZ = "TRY_TO_TIMESTAMP_TZ";
        private const string COLUMN_NAME = "SOURCE_COLUMN";
        static readonly string s_testTablePrefix = "ADBCCASTTEST_"; // Make configurable? Also; must be all caps if not double quoted
        readonly SnowflakeTestConfiguration _snowflakeTestConfiguration;
        readonly AdbcConnection _connection;
        readonly AdbcStatement _statement;
        readonly string _catalogSchema;
        private readonly ITestOutputHelper _output;
        private bool _disposed = false;

        /// <summary>
        /// Validates that specific conversion and casting functions perform correctly.
        /// Note: Does not test the generic CAST and TRY_CAST, but instead uses the
        /// direct conversion functions.
        /// </summary>
        public CastTests(ITestOutputHelper output)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
            _snowflakeTestConfiguration = SnowflakeTestingUtils.TestConfiguration;
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_snowflakeTestConfiguration, out parameters);
            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            _connection = adbcDatabase.Connect(options);
            _statement = _connection.CreateStatement();
            _catalogSchema = string.Format("{0}.{1}", _snowflakeTestConfiguration.Metadata.Catalog, _snowflakeTestConfiguration.Metadata.Schema);
            _output = output;
            // Simplify TIMEZONE tests so we don't have to deal with time zone offsets.
            SetSessionTimezone(_statement, "UTC");
        }

        [SkippableTheory]
        // From NUMERIC
        [InlineData(BIGINT, "2345", "2345", ArrowTypeId.String, TO_VARCHAR)]
        [InlineData(INTEGER, "2345", "2345", ArrowTypeId.String, TO_VARCHAR)]
        [InlineData(NUMERIC + "(1,0)", "-9", "-09.00", ArrowTypeId.String, TO_VARCHAR, COLUMN_NAME + ", '00.00'")]
        [InlineData(NUMERIC + "(1,0)", "9", "09.00 ", ArrowTypeId.String, TO_VARCHAR, COLUMN_NAME + ", '00.00MI'")]
        [InlineData(NUMERIC + "(38,2)", "9.50", 9.5, ArrowTypeId.Double, TO_DOUBLE)]
        [InlineData(NUMERIC + "(1,0)", "1", true, ArrowTypeId.Boolean, TO_BOOLEAN)]
        [InlineData(NUMERIC + "(1,0)", "0", false, ArrowTypeId.Boolean, TO_BOOLEAN)]
        // From DOUBLE
        [InlineData(DOUBLE, "2345.67", "2345.67", ArrowTypeId.String, TO_VARCHAR)]
        [InlineData(DOUBLE, "2345.67", 2345.67, ArrowTypeId.Double, TO_DOUBLE)]
        [InlineData(DOUBLE, "2345.5", 2346, ArrowTypeId.Decimal128, TO_NUMERIC)] // Rounded up
        [InlineData(DOUBLE, "2345.4", 2345, ArrowTypeId.Decimal128, TO_NUMERIC)] // Rounded down
        [InlineData(DOUBLE, "2345.67", 2345.67, ArrowTypeId.Decimal128, TO_NUMERIC, COLUMN_NAME + ", 6, 2")]
        // From BOOLEAN
        [InlineData(BOOLEAN, "'true'", "true", ArrowTypeId.String, TO_VARCHAR)]
        [InlineData(BOOLEAN, "'false'", "false", ArrowTypeId.String, TO_VARCHAR)]
        // From VARCHAR
        [InlineData(VARCHAR, "'欢迎'", "欢迎", ArrowTypeId.String, TO_VARCHAR)]
        [InlineData(VARCHAR, "'123'", 123, ArrowTypeId.Decimal128, TO_NUMERIC)]
        [InlineData(VARCHAR, "'123.45'", 123.45, ArrowTypeId.Decimal128, TO_NUMERIC, COLUMN_NAME + ", 5, 2")]
        [InlineData(VARCHAR, "'123.45'", 123, ArrowTypeId.Decimal128, TO_NUMERIC, COLUMN_NAME + ", 5, 0")]
        [InlineData(VARCHAR, "'123.45'", 123.45, ArrowTypeId.Double, TO_DOUBLE)]
        [InlineData(VARCHAR, "'123,456'", 123456, ArrowTypeId.Double, TO_DOUBLE, COLUMN_NAME + ", '000,000'")]
        [InlineData(VARCHAR, "'NaN'", double.NaN, ArrowTypeId.Double, TO_DOUBLE)]
        [InlineData(VARCHAR, "'inf'", double.PositiveInfinity, ArrowTypeId.Double, TO_DOUBLE)]
        [InlineData(VARCHAR, "'-inf'", double.NegativeInfinity, ArrowTypeId.Double, TO_DOUBLE)]
        [InlineData(VARCHAR, "'true'", true, ArrowTypeId.Boolean, TO_BOOLEAN)]
        [InlineData(VARCHAR, "'fALSE'", false, ArrowTypeId.Boolean, TO_BOOLEAN)]
        [InlineData(VARCHAR, "'1970-01-01 00:00:00+0000'", "1970-01-01 00:00:00+0000", ArrowTypeId.Timestamp, TO_TIMESTAMP_TZ)]
        [InlineData(VARCHAR, "'31/12/1970 00:00:00'", "1970-12-31 00:00:00+0000", ArrowTypeId.Timestamp, TO_TIMESTAMP_TZ, COLUMN_NAME + ", 'dd/mm/yyyy hh24:mi:ss'")]
        // From TIMESTAMP/DATE/TIME
        [InlineData(TIMESTAMP_TZ, "'1970-01-01 00:00:00+0000'", "1970-01-01 00:00:00+0000", ArrowTypeId.Timestamp, TO_TIMESTAMP_TZ)]
        [InlineData(TIMESTAMP_TZ, "'1970-01-01 12:34:56+0000'", "1970-01-01 00:00:00+0000", ArrowTypeId.Date32, TO_DATE)] // Date portion, only
        [InlineData(TIMESTAMP_TZ, "'2970-01-01 12:00:00+0000'", "2970-01-01 12:00:00.000 Z", ArrowTypeId.String, TO_VARCHAR)]
        [InlineData(TIMESTAMP_TZ, "'2970-01-01 12:00:00+0000'", "Jan 01, 2970", ArrowTypeId.String, TO_VARCHAR, COLUMN_NAME + ", 'mon dd, yyyy'")]
#if NET6_0_OR_GREATER
        [InlineData(TIMESTAMP_TZ, "'2970-01-01 12:00:00+0000'", "1970-01-01 12:00:00+0000", ArrowTypeId.Time64, TO_TIME)] // Time portion, only
#else
        [InlineData(TIMESTAMP_TZ, "'2970-01-01 12:00:00+0000'", 43200000000, ArrowTypeId.Time64, TO_TIME)] // Microseconds
#endif
        public void TestCastPositive(
            string columnSpecification,
            string sourceValue,
            object expectedValue,
            ArrowTypeId expectedType,
            string castFunction,
            string? castExpression = null)
        {
            InitializeTest(columnSpecification, sourceValue, out string columnName, out string table);
            SelectWithCastAndValidateValue(
                table,
                castFunction,
                castExpression ?? columnName,
                expectedValue,
                expectedType);
        }

        [SkippableTheory]
        [InlineData(NUMERIC, "2345", typeof(AdbcException), TO_VARCHAR, COLUMN_NAME + ", 123", new[] { "42601", "SQL compilation error" })] // Invalid format type.
        [InlineData(NUMERIC, "2345", typeof(AdbcException), TO_VARCHAR, COLUMN_NAME + ", '123'", new[] { "22007", "Bad output format" })] // Invalid format type.
        [InlineData(VARCHAR, "'ABC'", typeof(AdbcException), TO_NUMERIC, null, new[] { "22018", "Numeric value" })] // Non numeric value
        [InlineData(VARCHAR, "'3'", typeof(AdbcException), TO_BOOLEAN, null, new[] { "22018", "Boolean value" })] // Non boolean value
        [InlineData(VARCHAR, "'31/12/1970'", typeof(AdbcException), TO_TIMESTAMP_TZ, null, new[] { "22007", "Timestamp" })] // Non date value
        public void TestCastNegative(
            string columnSpecification,
            string sourceValue,
            Type expectedExceptionType,
            string castFunction,
            string? castExpression = null,
            string[]? expectedExceptionTextContains = null)
        {
            InitializeTest(columnSpecification, sourceValue, out string columnName, out string table);
            SelectWithCastAndValidateException(
                table,
                castFunction,
                castExpression ?? columnName,
                expectedExceptionType,
                expectedExceptionTextContains);
        }

        [SkippableTheory]
        [InlineData(ARRAY, "SELECT ARRAY_CONSTRUCT('TRUE', 'FALSE')", "[\n  \"TRUE\",\n  \"FALSE\"\n]", ArrowTypeId.String, TO_ARRAY)]
        [InlineData(ARRAY, "SELECT ARRAY_CONSTRUCT('TRUE', 'FALSE')", "[\n  \"TRUE\",\n  \"FALSE\"\n]", ArrowTypeId.String, TO_VARIANT)]
        [InlineData(BOOLEAN, "SELECT 'TRUE'", "true", ArrowTypeId.String, TO_VARIANT)]
        [InlineData(NUMERIC, "SELECT 42", "42", ArrowTypeId.String, TO_VARIANT)]
        [InlineData(OBJECT, "SELECT OBJECT_CONSTRUCT('fortyTwo', 42::VARIANT)", "{\n  \"fortyTwo\": 42\n}", ArrowTypeId.String, TO_OBJECT)]
        [InlineData(OBJECT, "SELECT OBJECT_CONSTRUCT('fortyTwo', 42::VARIANT)", "{\n  \"fortyTwo\": 42\n}", ArrowTypeId.String, TO_VARIANT)]
        [InlineData(OBJECT, "SELECT OBJECT_CONSTRUCT('fortyTwo', 42::NUMERIC)", "{\n  \"fortyTwo\": 42\n}", ArrowTypeId.String, TO_VARIANT)]
        [InlineData(VARCHAR, "SELECT 42", "\"42\"", ArrowTypeId.String, TO_VARIANT)]
        [InlineData(VARIANT, "SELECT 42::VARIANT", "42", ArrowTypeId.String, TO_VARIANT)]
        [InlineData(VARIANT, "SELECT 'JONES'::VARIANT", "\"JONES\"", ArrowTypeId.String, TO_VARIANT)]
        public void TestCastPositiveStructured(
            string columnSpecification,
            string sourceValue,
            object expectedValue,
            ArrowTypeId expectedType,
            string castFunction,
            string? castExpression = null)
        {
            InitializeTest(columnSpecification, sourceValue, out string columnName, out string table, useSelectSyntax: true);
            SelectWithCastAndValidateValue(
                table,
                castFunction,
                castExpression ?? columnName,
                expectedValue,
                expectedType);
        }

        [SkippableTheory]
        [InlineData(VARCHAR, "'ABC'", ArrowTypeId.Decimal128, TRY_TO_NUMERIC, null)] // Non numeric value
        [InlineData(VARCHAR, "'3'", ArrowTypeId.Boolean, TRY_TO_BOOLEAN, null)] // Non boolean value
        [InlineData(VARCHAR, "'31/12/1970'", ArrowTypeId.Timestamp, TRY_TO_TIMESTAMP_TZ, null)] // Non date value
        public void TestTryCastNegative(
            string columnSpecification,
            string sourceValue,
            ArrowTypeId expectedType,
            string castFunction,
            string? castExpression = null)
        {
            {
                InitializeTest(columnSpecification, sourceValue, out string columnName, out string table);
                SelectWithCastAndValidateValue(
                    table,
                    castFunction,
                    castExpression ?? columnName,
                    null, // Returns null if value cannot be converted.
                    expectedType);
            }
        }

        private void InitializeTest(
            string columnSpecification,
            string sourceValue,
            out string columnName,
            out string table,
            bool useSelectSyntax = false)
        {
            columnName = COLUMN_NAME;
            table = CreateTemporaryTable(
                _statement,
                s_testTablePrefix,
                _catalogSchema,
                string.Format("{0} {1}", columnName, columnSpecification));
            if (useSelectSyntax)
            {
                InsertIntoFromSelect(table, columnName, sourceValue);
            }
            else
            {
                InsertSingleValue(table, columnName, sourceValue);
            }
        }

        private void InsertSingleValue(string table, string columnName, string value)
        {
            string insertStatement = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", table, columnName, value);
            _output.WriteLine(insertStatement);
            _statement.SqlQuery = insertStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(1, updateResult.AffectedRows);
        }

        private void InsertIntoFromSelect(string table, string columnName, string selectQuery, long expectedAffectedRows = 1)
        {
            string insertStatement = string.Format("INSERT INTO {0} ({1}) {2};", table, columnName, selectQuery);
            _output.WriteLine(insertStatement);
            _statement.SqlQuery = insertStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(expectedAffectedRows, updateResult.AffectedRows);
        }

        private async void SelectWithCastAndValidateValue(
            string table,
            string castFunction,
            string castExpression,
            object? value,
            ArrowTypeId expectedType)
        {
            QueryResult queryResult = PerformQuery(table, castFunction, castExpression);
            await ValidateCast(value, expectedType, queryResult);
        }

        private void SelectWithCastAndValidateException(
            string table,
            string castFunction,
            string castExpression,
            Type expectedExceptionType,
            string[]? expectedExceptionTextContains = null)
        {
            Exception actualException = Assert.Throws(expectedExceptionType, () => PerformQuery(table, castFunction, castExpression));
            SnowflakeTestingUtils.AssertContainsAll(expectedExceptionTextContains, actualException.Message);
        }

        private QueryResult PerformQuery(string table, string castFunction, string castExpression)
        {
            string selectStatement = string.Format(
                "SELECT {0}({1}) AS CASTRESULT FROM {2};",
                castFunction,
                castExpression,
                table);
            _output.WriteLine(selectStatement);
            _statement.SqlQuery = selectStatement;
            return _statement.ExecuteQuery();
        }

        private static async Task ValidateCast(object? value, ArrowTypeId expectedType, QueryResult queryResult)
        {
            Assert.Equal(1, queryResult.RowCount);
            using IArrowArrayStream stream = queryResult.Stream ?? throw new InvalidOperationException("empty result");
            Field field = stream.Schema.GetFieldByName("CASTRESULT");
            Assert.NotNull(field);
            while (true)
            {
                using RecordBatch nextBatch = await stream.ReadNextRecordBatchAsync();
                if (nextBatch == null) { break; }
                Assert.Equal(expectedType, field.DataType.TypeId);
                IArrowArray valueArray = nextBatch.Column(0);

                switch (field.DataType.TypeId)
                {
                    case ArrowTypeId.Double:
                        var doubleArray = (DoubleArray)valueArray;
                        for (int i = 0; i < doubleArray.Length; i++)
                        {
                            Assert.Equal(Convert.ToDouble(value), doubleArray.GetValue(i));
                        }
                        break;
                    case ArrowTypeId.Int32:
                        var int32Array = (Int32Array)valueArray;
                        for (int i = 0; i < int32Array.Length; i++)
                        {
                            Assert.Equal(value, int32Array.GetValue(i));
                        }
                        break;
                    case ArrowTypeId.Int64:
                        var int64Array = (Int64Array)valueArray;
                        for (int i = 0; i < int64Array.Length; i++)
                        {
                            Assert.Equal(value, int64Array.GetValue(i));
                        }
                        break;
                    case ArrowTypeId.String:
                        var stringArray = (StringArray)valueArray;
                        for (int i = 0; i < stringArray.Length; i++)
                        {
                            Assert.Equal(value, stringArray.GetString(i));
                        }
                        break;
                    case ArrowTypeId.Boolean:
                        var booleanArray = (BooleanArray)valueArray;
                        for (int i = 0; i < booleanArray.Length; i++)
                        {
                            Assert.Equal(value, booleanArray.GetValue(i));
                        }
                        break;
                    case ArrowTypeId.Date64:
                        var date64Array = (Date64Array)valueArray;
                        for (int i = 0; i < date64Array.Length; i++)
                        {
                            Assert.Equal(value, date64Array.GetValue(i));
                        }
                        break;
                    case ArrowTypeId.Decimal128:
                        var decimal128Array = (Decimal128Array)valueArray;
                        for (int i = 0; i < decimal128Array.Length; i++)
                        {

                            Assert.Equal(value == null ? null : Convert.ToDecimal(value), decimal128Array.GetValue(i));
                        }
                        break;
                    case ArrowTypeId.Decimal256:
                        var decimal256Array = (Decimal256Array)valueArray;
                        for (int i = 0; i < decimal256Array.Length; i++)
                        {
                            Assert.Equal(value, decimal256Array.GetValue(i));
                        }
                        break;
                    case ArrowTypeId.Timestamp:
                        var timestampArray = (TimestampArray)valueArray;
                        for (int i = 0; i < timestampArray.Length; i++)
                        {
                            Assert.Equal(value == null ? null : new DateTimeOffset(Convert.ToDateTime(value).ToUniversalTime()), timestampArray.GetTimestamp(i));
                        }
                        break;
                    case ArrowTypeId.Time64:
                        var time64Array = (Time64Array)valueArray;
                        for (int i = 0; i < time64Array.Length; i++)
                        {
#if NET6_0_OR_GREATER
                            Assert.Equal(TimeOnly.FromDateTime(Convert.ToDateTime(value).ToUniversalTime()), time64Array.GetTime(i));
#else
                            Assert.Equal(Convert.ToInt64(value), time64Array.GetMicroSeconds(i));
#endif
                        }
                        break;
                    case ArrowTypeId.Date32:
                        var date32Array = (Date32Array)valueArray;
                        for (int i = 0; i < date32Array.Length; i++)
                        {
                            Assert.Equal(Convert.ToDateTime(value), date32Array.GetDateTimeOffset(i));
                        }
                        break;
                    default:
                        throw new ArgumentException(string.Format("Unexpected ArrowTypeId: {0}({1})", field.DataType.TypeId.ToString(), field.DataType.TypeId));

                }
            }
        }

        private static string CreateTemporaryTable(AdbcStatement statement, string testTablePrefix, string catalogSchema, string columns)
        {
            string tableName = string.Format("{0}.{1}{2}", catalogSchema, testTablePrefix, Guid.NewGuid().ToString().Replace("-", ""));
            string createTableStatement = string.Format("CREATE TEMPORARY TABLE {0} ({1})", tableName, columns);
            statement.SqlQuery = createTableStatement;
            statement.ExecuteUpdate();
            return tableName;
        }

        private static void SetSessionTimezone(AdbcStatement statement, string timezone)
        {
            statement.SqlQuery = string.Format("ALTER SESSION SET TIMEZONE = '{0}'", timezone);
            UpdateResult result = statement.ExecuteUpdate();
            Assert.Equal(1, result.AffectedRows);
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
                _connection?.Dispose();
                _statement?.Dispose();
                _disposed = true;
            }
        }
    }
}

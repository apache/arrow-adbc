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
using System.Data.SqlTypes;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    public class NumericOperatorTests : IDisposable
    {
        private const string TestTablePrefix = "ADBCOPERATORTEST_"; // Make configurable? Also; must be all caps if not double quoted
        private const string ColumnNameLeft = "LEFT_OPERAND";
        private const string ColumnNameRight = "RIGHT_OPERAND";
        readonly SnowflakeTestConfiguration _snowflakeTestConfiguration;
        readonly AdbcConnection _connection;
        readonly AdbcStatement _statement;
        readonly string _catalogSchema;
        private readonly ITestOutputHelper _output;

        private readonly record struct ColumnPair
        {
            public ColumnPair(string columnDefLeft, string valueLeft, string columnDefRight, string valueRight)
            {
                ColumnDefLeft = columnDefLeft;
                ValueLeft = valueLeft;
                ColumnDefRight = columnDefRight;
                ValueRight = valueRight;
            }

            public static string ColumnNameLeft { get; } = NumericOperatorTests.ColumnNameLeft;
            public static string ColumnNameRight { get; } = NumericOperatorTests.ColumnNameRight;
            public string ColumnDefLeft { get; }
            public string ValueLeft { get; }
            public string ColumnDefRight { get; }
            public string ValueRight { get; }
        }

        /// <summary>
        /// Validates that specific numeric values can be inserted, retrieved and targeted correctly
        /// </summary>
        public NumericOperatorTests(ITestOutputHelper output)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
            _snowflakeTestConfiguration = SnowflakeTestingUtils.TestConfiguration;
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            Dictionary<string, string> options = new Dictionary<string, string>();
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_snowflakeTestConfiguration, out parameters);
            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            _connection = adbcDatabase.Connect(options);
            _statement = _connection.CreateStatement();
            _catalogSchema = string.Format("{0}.{1}", _snowflakeTestConfiguration.Metadata?.Catalog, _snowflakeTestConfiguration.Metadata?.Schema);
            _output = output;
        }

        /// <summary>
        /// Validates if driver can handle various operators and their values correctly
        /// </summary>
        [SkippableTheory]
        [InlineData("NUMERIC", 0, "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", -1, "-1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", 1, "1", ArrowTypeId.Decimal128)]
        public void TestPositiveOperatorDecimalResult(
            string columnDef,
            object testValue,
            string expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(columnDef, testValue.ToString()!, out string columnName, out string table);
            SelectUnaryOperatorAndValidateValues(table, columnName, "+", SqlDecimal.Parse(expectedValue), expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("DOUBLE", 0.0d, 0.0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", -1.0d, -1.0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", 1.0d, 1.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "0", 0, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "-1", -1, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "1", 1, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "0.0", 0.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "-1.0", -1.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "1.0", 1.0d, ArrowTypeId.Double)]
        public void TestPositiveOperatorDoubleResult(
            string columnDef,
            object testValue,
            double expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(columnDef, testValue.ToString()!, out string columnName, out string table);
            SelectUnaryOperatorAndValidateValues(table, columnName, "+", expectedValue, expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("NUMERIC", "0", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-1", "-1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "99999999999999999999999999999999999999", "99999999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-99999999999999999999999999999999999999", "-99999999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        public void TestPositiveOperatorNumericResult(
            string columnDef,
            string testValue,
            string expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(columnDef, testValue.ToString(), out string columnName, out string table);
            SelectUnaryOperatorAndValidateValues(table, columnName, "+", SqlDecimal.Parse(expectedValue), expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("DOUBLE", 0.0d, 0.0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", -1.0d, 1.0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", 1.0d, -1.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "0", 0.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "-1", 1.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "1", -1.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "0.0", 0.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "-1.0", 1.0d, ArrowTypeId.Double)]
        [InlineData("VARCHAR", "1.0", -1.0d, ArrowTypeId.Double)]
        public void TestNegativeOperatorDoubleResult(
            string columnDef,
            object testValue,
            double expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(columnDef, testValue.ToString()!, out string columnName, out string table);
            SelectUnaryOperatorAndValidateValues(table, columnName, "-", expectedValue, expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("DOUBLE", "1.0", "+", "NUMERIC", "0.0", 1.0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "+", "NUMERIC", "1", 2d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-1.7976899999999999E+308", "+", "DOUBLE", "-0.0000000000000001E+308", -1.79769E+308, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1.7976899999999999E+308", "+", "DOUBLE", "0.0000000000000001E+308", 1.79769E+308, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-2.2249999999999997E-307", "+", "DOUBLE", "-0.0000000000000004E-307", -2.225E-307, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "2.2249999999999997E-307", "+", "DOUBLE", "0.0000000000000004E-307", 2.225E-307, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1.0", "+", "VARCHAR", "0.0", 1.0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "+", "VARCHAR", "1", 2d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "99999999999999999999999999999999999999", "+", "VARCHAR", "1", 99999999999999999999999999999999999998d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-99999999999999999999999999999999999999", "+", "VARCHAR", "1", -99999999999999999999999999999999999998d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "-", "NUMERIC", "0", 1d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "-", "NUMERIC", "1", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "99999999999999999999999999999999999999", "-", "NUMERIC", "1", 99999999999999999999999999999999999998d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-99999999999999999999999999999999999998", "-", "NUMERIC", "1", -99999999999999999999999999999999999999d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "-", "VARCHAR", "0", 1d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "-", "VARCHAR", "1", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "99999999999999999999999999999999999999", "-", "VARCHAR", "1", 99999999999999999999999999999999999998d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-99999999999999999999999999999999999998", "-", "VARCHAR", "1", -99999999999999999999999999999999999999d, ArrowTypeId.Double)]
        public void TestAdditionSubtractionOperatorDoubleResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            double expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, expectedValue, expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("NUMERIC", "1", "+", "NUMERIC", "0", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "+", "NUMERIC", "1", "2", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "99999999999999999999999999999999999998", "+", "NUMERIC", "1", "99999999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-99999999999999999999999999999999999999", "+", "NUMERIC", "1", "-99999999999999999999999999999999999998", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "-", "NUMERIC", "0", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "-", "NUMERIC", "1", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "99999999999999999999999999999999999999", "-", "NUMERIC", "1", "99999999999999999999999999999999999998", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-99999999999999999999999999999999999998", "-", "NUMERIC", "1", "-99999999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "+", "VARCHAR", "0", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "+", "VARCHAR", "1", "2", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "999999999999999999999999999999998", "+", "VARCHAR", "1", "999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-999999999999999999999999999999999", "+", "VARCHAR", "1", "-999999999999999999999999999999998", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "-", "VARCHAR", "0", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "-", "VARCHAR", "1", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "999999999999999999999999999999999", "-", "VARCHAR", "1", "999999999999999999999999999999998", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-999999999999999999999999999999998", "-", "VARCHAR", "1", "-999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        public void TestAdditionSubtractionOperatorNumericResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            string expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, SqlDecimal.Parse(expectedValue), expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("DOUBLE", "1.0", "*", "NUMERIC", "0.0", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "*", "NUMERIC", "1", 1d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-1.7976899999999999E+308", "*", "DOUBLE", "1", -1.7976899999999999E+308, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1.7976899999999999E+308", "*", "DOUBLE", "1", 1.7976899999999999E+308, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-2.2249999999999997E-307", "*", "DOUBLE", "1", -2.2249999999999997E-307, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "2.2249999999999997E-307", "*", "DOUBLE", "1", 2.2249999999999997E-307, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1.0", "*", "VARCHAR", "0.0", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "*", "VARCHAR", "1", 1d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "99999999999999999999999999999999999999", "*", "VARCHAR", "1", 99999999999999999999999999999999999999d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-99999999999999999999999999999999999999", "*", "VARCHAR", "1", -99999999999999999999999999999999999999d, ArrowTypeId.Double)]
        public void TestMultiplicationOperatorDoubleResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            double expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, expectedValue, expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("NUMERIC", "1.0", "*", "NUMERIC", "0.0", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "*", "NUMERIC", "1", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1.0", "*", "VARCHAR", "0.0", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "*", "VARCHAR", "1", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "99999999999999999999999999999999999999", "*", "NUMERIC", "1", "99999999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-99999999999999999999999999999999999999", "*", "NUMERIC", "1", "-99999999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        // Note: Itermediate type conversion to NUMERIC(38,5) requires a smaller number when interacting with VARCHAR
        [InlineData("NUMERIC", "999999999999999999999999999999999", "*", "VARCHAR", "'1'", "999999999999999999999999999999999", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-99999999999999999999999999999999", "*", "VARCHAR", "1", "-99999999999999999999999999999999", ArrowTypeId.Decimal128)]
        public void TestMultiplicationOperatorNumericResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            string expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, SqlDecimal.Parse(expectedValue), expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("DOUBLE", "0.0", "/", "NUMERIC", "1.0", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "/", "NUMERIC", "1", 1d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-1.7976899999999999E+308", "/", "DOUBLE", "1", -1.7976899999999999E+308, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1.7976899999999999E+308", "/", "DOUBLE", "1", 1.7976899999999999E+308, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-2.2249999999999997E-307", "/", "DOUBLE", "1", -2.2249999999999997E-307, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "2.2249999999999997E-307", "/", "DOUBLE", "1", 2.2249999999999997E-307, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "0.0", "/", "VARCHAR", "1.0", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "/", "VARCHAR", "1", 1d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "99999999999999999999999999999999999999", "/", "VARCHAR", "1", 99999999999999999999999999999999999999d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-99999999999999999999999999999999999999", "/", "VARCHAR", "1", -99999999999999999999999999999999999999d, ArrowTypeId.Double)]
        public void TestDivisionOperatorDoubleResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            double expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, expectedValue, expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("NUMERIC", "0", "/", "NUMERIC", "1", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "/", "NUMERIC", "1", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "0", "/", "VARCHAR", "1", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "/", "VARCHAR", "1", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "99999999999999999999999999999999", "/", "VARCHAR", "1", "99999999999999999999999999999999", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-99999999999999999999999999999999", "/", "VARCHAR", "1", "-99999999999999999999999999999999", ArrowTypeId.Decimal128)]
        public void TestDivisionOperatorNumericResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            string expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, SqlDecimal.Parse(expectedValue), expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("DOUBLE", "0.0", "%", "NUMERIC", "1", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1.0", "%", "NUMERIC", "1", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "5.0", "%", "NUMERIC", "2", 1, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "5.1", "%", "NUMERIC", "2", 1.0999999999999996, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-5.0", "%", "NUMERIC", "2", -1, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "-5.1", "%", "NUMERIC", "2", -1.0999999999999996, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "0.0", "%", "VARCHAR", "'1.0'", 0d, ArrowTypeId.Double)]
        [InlineData("DOUBLE", "1", "%", "VARCHAR", "'1'", 0d, ArrowTypeId.Double)]
        public void TestModulusOperatorDoubleResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            double expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, expectedValue, expectedTypeId);
        }

        [SkippableTheory]
        [InlineData("NUMERIC", "0", "%", "NUMERIC", "1", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "%", "NUMERIC", "1", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC(38,1)", "5.0", "%", "NUMERIC", "2", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC(38,1)", "5.2", "%", "NUMERIC", "2", "1.2", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC(38,1)", "-5.0", "%", "NUMERIC", "2", "-1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC(38,1)", "-5.2", "%", "NUMERIC", "2", "-1.2", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "0.0", "%", "VARCHAR", "1.0", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "1", "%", "VARCHAR", "1", "0", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "999999999999999999999999999999999", "%", "VARCHAR", "2", "1", ArrowTypeId.Decimal128)]
        [InlineData("NUMERIC", "-999999999999999999999999999999999", "%", "VARCHAR", "2", "-1", ArrowTypeId.Decimal128)]
        public void TestModulusOperatorDecimalResult(
            string columnDefLeft,
            string testValueLeft,
            string operatorName,
            string columnDefRight,
            string testValueRight,
            string expectedValue,
            ArrowTypeId expectedTypeId)
        {
            InitializeTest(new ColumnPair(columnDefLeft, testValueLeft, columnDefRight, testValueRight), out string table);
            SelectBinaryOperatorAndValidateValues(table, operatorName, SqlDecimal.Parse(expectedValue), expectedTypeId);
        }

        private void InsertSingleValue(string table, string columnName, string value)
        {
            string insertNumberStatement = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", table, columnName, value);
            Console.WriteLine(insertNumberStatement);
            _statement.SqlQuery = insertNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(1, updateResult.AffectedRows);
        }

        private void InsertValues(string table, ColumnPair initialData)
        {
            string insertNumberStatement = $"INSERT INTO {table} "
                + $"({ColumnPair.ColumnNameLeft}, {ColumnPair.ColumnNameRight}) "
                + $"VALUES ({initialData.ValueLeft}, {initialData.ValueRight});";
            Console.WriteLine(insertNumberStatement);
            _statement.SqlQuery = insertNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(1, updateResult.AffectedRows);
        }

        private async void SelectUnaryOperatorAndValidateValues(
            string table,
            string columnName,
            string operatorText,
            object expectedValue,
            ArrowTypeId expectedTypeId,
            int expectedCount = 1)
        {
            const string operatorColumnName = "TEST_COLUMN";
            string selectStatement = $"SELECT {columnName}, {operatorText}({columnName}) AS {operatorColumnName} FROM {table}";

            _output.WriteLine(selectStatement);
            _statement.SqlQuery = selectStatement;
            QueryResult queryResult = _statement.ExecuteQuery();
            Assert.Equal(expectedCount, queryResult.RowCount);

            await ValidateValues(operatorColumnName, expectedValue, expectedTypeId, queryResult);
        }

        private async void SelectBinaryOperatorAndValidateValues(
            string table,
            string operatorText,
            object expectedValue,
            ArrowTypeId expectedTypeId,
            int expectedCount = 1)
        {
            const string operatorColumnName = "TEST_COLUMN";
            string selectStatement = $"SELECT {ColumnPair.ColumnNameLeft} {operatorText} {ColumnPair.ColumnNameRight} AS {operatorColumnName} FROM {table}";

            _output.WriteLine(selectStatement);
            _statement.SqlQuery = selectStatement;
            QueryResult queryResult = _statement.ExecuteQuery();
            Assert.Equal(expectedCount, queryResult.RowCount);

            await ValidateValues(operatorColumnName, expectedValue, expectedTypeId, queryResult);
        }

        private static async Task ValidateValues(
            string columnName,
            object value,
            ArrowTypeId expectedTypeId,
            QueryResult queryResult)
        {
            using (IArrowArrayStream stream = queryResult.Stream ?? throw new InvalidOperationException("empty result"))
            {
                Field field = stream.Schema.GetFieldByName(columnName);
                while (true)
                {
                    using (RecordBatch nextBatch = await stream.ReadNextRecordBatchAsync())
                    {
                        if (nextBatch == null) { break; }
                        Assert.Equal(expectedTypeId, field.DataType.TypeId);
                        switch (field.DataType)
                        {
                            case Decimal128Type:
                                Decimal128Array decimalArray = (Decimal128Array)nextBatch.Column(columnName);
                                for (int i = 0; i < decimalArray.Length; i++)
                                {
                                    Assert.Equal(value, decimalArray.GetSqlDecimal(i));
                                }
                                break;
                            case DoubleType:
                                DoubleArray doubleArray = (DoubleArray)nextBatch.Column(columnName);
                                for (int i = 0; i < doubleArray.Length; i++)
                                {
                                    Assert.Equal(value, doubleArray.GetValue(i));
                                }
                                break;
                            case BooleanType:
                                BooleanArray booleanArray = (BooleanArray)nextBatch.Column(columnName);
                                for (int i = 0; i < booleanArray.Length; i++)
                                {
                                    Assert.Equal(value, booleanArray.GetValue(i));
                                }
                                break;
                            default:
                                Assert.Fail("Unsupported datatype is not handled in test.");
                                break;
                        }
                    }
                }
            }
        }

        private void InitializeTest(
            ColumnPair initialData,
            out string table,
            bool useSelectSyntax = false)
        {
            string columnsDef = $"{ColumnPair.ColumnNameLeft} {initialData.ColumnDefLeft}, {ColumnPair.ColumnNameRight} {initialData.ColumnDefRight}";
            table = CreateTemporaryTable(
                _statement,
                TestTablePrefix,
                _catalogSchema,
                columnsDef);
            InsertValues(table, initialData);
        }

        private void InitializeTest(
            string columnSpecification,
            string sourceValue,
            out string columnName,
            out string table,
            bool useSelectSyntax = false)
        {
            columnName = ColumnNameLeft;
            table = CreateTemporaryTable(
                _statement,
                TestTablePrefix,
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
            _connection.Dispose();
            _statement.Dispose();
        }
    }
}

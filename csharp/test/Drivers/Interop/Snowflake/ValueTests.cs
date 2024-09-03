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
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    public class ValueTests : IDisposable
    {
        static readonly string s_testTablePrefix = "ADBCVALUETEST_"; // Make configurable? Also; must be all caps if not double quoted
        readonly SnowflakeTestConfiguration _snowflakeTestConfiguration;
        readonly AdbcConnection _connection;
        readonly AdbcStatement _statement;
        readonly string _catalogSchema;

        /// <summary>
        /// Validates that specific numeric values can be inserted, retrieved and targeted correctly
        /// </summary>
        public ValueTests()
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
        }

        /// <summary>
        /// Validates if driver can send and receive specific Integer values correctly
        /// These Snowflake types are equivalent: NUMBER(38,0),INT,INTEGER,BIGINT,SMALLINT,TINYINT,BYTEINT
        /// </summary>
        [SkippableTheory]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(1)]
        public void TestIntegerSanity(int value)
        {
            string columnName = "INTTYPE";
            string table = CreateTemporaryTable(string.Format("{0} INT", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, value);
        }

        /// <summary>
        /// Validates if driver can handle the largest / smallest numbers
        /// </summary>
        [SkippableTheory]
        [InlineData("99999999999999999999999999999999999999")]
        [InlineData("-99999999999999999999999999999999999999")]
        public void TestIntegerMinMax(string value)
        {
            string columnName = "INTTYPE";
            string table = CreateTemporaryTable(string.Format("{0} INT", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse(value));
        }

        /// <summary>
        /// Validates if driver can handle smaller Number type correctly
        /// </summary>
        [SkippableTheory]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(99)]
        [InlineData(-99)]
        public void TestSmallNumberRange(int value)
        {
            string columnName = "SMALLNUMBER";
            string table = CreateTemporaryTable(string.Format("{0} NUMBER(2,0)", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, value);
        }

        /// <summary>
        /// Validates if driver correctly errors out when the values exceed the column's limit
        /// </summary>
        [SkippableTheory]
        [InlineData(-100)]
        [InlineData(100)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        public void TestSmallNumberRangeOverlimit(int value)
        {
            string columnName = "SMALLNUMBER";
            string table = CreateTemporaryTable(string.Format("{0} NUMBER(2,0)", columnName));
            Assert.ThrowsAny<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, value));
        }

        /// <summary>
        /// Validates if driver can handle a large scale Number type correctly
        /// </summary>
        [SkippableTheory]
        [InlineData("0")]
        [InlineData("-2.003")]
        [InlineData("4.85")]
        [InlineData("0.0000000000000000000000000000000000001")]
        [InlineData("9.5545204502636499875576383003668916798")]
        public void TestLargeScaleNumberRange(string value)
        {
            string columnName = "LARGESCALENUMBER";
            string table = CreateTemporaryTable(string.Format("{0} NUMBER(38,37)", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse(value));
        }

        /// <summary>
        /// Validates if driver can error handle when input goes beyond a large scale Number type
        /// </summary>
        [SkippableTheory]
        [InlineData("-10")]
        [InlineData("10")]
        [InlineData("99999999999999999999999999999999999999")]
        [InlineData("-99999999999999999999999999999999999999")]
        public void TestLargeScaleNumberOverlimit(string value)
        {
            string columnName = "LARGESCALENUMBER";
            string table = CreateTemporaryTable(string.Format("{0} NUMBER(38,37)", columnName));
            Assert.ThrowsAny<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse(value)));
        }

        /// <summary>
        /// Validates if driver can handle a small scale Number type correctly
        /// </summary>
        [SkippableTheory]
        [InlineData("0")]
        [InlineData("4.85")]
        [InlineData("-999999999999999999999999999999999999.99")]
        [InlineData("999999999999999999999999999999999999.99")]
        public void TestSmallScaleNumberRange(string value)
        {
            string columnName = "SMALLSCALENUMBER";
            string table = CreateTemporaryTable(string.Format("{0} NUMBER(38,2)", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse(value));
        }

        /// <summary>
        /// Validates if driver can error handle when an insert goes beyond a small scale Number type correctly
        /// </summary>
        [SkippableTheory]
        [InlineData("-99999999999999999999999999999999999999")]
        [InlineData("99999999999999999999999999999999999999")]
        public void TestSmallScaleNumberOverlimit(string value)
        {
            string columnName = "SMALLSCALENUMBER";
            string table = CreateTemporaryTable(string.Format("{0} NUMBER(38,2)", columnName));
            Assert.ThrowsAny<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse(value)));
        }


        /// <summary>
        /// Tests that decimals are rounded as expected.
        /// Snowflake allows inserts of scales beyond the data type size, but storage of value will round it up or down
        /// </summary>
        [SkippableTheory]
        [InlineData(2.467, 2.47)]
        [InlineData(-672.613, -672.61)]
        public void TestRoundingNumbers(decimal input, decimal output)
        {
            string columnName = "SMALLSCALENUMBER";
            string table = CreateTemporaryTable(string.Format("{0} NUMBER(38,2)", columnName));
            SqlDecimal value = new SqlDecimal(input);
            SqlDecimal returned = new SqlDecimal(output);
            InsertSingleValue(table, columnName, value.ToString());
            SelectAndValidateValues(table, columnName, returned, 1);
            DeleteFromTable(table, string.Format("{0}={1}", columnName, returned), 1);
        }

        /// <summary>
        /// Validates if driver can handle floating point number type correctly
        /// These Snowflake types are equivalent: FLOAT,FLOAT4,FLOAT8,DOUBLE,DOUBLE PRECISION,REAL
        /// </summary>
        [SkippableTheory]
        [InlineData(0)]
        [InlineData(0.2)]
        [InlineData(15e-03)]
        [InlineData(1.234E+2)]
        [InlineData(double.NegativeInfinity)]
        [InlineData(double.PositiveInfinity)]
        [InlineData(double.NaN)]
        [InlineData(double.MinValue)]
        [InlineData(double.MaxValue)]
        public void TestFloatValuesInsertSelectDelete(double value)
        {
            string columnName = "FLOATTYPE";
            string table = CreateTemporaryTable(string.Format("{0} FLOAT", columnName));
            string valueString = ConvertDoubleToString(value);
            InsertSingleValue(table, columnName, valueString);
            SelectAndValidateValues(table, columnName, value, 1);
            DeleteFromTable(table, string.Format("{0}={1}", columnName, valueString), 1);
        }

        private void ValidateInsertSelectDeleteSingleDecimalValue(string table, string columnName, SqlDecimal value)
        {
            InsertSingleValue(table, columnName, value.ToString());
            SelectAndValidateValues(table, columnName, value, 1);
            DeleteFromTable(table, string.Format("{0}={1}", columnName, value), 1);
        }

        private void InsertSingleValue(string table, string columnName, string value)
        {
            string insertNumberStatement = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", table, columnName, value);
            Console.WriteLine(insertNumberStatement);
            _statement.SqlQuery = insertNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(1, updateResult.AffectedRows);
        }

        private static string ConvertDoubleToString(double value)
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
                    return value.ToString();
            }
        }

        private async void SelectAndValidateValues(string table, string columnName, object value, int count)
        {
            string selectNumberStatement;
            if (value.GetType() == typeof(double))
            {
                selectNumberStatement = string.Format("SELECT {0} FROM {1} WHERE {0}={2};", columnName, table, ConvertDoubleToString((double)value));
            }
            else
            {
                selectNumberStatement = string.Format("SELECT {0} FROM {1} WHERE {0}={2};", columnName, table, value);
            }
            Console.WriteLine(selectNumberStatement);
            _statement.SqlQuery = selectNumberStatement;
            QueryResult queryResult = _statement.ExecuteQuery();
            Assert.Equal(count, queryResult.RowCount);
            using (IArrowArrayStream stream = queryResult.Stream ?? throw new InvalidOperationException("empty result"))
            {
                Field field = stream.Schema.GetFieldByName(columnName);
                while (true)
                {
                    using (RecordBatch nextBatch = await stream.ReadNextRecordBatchAsync())
                    {
                        if (nextBatch == null) { break; }
                        switch (field.DataType)
                        {
                            case Decimal128Type:
                                Decimal128Array decimalArray = (Decimal128Array)nextBatch.Column(0);
                                for (int i = 0; i < decimalArray.Length; i++)
                                {
                                    Assert.Equal(value, decimalArray.GetSqlDecimal(i));
                                }
                                break;
                            case DoubleType:
                                DoubleArray doubleArray = (DoubleArray)nextBatch.Column(0);
                                for (int i = 0; i < doubleArray.Length; i++)
                                {
                                    Assert.Equal(value, doubleArray.GetValue(i));
                                }
                                break;
                        }
                    }
                }
            }
        }

        private void DeleteFromTable(string tableName, string whereClause, int expectedRowsAffected)
        {
            string deleteNumberStatement = string.Format("DELETE FROM {0} WHERE {1};", tableName, whereClause);
            Console.WriteLine(deleteNumberStatement);
            _statement.SqlQuery = deleteNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(expectedRowsAffected, updateResult.AffectedRows);
        }

        private string CreateTemporaryTable(string columns)
        {
            string tableName = string.Format("{0}.{1}{2}", _catalogSchema, s_testTablePrefix, Guid.NewGuid().ToString().Replace("-", ""));
            string createTableStatement = string.Format("CREATE TEMPORARY TABLE {0} ({1})", tableName, columns);
            _statement.SqlQuery = createTableStatement;
            _statement.ExecuteUpdate();
            return tableName;
        }

        public void Dispose()
        {
            _connection.Dispose();
            _statement.Dispose();
        }
    }
}

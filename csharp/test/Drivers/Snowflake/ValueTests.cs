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
using Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
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
            if (Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE))
            {
                _snowflakeTestConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
                Dictionary<string, string> parameters = new();
                Dictionary<string, string> options = new();
                AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_snowflakeTestConfiguration, out parameters);
                AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
                _connection = adbcDatabase.Connect(options);
                _statement = _connection.CreateStatement();
                _catalogSchema = string.Format("{0}.{1}", _snowflakeTestConfiguration.Metadata.Catalog, _snowflakeTestConfiguration.Metadata.Schema);
            }
        }

        /// <summary>
        /// Validates if driver can send and receive specific Integer values correctly
        /// These Snowflake types are equivalent: INT,INTEGER,BIGINT,SMALLINT,TINYINT,BYTEINT
        /// </summary>
        [SkippableFact]
        public void TestIntegerRanges()
        {
            string columnName = "INTTYPE";
            string table = CreateTable(string.Format("{0} INT", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, -1);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 0);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 1);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MinValue);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MaxValue);
        }

        /// <summary>
        /// Validates if driver can handle smaller Number type correctly
        /// TODO: Currently fails, requires PR #1242 merge to pass.
        /// </summary>
        [SkippableFact]
        public void TestSmallNumberRange()
        {
            string columnName = "SMALLNUMBER";
            string table = CreateTable(string.Format("{0} NUMBER(2,0)", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, -1);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 0);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 1);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, -99);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 99);
            Assert.Throws<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MinValue));
            Assert.Throws<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MaxValue));
            Assert.Throws<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, -100));
            Assert.Throws<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 100));
        }

        /// <summary>
        /// Validates if driver can handle a large scale Number type correctly
        /// TODO: Currently fails, requires PR #1242 merge to pass.
        /// </summary>
        [SkippableFact]
        public void TestLargeScaleNumberRange()
        {
            string columnName = "LARGESCALENUMBER";
            string table = CreateTable(string.Format("{0} NUMBER(38,37)", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 0);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MinValue);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MaxValue);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse("-2.003"));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse("4.85"));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse("0.0000000000000000000000000000000000001"));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse("9.5545204502636499875576383003668916798"));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse("99999999999999999999999999999999999999.9"));
        }

        /// <summary>
        /// Validates if driver can handle a large scale Number type correctly
        /// TODO: Currently fails, requires PR #1242 merge to pass.
        /// </summary>
        [SkippableFact]
        public void TestSmallScaleNumberRange()
        {
            string columnName = "LARGESCALENUMBER";
            string table = CreateTable(string.Format("{0} NUMBER(38,2)", columnName));
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, 0);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MinValue);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.MaxValue);
            ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse("4.85"));
            Assert.Throws<AdbcException>(() => ValidateInsertSelectDeleteSingleDecimalValue(table, columnName, SqlDecimal.Parse("-2.003")));
        }

        /// <summary>
        /// Validates if driver can handle floating point number type correctly
        /// These Snowflake types are equivalent: FLOAT,FLOAT4,FLOAT8,DOUBLE,DOUBLE PRECISION,REAL
        /// </summary>
        [SkippableFact]
        public void TestFloatNumberRange()
        {
            string columnName = "FLOATTYPE";
            string table = CreateTable(string.Format("{0} FLOAT", columnName));
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, 0);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, double.NegativeInfinity);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, double.PositiveInfinity);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, double.MaxValue);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, double.MinValue);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, double.NaN);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, 0.2);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, 15e-03);
            ValidateInsertSelectDeleteSingleDoubleValue(table, columnName, 1.234E+2);
        }

        private void ValidateInsertSelectDeleteSingleDecimalValue(string table, string columnName, SqlDecimal value)
        {
            InsertSingleValue(table, columnName, value.ToString());
            SelectAndValidateDecimalValues(table, columnName, value, 1);
            DeleteFromTable(table, string.Format("{0}={1}", columnName, value), 1);
        }

        private void ValidateInsertSelectDeleteSingleDoubleValue(string table, string columnName, double value)
        {
            string valueString = ConvertDoubleToString(value);
            InsertSingleValue(table, columnName, valueString);
            SelectAndValidateDoubleValues(table, columnName, value, 1);
            DeleteFromTable(table, string.Format("{0}={1}", columnName, valueString), 1);
        }

        private void InsertSingleValue(string table, string columnName, string value)
        {
            string insertNumberStatement = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", table, columnName, value);
            Console.WriteLine(insertNumberStatement);
            _statement.SqlQuery = insertNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(1, updateResult.AffectedRows);
        }

        private string ConvertDoubleToString(double value)
        {
            switch (value)
            {
                case double.PositiveInfinity:
                    return "'inf'";
                case double.NegativeInfinity:
                    return "'-inf'";
                case double.NaN:
                    return "'NaN'";
                default:
                    return value.ToString();
            }
        }

        private void SelectAndValidateDecimalValues(string table, string columnName, SqlDecimal value, int count)
        {
            string selectNumberStatement = string.Format("SELECT {0} FROM {1} WHERE {0}={2};", columnName, table, value);
            Console.WriteLine(selectNumberStatement);
            _statement.SqlQuery = selectNumberStatement;
            QueryResult queryResult = _statement.ExecuteQuery();
            Assert.Equal(count, queryResult.RowCount);
            while (true)
            {
                RecordBatch nextBatch = queryResult.Stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                Decimal128Array decimalArray = (Decimal128Array)nextBatch.Column(0);
                for (int i = 0; i < decimalArray.Length; i++)
                {
                    Assert.Equal(value, decimalArray.GetSqlDecimal(i));
                }
            }
        }

        private void SelectAndValidateDoubleValues(string table, string columnName, double value, int count)
        {
            string selectNumberStatement = string.Format("SELECT {0} FROM {1} WHERE {0}={2};", columnName, table, ConvertDoubleToString(value));
            Console.WriteLine(selectNumberStatement);
            _statement.SqlQuery = selectNumberStatement;
            QueryResult queryResult = _statement.ExecuteQuery();
            Assert.Equal(count, queryResult.RowCount);
            while (true)
            {
                RecordBatch nextBatch = queryResult.Stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                DoubleArray doubleArray = (DoubleArray)nextBatch.Column(0);
                for (int i = 0; i < doubleArray.Length; i++)
                {
                    Assert.Equal(value, doubleArray.GetValue(i));
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

        private string CreateTable(string columns)
        {
            string tableName = string.Format("{0}.{1}{2}", _catalogSchema, s_testTablePrefix, Guid.NewGuid().ToString().Replace("-", ""));
            string createTableStatement = string.Format("CREATE TABLE {0} ({1})", tableName, columns);
            _statement.SqlQuery = createTableStatement;
            _statement.ExecuteUpdate();
            return tableName;
        }

        public void Dispose()
        {
            string getDropCommandsQuery = string.Format(
                "SELECT 'DROP TABLE {0}.' || table_name || ';' FROM {1}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '{2}_%';",
                _catalogSchema, _snowflakeTestConfiguration.Metadata.Catalog, s_testTablePrefix);
            Console.WriteLine(getDropCommandsQuery);
            _statement.SqlQuery = getDropCommandsQuery;
            try
            {
                QueryResult dropQueries = _statement.ExecuteQuery();
                while (true)
                {
                    RecordBatch nextBatch = dropQueries.Stream.ReadNextRecordBatchAsync().Result;
                    if (nextBatch == null) { break; }
                    StringArray dropCommandsArray = (StringArray)nextBatch.Column(0);
                    for (int i = 0; i < dropCommandsArray.Length; i++)
                    {
                        string dropCommandQuery = dropCommandsArray.GetString(i);
                        Console.WriteLine(dropCommandQuery);
                        _statement.SqlQuery = dropCommandQuery;
                        _statement.ExecuteUpdate();
                    }
                }
            }
            catch (ArgumentNullException ex)
            {
                if (ex.Message.Contains("'fields'"))
                {
                    Console.WriteLine(string.Format("No tables found with prefix {0}", s_testTablePrefix));
                }
                else throw ex;
            }
        }
    }
}

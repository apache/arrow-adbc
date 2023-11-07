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
using System.Numerics;
using Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    public class ValueTests : IDisposable
    {
        static readonly string s_testTablePrefix = "ADBCVALUETEST_"; // Make configurable? Also; must be all caps if not double quoted

        readonly SnowflakeTestConfiguration _snowflakeTestConfiguration;
        readonly AdbcConnection _connection;
        AdbcStatement _statement;
        readonly string _catalogSchema;

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
        /// Snowflake types INT , INTEGER , BIGINT , SMALLINT , TINYINT , BYTEINT are all equivalent
        /// </summary>
        [SkippableFact]
        public void TestIntegerRanges()
        {
            string columnName = "INTTYPE";
            string table = CreateTable(string.Format("{0} INT", columnName));
            InsertAndValidateSelectNumberValue(table, columnName, -1);
            InsertAndValidateSelectNumberValue(table, columnName, 0);
            InsertAndValidateSelectNumberValue(table, columnName, 1);
            InsertAndValidateSelectNumberValue(table, columnName, SqlDecimal.MinValue);
            InsertAndValidateSelectNumberValue(table, columnName, SqlDecimal.MaxValue);
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
            InsertAndValidateSelectNumberValue(table, columnName, -1);
            InsertAndValidateSelectNumberValue(table, columnName, 0);
            InsertAndValidateSelectNumberValue(table, columnName, 1);
            InsertAndValidateSelectNumberValue(table, columnName, -99);
            InsertAndValidateSelectNumberValue(table, columnName, 99);
            Assert.Throws<AdbcException>(() => InsertAndValidateSelectNumberValue(table, columnName, SqlDecimal.MinValue));
            Assert.Throws<AdbcException>(() => InsertAndValidateSelectNumberValue(table, columnName, SqlDecimal.MaxValue));
            Assert.Throws<AdbcException>(() => InsertAndValidateSelectNumberValue(table, columnName, -100));
            Assert.Throws<AdbcException>(() => InsertAndValidateSelectNumberValue(table, columnName, 100));
        }

        private void InsertAndValidateSelectNumberValue(string table, string columnName,SqlDecimal value)
        {
            string insertNumberStatement = string.Format("INSERT INTO {0} ({1}) VALUES ({2});", table, columnName, value);
            Console.WriteLine(insertNumberStatement);
            _statement.SqlQuery = insertNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(1, updateResult.AffectedRows);

            string selectNumberStatement = string.Format("SELECT {0} FROM {1} WHERE {0}={2};", columnName, table, value);
            Console.WriteLine(selectNumberStatement);
            _statement.SqlQuery = selectNumberStatement;
            QueryResult queryResult = _statement.ExecuteQuery();
            Assert.Equal(1, queryResult.RowCount);
            while (true)
            {
                RecordBatch nextBatch = queryResult.Stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                Decimal128Array queryResultArray = (Decimal128Array)nextBatch.Column(0);
                for (int i = 0; i < queryResultArray.Length; i++)
                {
                    Assert.Equal(value, queryResultArray.GetSqlDecimal(i));
                }
            }
        }

        private string CreateTable(string columns)
        {
            string tableName = string.Format("{0}.{1}{2}", _catalogSchema, s_testTablePrefix, Guid.NewGuid().ToString().Replace("-",""));
            string createTableStatement = string.Format("CREATE TABLE {0} ({1})",tableName,columns);
            _statement.SqlQuery = createTableStatement;
            _statement.ExecuteUpdate();
            return tableName;
        }

        private void DeleteFromTable(string tableName, string whereClause, int expectedRowsAffected)
        {
            string deleteNumberStatement = string.Format("DELETE FROM {0} WHERE {1};", tableName, whereClause);
            Console.WriteLine(deleteNumberStatement);
            _statement.SqlQuery = deleteNumberStatement;
            UpdateResult updateResult = _statement.ExecuteUpdate();
            Assert.Equal(expectedRowsAffected, updateResult.AffectedRows);
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

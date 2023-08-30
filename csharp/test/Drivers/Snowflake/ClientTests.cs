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
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using Apache.Arrow.Adbc.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
{
    /// <summary>
    /// Class for testing the ADBC Client using the Snowflake ADBC driver.
    /// </summary>
    [TestClass]
    public class ClientTests
    {
        [TestMethod]
        public void CanClientExecuteQuery()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeclientconfig.json");

            long count = 0;

            using (Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnection(testConfiguration))
            {
                AdbcCommand adbcCommand = new AdbcCommand(testConfiguration.Query, adbcConnection);

                adbcConnection.Open();

                AdbcDataReader reader = adbcCommand.ExecuteReader();

                try
                {
                    while (reader.Read())
                    {
                        count++;
                    }
                }
                finally { reader.Close(); }
            }

            Assert.AreEqual(testConfiguration.ExpectedResultsCount, count);
        }

        [TestMethod]
        [Ignore]
        public void TypeCheckUsingPrivateKey()
        {
            //int count = 0;

            //// follow a typical docs pattern of connecting using a reader

            //using (DbConnection connection = AdfGetSnowflakeAdbcConnection(usePrivateKey: true))
            //{
            //    string query = "select NUMBERTYPE from TEST_DB.PUBLIC.TEST_ALLTYPE";

            //    DbCommand command = connection.CreateCommand();
            //    command.CommandText = query;

            //    connection.Open();

            //    DbDataReader reader = command.ExecuteReader(CommandBehavior.Default);

            //    try
            //    {
            //        while (reader.Read())
            //        {
            //            count++;
            //        }
            //    }
            //    finally { reader.Close(); }
            //}

            //Assert.AreEqual(3, count);
        }

        [TestMethod]
        [Ignore]
        public void CanClientExecuteUpdate()
        {
           
        }

        [TestMethod]
        [Ignore]
        public void CanClientExecuteUpdateUsingExecuteReader()
        {
            
        }

        [TestMethod()]
        [Ignore]
        public void VerifyBadQueryGeneratesError()
        {
            
        }

        [TestMethod]
        [Ignore]
        public void VerifyGetStringFromDecimal()
        {
            
        }

        [TestMethod]
        public void CanReadColumnSchema()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeclientconfig.json");

            Client.AdbcConnection dbConnection = GetSnowflakeAdbcConnection(testConfiguration);

            DbCommand dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandText = testConfiguration.Query;

            dbConnection.Open();

            DbDataReader reader = dbCommand.ExecuteReader(System.Data.CommandBehavior.SchemaOnly);

            Dictionary<string, System.Type> types = new Dictionary<string, System.Type>();

            foreach (DbColumn dc in reader.GetColumnSchema())
            {
                types.Add(dc.ColumnName, dc.DataType);

                if (dc.DataType == typeof(decimal) ||
                    dc.DataType == typeof(double) ||
                    dc.DataType == typeof(float))
                {
                    Assert.IsTrue(dc.NumericPrecision.HasValue);
                    Assert.IsTrue(dc.NumericScale.HasValue);
                }
            }

            Assert.AreEqual(testConfiguration.ExpectedResultsCount, types.Keys.Count);
        }

        [TestMethod]
        [Ignore]
        public void VerifyTypesAndValues()
        {
           
        }

        [TestMethod]
        public void CanReadTableSchema()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeclientconfig.json");

            Client.AdbcConnection dbConnection = GetSnowflakeAdbcConnection(testConfiguration);

            DbCommand dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandText = testConfiguration.Query;

            dbConnection.Open();

            DbDataReader reader = dbCommand.ExecuteReader(CommandBehavior.SchemaOnly);

            Dictionary<string, string> types = new Dictionary<string, string>();

            DataTable table = reader.GetSchemaTable();

            foreach (DataRow row in table?.Rows)
            {
                types.Add(row[SchemaTableColumn.ColumnName].ToString(), row[SchemaTableColumn.DataType].ToString());
            }

            Assert.AreEqual(testConfiguration.ExpectedResultsCount, types.Keys.Count);

        }

        [TestMethod]
        public void VerifyDataTypes()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeconfig.json");

            using (Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnection(testConfiguration))
            {
                DbCommand dbCommand = adbcConnection.CreateCommand();
                dbCommand.CommandText = "select top 1 * from DEMO_DB.PUBLIC.ADBC_ALLTYPES";

                adbcConnection.Open();

                DbDataReader reader = dbCommand.ExecuteReader();

                if (reader.Read())
                {
                    var column_schema = reader.GetColumnSchema();
                    DataTable dataTable = reader.GetSchemaTable();

                    AssertType<long>("NUMBERTYPE", reader, column_schema, dataTable);
                    AssertType<long>("DECIMALTYPE", reader, column_schema, dataTable);
                    AssertType<long>("NUMERICTYPE", reader, column_schema, dataTable);
                    AssertType<int>("INTTYPE", reader, column_schema, dataTable);
                    AssertType<int>("INTEGERTYPE", reader, column_schema, dataTable);
                    AssertType<long>("BIGINTTYPE", reader, column_schema, dataTable);
                    AssertType<short>("SMALLINTTYPE", reader, column_schema, dataTable);
                    AssertType<sbyte>("TINYINTTYPE", reader, column_schema, dataTable);
                    AssertType<byte>("BYTEINTTYPE", reader, column_schema, dataTable);
                    AssertType<float>("FLOATTYPE", reader, column_schema, dataTable);
                    AssertType<float>("FLOAT4TYPE", reader, column_schema, dataTable);
                    AssertType<float>("FLOAT8TYPE", reader, column_schema, dataTable);
                    AssertType<double>("DOUBLETYPE", reader, column_schema, dataTable);
                    AssertType<string>("DOUBLEPRECISIONTYPE", reader, column_schema, dataTable);
                    AssertType<string>("REALTYPE", reader, column_schema, dataTable);
                    AssertType<string>("VARCHARTYPE", reader, column_schema, dataTable);
                    AssertType<string>("CHARTYPE", reader, column_schema, dataTable);
                    AssertType<string>("CHARACTERTYPE", reader, column_schema, dataTable);
                    AssertType<string>("STRINGTYPE", reader, column_schema, dataTable);
                    AssertType<string>("TEXTTYPE", reader, column_schema, dataTable);
                    AssertType<string>("BINARYTYPE", reader, column_schema, dataTable);
                    AssertType<string>("VARBINARYTYPE", reader, column_schema, dataTable);
                    AssertType<bool>("BOOLEANTYPE", reader, column_schema, dataTable);
                    AssertType<DateTime>("DATETYPE", reader, column_schema, dataTable);
                    AssertType<DateTime>("DATETIMETYPE", reader, column_schema, dataTable);
                    AssertType<DateTime>("TIMETYPE", reader, column_schema, dataTable);
                    AssertType<DateTimeOffset>("TIMESTAMPTYPE", reader, column_schema, dataTable);
                    AssertType<DateTimeOffset>("TIMESTAMPLTZTYPE", reader, column_schema, dataTable);
                    AssertType<DateTimeOffset>("TIMESTAMPNTZTYPE", reader, column_schema, dataTable);
                    AssertType<DateTimeOffset>("TIMESTAMPTZTYPE", reader, column_schema, dataTable);
                }
            }
        }

        private void AssertType<T>(string name, DbDataReader reader, ReadOnlyCollection<DbColumn> column_schema, DataTable dataTable)
        {
            Type arrowType = column_schema.Where(x => x.ColumnName == name).FirstOrDefault()?.DataType;
            Type dataTableType = null;

            foreach (DataRow row in dataTable.Rows)
            {
                if (row.ItemArray[0].ToString() == name)
                {
                    dataTableType = row.ItemArray[2] as Type;
                }
            }

            Type netType = reader[name]?.GetType();

            Assert.IsTrue(arrowType == typeof(T), $"{name} is {arrowType.Name} and not {typeof(T).Name} in the column schema");

            Assert.IsTrue(dataTableType == typeof(T), $"{name} is {dataTableType.Name} and not {typeof(T).Name} in the data table");

            if (netType != null) 
                Assert.IsTrue(netType == typeof(T), $"{name} is {netType.Name} and not {typeof(T).Name} in the reader");
        }

        [TestMethod]
        public void CanReadTableSchemaTypes()
        {
            
        }

        private Client.AdbcConnection GetSnowflakeAdbcConnectionUsingConnectionString(SnowflakeTestConfiguration testConfiguration)
        {
            DbConnectionStringBuilder builder = new DbConnectionStringBuilder(true);
            builder["account"] = testConfiguration.Account;
            builder["warehouse"] = testConfiguration.Warehouse;
            builder["user"] = testConfiguration.User;

            if (!string.IsNullOrEmpty(testConfiguration.AuthenticationTokenPath))
            {
                string privateKey = File.ReadAllText(testConfiguration.AuthenticationTokenPath);
                builder["AUTHENTICATOR"] = testConfiguration.AuthenticationType;
                builder["PRIVATE_KEY"] = privateKey;
            }
            else
            {
                builder["password"] = testConfiguration.Password;
            }

            Dictionary<string, string> parameters = new Dictionary<string, string>();

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            return new Client.AdbcConnection(builder.ConnectionString)
            {
                AdbcDriver = snowflakeDriver
            };
        }

        private Client.AdbcConnection GetSnowflakeAdbcConnection(SnowflakeTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);

            Client.AdbcConnection adbcConnection = new Client.AdbcConnection(
                snowflakeDriver,
                parameters: parameters,
                options: new Dictionary<string, string>()
            );

            return adbcConnection;
        }
    }
}

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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    /// <summary>
    /// Class for testing the ADBC Client using the BigQuery ADBC driver.
    /// </summary>
    [TestClass]
    public class ClientTests
    {
        /// <summary>
        /// Validates if the client can connect to a live server and
        /// parse the results.
        /// </summary>
        [TestMethod]
        public void CanClientExecuteQuery()
        {
            BigQueryTestConfiguration testConfiguration = Utils.GetTestConfiguration<BigQueryTestConfiguration>("resources/bigqueryconfig.json");

            long count = 0;

            using (Client.AdbcConnection adbcConnection = GetAdbcConnection(testConfiguration))
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


        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        [TestMethod]
        public void CanClientExecuteUpdate()
        {
            BigQueryTestConfiguration testConfiguration = Utils.GetTestConfiguration<BigQueryTestConfiguration>("resources/bigqueryconfig.json");

            using (Client.AdbcConnection adbcConnection = GetAdbcConnection(testConfiguration))
            {
                adbcConnection.Open();

                string[] queries = BigQueryTestingUtils.GetQueries(testConfiguration);

                List<int> expectedResults = new List<int>() { -1, 1, 1 };

                // the last query is blank
                for (int i = 0; i < queries.Length - 1; i++)
                {
                    string query = queries[i];
                    AdbcCommand adbcCommand = adbcConnection.CreateCommand();
                    adbcCommand.CommandText = query;

                    int rows = adbcCommand.ExecuteNonQuery();

                    Assert.AreEqual(expectedResults[i], rows, $"The expected affected rows do not match the actual affected rows at position {i}.");
                }
            }
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        [TestMethod]
        public void VerifyTypesAndValues()
        {
            BigQueryTestConfiguration testConfiguration = Utils.GetTestConfiguration<BigQueryTestConfiguration>("resources/bigqueryconfig.json");

            Client.AdbcConnection dbConnection = GetAdbcConnection(testConfiguration);

            dbConnection.Open();
            DbCommand dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandText = testConfiguration.Query;

            DbDataReader reader = dbCommand.ExecuteReader(CommandBehavior.Default);

            if (reader.Read())
            {
                var column_schema = reader.GetColumnSchema();
                DataTable dataTable = reader.GetSchemaTable();

                List<ColumnNetTypeArrowTypeValue> expectedValues = SampleData.GetSampleData();

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    object value = reader.GetValue(i);
                    ColumnNetTypeArrowTypeValue ctv = expectedValues[i];

                    AssertTypeAndValue(ctv, value, reader, column_schema, dataTable);
                }
            }
        }

        private void AssertTypeAndValue(ColumnNetTypeArrowTypeValue ctv, object value, DbDataReader reader, ReadOnlyCollection<DbColumn> column_schema, DataTable dataTable)
        {
            string name = ctv.Name;
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

            Assert.IsTrue(arrowType == ctv.ExpectedNetType, $"{name} is {arrowType.Name} and not {ctv.ExpectedNetType.Name} in the column schema");

            Assert.IsTrue(dataTableType == ctv.ExpectedNetType, $"{name} is {dataTableType.Name} and not {ctv.ExpectedNetType.Name} in the data table");

            if (netType != null)
            {
                if(netType.BaseType.Name.Contains("PrimitiveArray") && value != null)
                {
                    object internalValue = value.GetType().GetMethod("GetValue").Invoke(value, new object[] { 0 });

                    Assert.IsTrue(internalValue.GetType() == ctv.ExpectedNetType, $"{name} is {netType.Name} and not {ctv.ExpectedNetType.Name} in the reader");
                }
                else
                {
                    Assert.IsTrue(netType == ctv.ExpectedNetType, $"{name} is {netType.Name} and not {ctv.ExpectedNetType.Name} in the reader");
                }
            }

            if (value != null)
            {
                if (!value.GetType().BaseType.Name.Contains("PrimitiveArray"))
                {
                    Assert.AreEqual(ctv.ExpectedNetType, value.GetType(), $"Expected type does not match actual type for {ctv.Name}");

                    if (value.GetType() == typeof(byte[]))
                    {
                        byte[] actualBytes = (byte[])value;
                        byte[] expectedBytes = (byte[])ctv.ExpectedValue;

                        Assert.IsTrue(actualBytes.SequenceEqual(expectedBytes), $"byte[] values do not match expected values for {ctv.Name}");
                    }
                    else
                    {
                        Assert.AreEqual(ctv.ExpectedValue, value, $"Expected value does not match actual value for {ctv.Name}");
                    }
                }
                else
                {
                    IEnumerable list = value.GetType().GetMethod("ToList").Invoke(value, new object[] { false }) as IEnumerable;

                    IEnumerable expectedList = ctv.ExpectedValue.GetType().GetMethod("ToList").Invoke(ctv.ExpectedValue, new object[] { false }) as IEnumerable;

                    int i = -1;

                    foreach(var actual in list)
                    {
                        i++;
                        int j = -1;

                        foreach(var expected in expectedList)
                        {
                            j++;

                            if(i == j)
                            {
                                Assert.AreEqual(expected, actual, $"Expected value does not match actual value for {ctv.Name} at {i}");
                            }
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine($"The value for {ctv.Name} is null and cannot be verified");
            }
        }

        //private Client.AdbcConnection GetSnowflakeAdbcConnectionUsingConnectionString(BigQueryTestConfiguration testConfiguration)
        //{
        //    // see https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html

        //    DbConnectionStringBuilder builder = new DbConnectionStringBuilder(true);

        //    builder["adbc.snowflake.sql.account"] = testConfiguration.Account;
        //    builder["adbc.snowflake.sql.warehouse"] = testConfiguration.Warehouse;
        //    builder["username"] = testConfiguration.User;

        //    if (!string.IsNullOrEmpty(testConfiguration.AuthenticationTokenPath))
        //    {
        //        string privateKey = File.ReadAllText(testConfiguration.AuthenticationTokenPath);
        //        builder["adbc.snowflake.sql.auth_type"] = testConfiguration.AuthenticationType;
        //        builder["adbc.snowflake.sql.client_option.auth_token"] = privateKey;
        //    }
        //    else
        //    {
        //        builder["password"] = testConfiguration.Password;
        //    }

        //    AdbcDriver snowflakeDriver = BigQueryTestingUtils.GetBigQueryAdbcDriver(testConfiguration);

        //    return new Client.AdbcConnection(builder.ConnectionString)
        //    {
        //        AdbcDriver = snowflakeDriver
        //    };
        //}

        private Client.AdbcConnection GetAdbcConnection(BigQueryTestConfiguration testConfiguration)
        {
            return new Client.AdbcConnection(
                new BigQueryDriver(),
                BigQueryTestingUtils.GetBigQueryParameters(testConfiguration),
                new Dictionary<string,string>()
            );
        }
    }
}

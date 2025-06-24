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
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Class for testing the ADBC Client using the Snowflake ADBC driver.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created
    /// for the other queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class ClientTests
    {
        public ClientTests()
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
        }

        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        [SkippableFact, Order(1)]
        public void CanClientExecuteUpdate()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

                List<int> expectedResults = new List<int>() { -1, 1, 1 };

                Tests.ClientTests.CanClientExecuteUpdate(adbcConnection, testConfiguration, queries, expectedResults);
            }
        }

        /// <summary>
        /// Validates if the client execute updates using the reader.
        /// </summary>
        [SkippableFact, Order(2)]
        public void CanClientExecuteUpdateUsingExecuteReader()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                adbcConnection.Open();

                string[] queries = SnowflakeTestingUtils.GetQueries(testConfiguration);

                List<object> expectedResults = new List<object>() { $"Table {testConfiguration.Metadata.Table} successfully created.", new SqlDecimal(1L), new SqlDecimal(1L) };

                for (int i = 0; i < queries.Length; i++)
                {
                    string query = queries[i];
                    AdbcCommand adbcCommand = adbcConnection.CreateCommand();
                    adbcCommand.CommandText = query;

                    AdbcDataReader reader = adbcCommand.ExecuteReader(CommandBehavior.Default);

                    if (reader.Read())
                    {
                        Assert.True(expectedResults[i].Equals(reader.GetValue(0)), $"The expected affected rows do not match the actual affected rows at position {i}.");
                    }
                    else
                    {
                        Assert.Fail("Could not read the records");
                    }
                }
            }
        }

        /// <summary>
        /// Validates if the client can get the schema.
        /// </summary>
        [SkippableFact, Order(3)]
        public void CanClientGetSchema()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientGetSchema(adbcConnection, testConfiguration);
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQuery()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        // <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQueryWithNoResults()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Query = "SELECT * WHERE 0=1";
            testConfiguration.ExpectedResultsCount = 0;

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        // <summary>
        /// Validates if the client can connect to a live server and execute a parameterized query.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteParameterizedQuery()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Query = "SELECT ? as A, ? as B, ? as C, * FROM (SELECT column1 FROM (VALUES (1), (2), (3))) WHERE column1 < ?";
            testConfiguration.ExpectedResultsCount = 1;

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration, command =>
                {
                    DbParameter CreateParameter(DbType dbType, object value)
                    {
                        DbParameter result = command.CreateParameter();
                        result.DbType = dbType;
                        result.Value = value;
                        return result;
                    }

                    // TODO: Add tests for decimal and time once supported by the driver or gosnowflake
                    command.Parameters.Add(CreateParameter(DbType.Int32, 2));
                    command.Parameters.Add(CreateParameter(DbType.String, "text"));
                    command.Parameters.Add(CreateParameter(DbType.Double, 2.5));
                    command.Parameters.Add(CreateParameter(DbType.Int32, 2));
                });
            }
        }

        // <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQueryWithShowTerseTable()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Query = "SHOW TERSE TABLES";
            testConfiguration.ExpectedResultsCount = 0;

            // Throw exception Apache.Arrow.Adbc.AdbcException
            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        // <summary>
        /// Validates if the client can connect to a live server
        /// and parse the results.
        /// </summary>
        [SkippableFact, Order(4)]
        public void CanClientExecuteQueryWithShowTable()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.Query = "SHOW TABLES";
            testConfiguration.ExpectedResultsCount = 0;

            // Throw exception Apache.Arrow.Adbc.AdbcException
            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        /// <summary>
        /// Validates if the client can connect to a live server
        /// using a connection string / private key and parse the results.
        /// </summary>
        [SkippableFact, Order(5)]
        public void CanClientExecuteQueryUsingPrivateKey()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            Skip.If(testConfiguration.Authentication.SnowflakeJwt is null, "JWT authentication is not configured");

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration, SnowflakeAuthentication.AuthJwt))
            {
                Tests.ClientTests.CanClientExecuteQuery(adbcConnection, testConfiguration);
            }
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        [SkippableFact, Order(6)]
        public void VerifyTypesAndValues()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                SampleDataBuilder sampleDataBuilder = SnowflakeData.GetSampleData();

                Tests.ClientTests.VerifyTypesAndValues(adbcConnection, sampleDataBuilder);
            }
        }

        [SkippableFact, Order(6)]
        public void VerifyTimestampPrecision()
        {
            string query = "SELECT " +
                              "TO_TIMESTAMP('9999-12-31 00:00:00') December31_9999, " +
                              "TO_TIMESTAMP('2001-09-11 13:46:00') As September11_2001, " +
                              "TO_TIMESTAMP('33-04-03 15:00:00') as April3_0033";

            List<ColumnNetTypeArrowTypeValue> expectedMicrosecondValues = new List<ColumnNetTypeArrowTypeValue>()
            {
                new ColumnNetTypeArrowTypeValue("DECEMBER31_9999", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(9999, 12, 31, 0, 0, 0), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("SEPTEMBER11_2001", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2001, 9, 11, 13, 46, 0), TimeSpan.Zero)),
                new ColumnNetTypeArrowTypeValue("APRIL3_0033", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(0033, 4, 3, 15, 0, 0), TimeSpan.Zero)),
            };

            // if using microseconds as the max precision, everything returns correctly
            ValidateTimestampPrecision(SnowflakeConstants.OptionValueMicroseconds, query, expectedMicrosecondValues);

            List<ColumnNetTypeArrowTypeValue> expectedNanoseconddValues = new List<ColumnNetTypeArrowTypeValue>()
            {
                // 572833941680662774 ticks = 3/29/1816 5:56:08 AM +00:00 and not what we asked for :/
                new ColumnNetTypeArrowTypeValue("DECEMBER31_9999", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(572833941680662774, TimeSpan.Zero)),

                // within normal range, so the values return as expected
                new ColumnNetTypeArrowTypeValue("SEPTEMBER11_2001", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(new DateTime(2001, 9, 11, 13, 46, 0), TimeSpan.Zero)),

                // 563580782211286549 ticks = 12/1/1786 1:43:41 PM +00:00 and not what we asked for :/
                new ColumnNetTypeArrowTypeValue("APRIL3_0033", typeof(DateTimeOffset), typeof(TimestampType), new DateTimeOffset(563580782211286549, TimeSpan.Zero)),
            };

            // if you use the default (nanoseconds) precision, the values are incorrect
            ValidateTimestampPrecision(SnowflakeConstants.OptionValueNanoseconds, query, expectedNanoseconddValues);

            // if `error on overflow` is enforced, then an error is thrown
            Assert.Throws<Exception>(() => ValidateTimestampPrecision(SnowflakeConstants.OptionValueNanosecondsNoOverflow, query, expectedNanoseconddValues));
        }

        [SkippableFact, Order(6)]
        public void VerifyTimestampPrecisionJson()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.MaxTimestampPrecision = "microseconds";

            string tempTable = "pk_" + Guid.NewGuid().ToString().Replace("-", "");

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();
                sampleDataBuilder.Samples.Add(
                    new SampleData()
                    {
                        // create the table
                        PreQueryCommands = new List<string> {
                            @$"CREATE OR REPLACE TABLE {testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{tempTable}(
                             id INT PRIMARY KEY,
                             name STRING
                            );"
                        },
                        // run the SHOW PRIMARY KEYS command, which returns data as json
                        Query = $"SHOW PRIMARY KEYS IN TABLE {testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{tempTable}",
                        ExpectedValues = new List<ColumnNetTypeArrowTypeValue>()
                        {
                            new ColumnNetTypeArrowTypeValue("created_on", typeof(DateTimeOffset), typeof(TimestampType), true,
                                v =>
                                {
                                    if (v is DateTimeOffset createdOn)
                                    {
                                        // the key will have just been created, so just compare that the two times
                                        // are only a few minutes apart
                                        DateTimeOffset now = DateTimeOffset.UtcNow;
                                        TimeSpan difference = now - createdOn;
                                        return difference.Duration() < TimeSpan.FromMinutes(2);
                                    }
                                    else
                                    {
                                        return false;
                                     }
                                }),
                            new ColumnNetTypeArrowTypeValue("database_name", typeof(string), typeof(StringType), testConfiguration.Metadata.Catalog),
                            new ColumnNetTypeArrowTypeValue("schema_name", typeof(string), typeof(StringType), testConfiguration.Metadata.Schema),
                            new ColumnNetTypeArrowTypeValue("table_name", typeof(string), typeof(StringType), tempTable.ToUpper()),
                            new ColumnNetTypeArrowTypeValue("column_name", typeof(string), typeof(StringType), "ID"),
                            new ColumnNetTypeArrowTypeValue("key_sequence", typeof(SqlDecimal), typeof(Decimal128Type), new SqlDecimal(1m)),

                            // we dont control these, but also don't care about the value
                            new ColumnNetTypeArrowTypeValue("constraint_name", typeof(string), typeof(StringType), true, v => { return true; }),
                            new ColumnNetTypeArrowTypeValue("rely", typeof(string), typeof(StringType), true, v => { return true; }),
                            new ColumnNetTypeArrowTypeValue("comment", typeof(string), typeof(StringType), null)
                        },
                        // drop the table
                        PostQueryCommands = new List<string>() {
                            $"DROP TABLE {testConfiguration.Metadata.Catalog}.{testConfiguration.Metadata.Schema}.{tempTable}"
                        }
                    });

                Tests.ClientTests.VerifyTypesAndValues(adbcConnection, sampleDataBuilder);
            }
        }


        private void ValidateTimestampPrecision(string precision, string query, List<ColumnNetTypeArrowTypeValue> expectedValues)
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);
            testConfiguration.MaxTimestampPrecision = precision;

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                SampleDataBuilder sampleDataBuilder = new SampleDataBuilder();
                sampleDataBuilder.Samples.Add(
                   new SampleData()
                   {
                       Query = query,
                       ExpectedValues = expectedValues
                   });

                Tests.ClientTests.VerifyTypesAndValues(adbcConnection, sampleDataBuilder);
            }
        }

        [SkippableFact]
        public void VerifySchemaTables()
        {
            SnowflakeTestConfiguration testConfiguration = Utils.LoadTestConfiguration<SnowflakeTestConfiguration>(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE);

            using (Adbc.Client.AdbcConnection adbcConnection = GetSnowflakeAdbcConnectionUsingConnectionString(testConfiguration))
            {
                adbcConnection.Open();

                var collections = adbcConnection.GetSchema("MetaDataCollections");
                Assert.Equal(7, collections.Rows.Count);
                Assert.Equal(2, collections.Columns.Count);

                var restrictions = adbcConnection.GetSchema("Restrictions");
                Assert.Equal(11, restrictions.Rows.Count);
                Assert.Equal(3, restrictions.Columns.Count);

                var catalogs = adbcConnection.GetSchema("Catalogs");
                Assert.Single(catalogs.Columns);
                var catalog = (string?)catalogs.Rows[0].ItemArray[0];

                catalogs = adbcConnection.GetSchema("Catalogs", new[] { catalog });
                Assert.Equal(1, catalogs.Rows.Count);

                var schemas = adbcConnection.GetSchema("Schemas", new[] { catalog });
                Assert.Equal(2, schemas.Columns.Count);

                var schema = "INFORMATION_SCHEMA";
                schemas = adbcConnection.GetSchema("Schemas", new[] { catalog, schema });
                Assert.Equal(1, schemas.Rows.Count);

                var tableTypes = adbcConnection.GetSchema("TableTypes");
                Assert.Single(tableTypes.Columns);

                var tables = adbcConnection.GetSchema("Tables", new[] { catalog, schema });
                Assert.Equal(4, tables.Columns.Count);
                Assert.Equal(32, tables.Rows.Count);

                var columns = adbcConnection.GetSchema("Columns", new[] { catalog, schema });
                Assert.Equal(16, columns.Columns.Count);
                Assert.Equal(441, columns.Rows.Count);
            }
        }

        private Adbc.Client.AdbcConnection GetSnowflakeAdbcConnectionUsingConnectionString(SnowflakeTestConfiguration testConfiguration, string? authType = null)
        {
            // see https://arrow.apache.org/adbc/0.5.1/driver/snowflake.html

            DbConnectionStringBuilder builder = new DbConnectionStringBuilder(true);
            builder[SnowflakeParameters.ACCOUNT] = testConfiguration.Account;
            builder[SnowflakeParameters.WAREHOUSE] = testConfiguration.Warehouse;
            builder[SnowflakeParameters.HOST] = testConfiguration.Host;
            builder[SnowflakeParameters.DATABASE] = testConfiguration.Database;
            builder[SnowflakeParameters.USERNAME] = testConfiguration.User;
            builder[SnowflakeParameters.MAX_TIMESTAMP_PRECISION] = testConfiguration.MaxTimestampPrecision;

            if (authType == SnowflakeAuthentication.AuthJwt)
            {
                string privateKey = testConfiguration.Authentication.SnowflakeJwt!.PrivateKey;
                builder[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthJwt;
                builder[SnowflakeParameters.PKCS8_VALUE] = privateKey;
                builder[SnowflakeParameters.USERNAME] = testConfiguration.Authentication.SnowflakeJwt.User;
                if (!string.IsNullOrEmpty(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase))
                {
                    builder[SnowflakeParameters.PKCS8_PASS] = testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase;
                }
            }
            else if (authType == SnowflakeAuthentication.AuthOAuth)
            {
                builder[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthOAuth;
                builder[SnowflakeParameters.AUTH_TOKEN] = testConfiguration.Authentication.OAuth!.Token;
                if (testConfiguration.Authentication.OAuth.User != null)
                {
                    builder[SnowflakeParameters.USERNAME] = testConfiguration.Authentication.OAuth.User;
                }
            }
            else if (string.IsNullOrEmpty(authType) || authType == SnowflakeAuthentication.AuthSnowflake)
            {
                // if no auth type is specified, use the snowflake auth
                builder[SnowflakeParameters.AUTH_TYPE] = SnowflakeAuthentication.AuthSnowflake;
                builder[SnowflakeParameters.USERNAME] = testConfiguration.Authentication.Default!.User;
                builder[SnowflakeParameters.PASSWORD] = testConfiguration.Authentication.Default.Password;
            }
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration);
            return new Adbc.Client.AdbcConnection(builder.ConnectionString)
            {
                AdbcDriver = snowflakeDriver
            };
        }
    }
}

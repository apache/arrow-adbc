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
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Moq;
using Xunit;
using AdbcClient = Apache.Arrow.Adbc.Client;

namespace Apache.Arrow.Adbc.Tests.Client
{
    public class ClientTests
    {
        [Theory]
        [InlineData(DecimalBehavior.OverflowDecimalAsString, "79228162514264337593543950335", 29, 0, typeof(decimal))]
        [InlineData(DecimalBehavior.OverflowDecimalAsString, "792281625142643375935439503351", 30, 0, typeof(string))]
        [InlineData(DecimalBehavior.UseSqlDecimal, "792281625142643375935439503351", 30, 0, typeof(SqlDecimal))]
        public void TestDecimalValues(DecimalBehavior decimalBehavior, string value, int precision, int scale, Type expectedType)
        {
            AdbcDataReader rdr = GetMoqDataReader(decimalBehavior, value, precision, scale);
            Assert.True(rdr.Read());
            object rdrValue = rdr.GetValue(0);

            Assert.True(rdrValue.GetType().Equals(expectedType));
        }

        /// <summary>
        /// Demonstrates the OnGetValue method of an AdbcDataReader.
        /// </summary>
        /// <param name="treatIntegersAsStrings">True/False to treat integers as strings.</param>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestOnGetValue(bool treatIntegersAsStrings)
        {
            AdbcDataReader rdr = GetMoqDataReaderForIntegers();

            if (treatIntegersAsStrings)
            {
                rdr.OnGetValue += (o, e) =>
                {
                    if (o != null)
                    {
                        Int32Array? ints = o as Int32Array;

                        if (ints != null)
                        {
                            int? value = ints.GetValue(e);

                            if (value.HasValue)
                                return value.Value.ToString();
                            else
                                return string.Empty;
                        }
                    }

                    return string.Empty;
                };
            }

            while (rdr.Read())
            {
                object? rdrValue = rdr.GetValue(0);

                if (treatIntegersAsStrings)
                {
                    Assert.True(rdrValue.GetType().Equals(typeof(string)));
                }
                else
                {
                    if (rdrValue != DBNull.Value)
                        Assert.True(rdrValue.GetType().Equals(typeof(int)));
                }
            }
        }

        private AdbcDataReader GetMoqDataReader(DecimalBehavior decimalBehavior, string value, int precision, int scale)
        {
            SqlDecimal sqlDecimal = SqlDecimal.Parse(value);

            List<KeyValuePair<string, string>> metadata = new List<KeyValuePair<string, string>>();
            metadata.Add(new KeyValuePair<string, string>("precision", precision.ToString()));
            metadata.Add(new KeyValuePair<string, string>("scale", scale.ToString()));

            List<Field> fields = new List<Field>();
            fields.Add(new Field("Decimal128t", new Decimal128Type(precision, scale), true, metadata));

            Schema schema = new Schema(fields, metadata);
            Decimal128Array.Builder builder = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
            builder.Append(sqlDecimal);
            Decimal128Array array = builder.Build();

            List<IArrowArray> values = new List<IArrowArray>() { array };

            List<RecordBatch> records = new List<RecordBatch>()
            {
                new RecordBatch(schema, values, array.Length)
            };

            MockArrayStream mockArrayStream = new MockArrayStream(schema, records);
            QueryResult queryResult = new QueryResult(1, mockArrayStream);

            Mock<AdbcStatement> mockStatement = new Mock<AdbcStatement>();
            mockStatement.Setup(x => x.ExecuteQuery()).Returns(queryResult); ;

            Adbc.Client.AdbcConnection mockConnection = new Adbc.Client.AdbcConnection();
            mockConnection.DecimalBehavior = decimalBehavior;

            AdbcCommand cmd = new AdbcCommand(mockStatement.Object, mockConnection);

            AdbcDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        private AdbcDataReader GetMoqDataReaderForIntegers()
        {
            List<KeyValuePair<string, string>> metadata = new List<KeyValuePair<string, string>>();
            List<Field> fields = new List<Field>();
            fields.Add(new Field("TestIntegers", new Int32Type(), true, metadata));

            Schema schema = new Schema(fields, metadata);
            Int32Array.Builder numbersBuilder = new Int32Array.Builder();
            numbersBuilder.AppendRange(new List<int>() { 1, 2, 3 });
            numbersBuilder.AppendNull(); //null for #4
            numbersBuilder.Append(5);

            Int32Array numbersArray = numbersBuilder.Build();

            List<IArrowArray> values = new List<IArrowArray>() { numbersArray };

            List<RecordBatch> records = new List<RecordBatch>()
            {
                new RecordBatch(schema, values, numbersArray.Length)
            };

            MockArrayStream mockArrayStream = new MockArrayStream(schema, records);
            QueryResult queryResult = new QueryResult(1, mockArrayStream);

            Mock<AdbcStatement> mockStatement = new Mock<AdbcStatement>();
            mockStatement.Setup(x => x.ExecuteQuery()).Returns(queryResult); ;

            Adbc.Client.AdbcConnection mockConnection = new Adbc.Client.AdbcConnection();

            AdbcCommand cmd = new AdbcCommand(mockStatement.Object, mockConnection);

            AdbcDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        [Theory]
        [InlineData("(adbc.driver.value, 1, s)", "adbc.driver.value", 1, "s", true)]
        [InlineData("(somevalue,10, ms)", "somevalue", 10, "ms", true)]
        [InlineData("(somevalue,10, s)", "somevalue", 10, "s", true)]
        [InlineData("somevalue,10, s)", null, null, null, false)]
        [InlineData("(somevalue,10, s", null, null, null, false)]
        [InlineData("(some.value_goes.here,99,Q)", null, null, null, false)]
        [InlineData("some.value_goes.here,99,Q", null, null, null, false)]
        public void TestTimeoutParsing(string value, string? driverPropertyName, int? timeout, string? unit, bool success)
        {
            if (!success)
            {
                try
                {
                    ConnectionStringParser.ParseTimeoutValue(value);
                }
                catch (ArgumentOutOfRangeException) { }
                catch (InvalidOperationException) { }
                catch
                {
                    Assert.Fail("Unknown exception found");
                }
            }
            else
            {
                Assert.True(driverPropertyName != null);
                Assert.True(timeout != null);
                Assert.True(unit != null);

                TimeoutValue timeoutValue = ConnectionStringParser.ParseTimeoutValue(value);

                Assert.Equal(driverPropertyName, timeoutValue.DriverPropertyName);
                Assert.Equal(timeout, timeoutValue.Value);
                Assert.Equal(unit, timeoutValue.Units);
            }
        }

        [Theory]
        [ClassData(typeof(ConnectionParsingTestData))]
        internal void TestConnectionStringParsing(ConnectionStringExample connectionStringExample)
        {
            AdbcClient.AdbcConnection cn = new AdbcClient.AdbcConnection(connectionStringExample.ConnectionString);

            Mock<AdbcStatement> mockStatement = new Mock<AdbcStatement>();
            AdbcCommand cmd = new AdbcCommand(mockStatement.Object, cn);

            Assert.True(cn.StructBehavior == connectionStringExample.ExpectedStructBehavior);
            Assert.True(cn.DecimalBehavior == connectionStringExample.ExpectedDecimalBehavior);
            Assert.True(cn.ConnectionTimeout == connectionStringExample.ConnectionTimeout);

            if (!string.IsNullOrEmpty(connectionStringExample.CommandTimeoutProperty))
            {
                Assert.True(cmd.AdbcCommandTimeoutProperty == connectionStringExample.CommandTimeoutProperty);
                Assert.True(cmd.CommandTimeout == connectionStringExample.CommandTimeout);
            }
            else
            {
                Assert.Throws<InvalidOperationException>(() => cmd.AdbcCommandTimeoutProperty);
            }
        }
    }

    internal class ConnectionStringExample
    {
        public ConnectionStringExample(
            string connectionString,
            DecimalBehavior decimalBehavior,
            StructBehavior structBehavior,
            string connectionTimeoutPropertyName,
            int connectionTimeout,
            string commandTimeoutPropertyName,
            int commandTimeout)
        {
            ConnectionString = connectionString;
            ExpectedDecimalBehavior = decimalBehavior;
            ExpectedStructBehavior = structBehavior;
            ConnectionTimeoutProperty = connectionTimeoutPropertyName;
            ConnectionTimeout = connectionTimeout;
            CommandTimeoutProperty = commandTimeoutPropertyName;
            CommandTimeout = commandTimeout;
        }

        public string ConnectionString { get; }

        public string ConnectionTimeoutProperty { get; }

        public int ConnectionTimeout { get; }

        public DecimalBehavior ExpectedDecimalBehavior { get; }

        public StructBehavior ExpectedStructBehavior { get; }

        public string CommandTimeoutProperty { get; }

        public int CommandTimeout { get; }
    }

    /// <summary>
    /// Collection of <see cref="ConnectionStringExample"/> for testing statement timeouts."/>
    /// </summary>
    internal class ConnectionParsingTestData : TheoryData<ConnectionStringExample>
    {
        public ConnectionParsingTestData()
        {
            int defaultDbConnectionTimeout = 15;

            Add(new("StructBehavior=JsonString", default, StructBehavior.JsonString, "", defaultDbConnectionTimeout, "", 30));
            Add(new("StructBehavior=JsonString;AdbcCommandTimeout=(adbc.apache.statement.query_timeout_s,45,s)", default, StructBehavior.JsonString, "", defaultDbConnectionTimeout, "adbc.apache.statement.query_timeout_s", 45));
            Add(new("StructBehavior=JsonString;DecimalBehavior=OverflowDecimalAsString;AdbcConnectionTimeout=(adbc.spark.connect_timeout_ms,90,s);AdbcCommandTimeout=(adbc.apache.statement.query_timeout_s,45,s)", DecimalBehavior.OverflowDecimalAsString, StructBehavior.JsonString, "adbc.spark.connect_timeout_ms", 90, "adbc.apache.statement.query_timeout_s", 45));
        }
    }

    class MockArrayStream : IArrowArrayStream
    {
        private readonly List<RecordBatch> recordBatches;
        private readonly Schema schema;

        // start at -1 to use the count the number of calls as the index
        private int calls = -1;

        /// <summary>
        /// Initializes the TestArrayStream.
        /// </summary>
        /// <param name="schema">
        /// The Arrow schema.
        /// </param>
        /// <param name="recordBatches">
        /// A list of record batches.
        /// </param>
        public MockArrayStream(Schema schema, List<RecordBatch> recordBatches)
        {
            this.schema = schema;
            this.recordBatches = recordBatches;
        }

        public Schema Schema => this.schema;

        public void Dispose() { }

        /// <summary>
        /// Moves through the list of record batches.
        /// </summary>
        /// <param name="cancellationToken">
        /// Optional cancellation token.
        /// </param>
        public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            calls++;

            if (calls >= this.recordBatches.Count)
                return new ValueTask<RecordBatch>();
            else
                return new ValueTask<RecordBatch>(this.recordBatches[calls]);
        }
    }
}

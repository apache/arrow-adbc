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
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    // <summary>
    /// Performs tests related to connecting with the ADBC client.
    /// </summary>
    public class ClientTests
    {
        /// <summary>
        /// Validates if the client execute updates.
        /// </summary>
        /// <param name="adbcConnection">The <see cref="Adbc.Client.AdbcConnection"/> to use.</param>
        /// <param name="testConfiguration">The <see cref="TestConfiguration"/> to use</param>
        /// <param name="queries">The queries to run</param>
        /// <param name="expectedResults">The expected results (one per query)</param>
        public static void CanClientExecuteUpdate(
            Adbc.Client.AdbcConnection adbcConnection,
            TestConfiguration testConfiguration,
            string[] queries,
            IReadOnlyList<int> expectedResults,
            string? environmentName = null)
        {
            if (adbcConnection == null) throw new ArgumentNullException(nameof(adbcConnection));
            if (testConfiguration == null) throw new ArgumentNullException(nameof(testConfiguration));
            if (queries == null) throw new ArgumentNullException(nameof(queries));
            if (expectedResults == null) throw new ArgumentNullException(nameof(expectedResults));
            if (queries.Length != expectedResults.Count) throw new ArgumentException($"{nameof(queries)} and {nameof(expectedResults)} must have the same number of values");

            adbcConnection.Open();

            for (int i = 0; i < queries.Length; i++)
            {
                string query = queries[i];

                using AdbcCommand adbcCommand = adbcConnection.CreateCommand();

                adbcCommand.CommandText = query;

                int rows = adbcCommand.ExecuteNonQuery();

                Assert.True(expectedResults[i]==rows, Utils.FormatMessage("Expected results are not equal", environmentName));
            }
        }

        /// <summary>
        /// Validates if the client can get the schema.
        /// </summary>
        /// <param name="adbcConnection">The <see cref="Adbc.Client.AdbcConnection"/> to use.</param>
        /// <param name="testConfiguration">The <see cref="TestConfiguration"/> to use</param>
        /// <param name="customQuery">The custom query to use instead of query from <see cref="TestConfiguration.Query" /></param>"/>
        /// <param name="expectedColumnCount">The custom column count to use instead of query from <see cref="TestMetadata.ExpectedColumnCount" /></param>
        public static void CanClientGetSchema(
            Adbc.Client.AdbcConnection adbcConnection,
            TestConfiguration testConfiguration,
            string? customQuery = default,
            int? expectedColumnCount = default,
            string? environmentName = null)
        {
            if (adbcConnection == null) throw new ArgumentNullException(nameof(adbcConnection));
            if (testConfiguration == null) throw new ArgumentNullException(nameof(testConfiguration));

            adbcConnection.Open();

            using AdbcCommand adbcCommand = new AdbcCommand(customQuery ?? testConfiguration.Query, adbcConnection);
            using AdbcDataReader reader = adbcCommand.ExecuteReader(CommandBehavior.SchemaOnly);

            DataTable? table = reader.GetSchemaTable();

            // there is one row per field
            Assert.Equal(expectedColumnCount ?? testConfiguration.Metadata.ExpectedColumnCount, table?.Rows.Count);
        }

        /// <summary>
        /// Validates if the client can connect to a live server and
        /// parse the results.
        /// </summary>
        /// <param name="adbcConnection">The <see cref="Adbc.Client.AdbcConnection"/> to use.</param>
        /// <param name="testConfiguration">The <see cref="TestConfiguration"/> to use</param>
        /// <param name="additionalCommandOptionsSetter">Allows additional options to be set on the command before execution</param>
        public static void CanClientExecuteQuery(
            Adbc.Client.AdbcConnection adbcConnection,
            TestConfiguration testConfiguration,
            Action<AdbcCommand>? additionalCommandOptionsSetter = null,
            string? customQuery = default,
            int? expectedResultsCount = default,
            string? environmentName = null)
        {
            if (adbcConnection == null) throw new ArgumentNullException(nameof(adbcConnection));
            if (testConfiguration == null) throw new ArgumentNullException(nameof(testConfiguration));

            long count = 0;

            adbcConnection.Open();

            using AdbcCommand adbcCommand = new AdbcCommand(customQuery ?? testConfiguration.Query, adbcConnection);
            additionalCommandOptionsSetter?.Invoke(adbcCommand);
            using AdbcDataReader reader = adbcCommand.ExecuteReader();

            try
            {
                while (reader.Read())
                {
                    count++;

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        object value = reader.GetValue(i);

                        if (value == null)
                            value = "(null)";

                        // write out the values to ensure things like null are correctly returned
                        Console.WriteLine($"{reader.GetName(i)}: {value}");
                    }
                }
            }
            finally { reader.Close(); }

            Assert.Equal(expectedResultsCount ?? testConfiguration.ExpectedResultsCount, count);
        }

        /// <summary>
        /// Validates if the client is retrieving and converting values
        /// to the expected types.
        /// </summary>
        /// <param name="adbcConnection">The <see cref="Adbc.Client.AdbcConnection"/> to use.</param>
        /// <param name="sampleDataBuilder">The <see cref="SampleDataBuilder"/> to use</param>
        public static void VerifyTypesAndValues(
            Adbc.Client.AdbcConnection adbcConnection,
            SampleDataBuilder sampleDataBuilder,
            string? environmentName = null)
        {
            if (adbcConnection == null) throw new ArgumentNullException(nameof(adbcConnection));
            if (sampleDataBuilder == null) throw new ArgumentNullException(nameof(sampleDataBuilder));

            adbcConnection.Open();

            foreach (SampleData sample in sampleDataBuilder.Samples)
            {
                foreach (string preQueryCommandText in sample.PreQueryCommands)
                {
                    using AdbcCommand preQueryCommand = adbcConnection.CreateCommand();
                    preQueryCommand.CommandText = preQueryCommandText;
                    preQueryCommand.ExecuteNonQuery();
                }

                using AdbcCommand dbCommand = adbcConnection.CreateCommand();
                dbCommand.CommandText = sample.Query;

                using AdbcDataReader reader = dbCommand.ExecuteReader(CommandBehavior.Default);
                if (reader.Read())
                {
                    var column_schema = reader.GetColumnSchema();
                    DataTable? dataTable = reader.GetSchemaTable();
                    Assert.True(dataTable != null, Utils.FormatMessage("dataTable is null", environmentName) );

                    Assert.True(reader.FieldCount == sample.ExpectedValues.Count, Utils.FormatMessage($"{sample.ExpectedValues.Count} fields were expected but {reader.FieldCount} fields were returned for the query [{sample.Query}]", environmentName));

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        object value = reader.GetValue(i);
                        ColumnNetTypeArrowTypeValue ctv = sample.ExpectedValues[i];

                        AssertTypeAndValue(ctv, value, reader, column_schema, dataTable, sample.Query, environmentName);
                    }
                }

                foreach (string postQueryCommandText in sample.PostQueryCommands)
                {
                    using AdbcCommand preQueryCommand = adbcConnection.CreateCommand();
                    preQueryCommand.CommandText = postQueryCommandText;
                    preQueryCommand.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Validates a column contains the correct types and values.
        /// </summary>
        /// <param name="ctv"><see cref="ColumnNetTypeArrowTypeValue"/></param>
        /// <param name="value">The object's value</param>
        /// <param name="reader">The current reader</param>
        /// <param name="column_schema">The column schema from the reader</param>
        /// <param name="dataTable">The <see cref="DataTable"/></param>
        static void AssertTypeAndValue(
            ColumnNetTypeArrowTypeValue ctv,
            object value,
            DbDataReader reader,
            ReadOnlyCollection<DbColumn> column_schema,
            DataTable dataTable,
            string query,
            string? environmentName = null)
        {
            string name = ctv.Name;
            Type? clientArrowType = column_schema.Where(x => x.ColumnName == name).FirstOrDefault()?.DataType;

            Type? dataTableType = null;
            IArrowType? arrowType = null;

            foreach (DataRow row in dataTable.Rows)
            {
                if (row[SchemaTableColumn.ColumnName].ToString() == name)
                {
                    dataTableType = row[SchemaTableColumn.DataType] as Type;
                    arrowType = row[SchemaTableColumn.ProviderType] as IArrowType;
                }
            }

            Type? netType = reader[name]?.GetType();
            if (netType == typeof(DBNull)) netType = null;

            Assert.True(clientArrowType == ctv.ExpectedNetType, Utils.FormatMessage($"{name} is {clientArrowType.Name} and not {ctv.ExpectedNetType.Name} in the column schema for query [{query}]", environmentName));

            Assert.True(dataTableType == ctv.ExpectedNetType, Utils.FormatMessage($"{name} is {dataTableType.Name} and not {ctv.ExpectedNetType.Name} in the data table for query [{query}]", environmentName));

            if (arrowType is null)
                Assert.True(ctv.ExpectedArrowArrayType is null, Utils.FormatMessage($"{name} is null and not {ctv.ExpectedArrowArrayType!.Name} in the provider type for query [{query}]", environmentName));
            else
                Assert.True(arrowType.GetType() == ctv.ExpectedArrowArrayType, Utils.FormatMessage($"{name} is {arrowType.Name} and not {ctv.ExpectedArrowArrayType.Name} in the provider type for query [{query}]", environmentName));

            if (netType != null)
            {
                Assert.True(netType == ctv.ExpectedNetType, Utils.FormatMessage($"{name} is {netType.Name} and not {ctv.ExpectedNetType.Name} in the reader for query [{query}]", environmentName));
            }

            if (value != DBNull.Value)
            {
                var type = value.GetType();
                if (type.BaseType?.Name.Contains("PrimitiveArray") == false)
                {
                    Assert.True(ctv.ExpectedNetType == type, Utils.FormatMessage($"Expected type does not match actual type for {ctv.Name} for query [{query}]", environmentName));

                    if (value is byte[] actualBytes)
                    {
                        byte[]? expectedBytes = ctv.ExpectedValue as byte[];
                        Assert.True(expectedBytes != null && actualBytes.SequenceEqual(expectedBytes), Utils.FormatMessage($"byte[] values do not match expected values for {ctv.Name} for query [{query}]", environmentName));
                    }
                    else if (ctv.ExpectedValue is null)
                    {
                        Assert.True(value is null, Utils.FormatMessage($"Expected value [{ctv.ExpectedValue}] does not match actual value [{value}] for {ctv.Name} for query [{query}]", environmentName));
                    }
                    else
                    {
                        Assert.True(ctv.ExpectedValue.Equals(value), Utils.FormatMessage($"Expected value [{ctv.ExpectedValue}] does not match actual value [{value}] for {ctv.Name} for query [{query}]", environmentName));
                    }
                }
                else
                {
                    IEnumerable? list = value.GetType().GetMethod("ToList")?.Invoke(value, new object[] { false }) as IEnumerable;

                    IEnumerable? expectedList = ctv.ExpectedValue?.GetType().GetMethod("ToList")?.Invoke(ctv.ExpectedValue, new object[] { false }) as IEnumerable;

                    if (list == null) { throw new ArgumentNullException(nameof(list)); }
                    if (expectedList == null) { throw new ArgumentNullException(nameof(expectedList)); }

                    int i = -1;

                    foreach (var actual in list)
                    {
                        i++;
                        int j = -1;

                        foreach (var expected in expectedList)
                        {
                            j++;

                            if (i == j)
                            {
                                Assert.True(expected.Equals(actual), Utils.FormatMessage($"Expected value does not match actual value for {ctv.Name} at {i} for query [{query}]", environmentName));
                            }
                        }
                    }
                }
            }
            else
            {
                Assert.True(ctv.ExpectedValue == null, Utils.FormatMessage($"The value for {ctv.Name} is null and but it's expected value is not null for query [{query}]", environmentName));
            }
        }
    }
}

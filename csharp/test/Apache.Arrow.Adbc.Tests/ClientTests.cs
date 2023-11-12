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
using System.Collections.ObjectModel;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Linq;
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
        /// Validates a column contains the correct types and values.
        /// </summary>
        /// <param name="ctv"><see cref="ColumnNetTypeArrowTypeValue"/></param>
        /// <param name="value">The object's value</param>
        /// <param name="reader">The current reader</param>
        /// <param name="column_schema">The column schema from the reader</param>
        /// <param name="dataTable">The <see cref="DataTable"/></param>
        public static void AssertTypeAndValue(ColumnNetTypeArrowTypeValue ctv, object value, DbDataReader reader, ReadOnlyCollection<DbColumn> column_schema, DataTable dataTable)
        {
            string name = ctv.Name;
            Type clientArrowType = column_schema.Where(x => x.ColumnName == name).FirstOrDefault()?.DataType;

            Type dataTableType = null;
            IArrowType arrowType = null;

            foreach (DataRow row in dataTable.Rows)
            {
                if (row[SchemaTableColumn.ColumnName].ToString() == name)
                {
                    dataTableType = row[SchemaTableColumn.DataType] as Type;
                    arrowType = row[SchemaTableColumn.ProviderType] as IArrowType;
                }
            }

            Type netType = reader[name]?.GetType();

            Assert.True(clientArrowType == ctv.ExpectedNetType, $"{name} is {clientArrowType.Name} and not {ctv.ExpectedNetType.Name} in the column schema");

            Assert.True(dataTableType == ctv.ExpectedNetType, $"{name} is {dataTableType.Name} and not {ctv.ExpectedNetType.Name} in the data table");

            Assert.True(arrowType.GetType() == ctv.ExpectedArrowArrayType, $"{name} is {arrowType.Name} and not {ctv.ExpectedArrowArrayType.Name} in the provider type");

            if (netType != null)
            {
                if (netType.BaseType.Name.Contains("PrimitiveArray") && value != null)
                {
                    int length = Convert.ToInt32(value.GetType().GetProperty("Length").GetValue(value));

                    if (length > 0)
                    {
                        object internalValue = value.GetType().GetMethod("GetValue").Invoke(value, new object[] { 0 });

                        Assert.True(internalValue.GetType() == ctv.ExpectedNetType, $"{name} is {netType.Name} and not {ctv.ExpectedNetType.Name} in the reader");
                    }
                    else
                    {
                        Console.WriteLine($"Could not validate the values inside of {netType.Name} because it is empty");
                    }
                }
                else
                {
                    Assert.True(netType == ctv.ExpectedNetType, $"{name} is {netType.Name} and not {ctv.ExpectedNetType.Name} in the reader");
                }
            }

            if (value != null)
            {
                if (!value.GetType().BaseType.Name.Contains("PrimitiveArray"))
                {
                    Assert.True(ctv.ExpectedNetType == value.GetType(), $"Expected type does not match actual type for {ctv.Name}");

                    if (value is byte[] actualBytes)
                    {
                        byte[] expectedBytes = (byte[])ctv.ExpectedValue;

                        Assert.True(actualBytes.SequenceEqual(expectedBytes), $"byte[] values do not match expected values for {ctv.Name}");
                    }
                    else
                    {
                        Assert.True(ctv.ExpectedValue.Equals(value), $"Expected value [{ctv.ExpectedValue}] does not match actual value [{value}] for {ctv.Name}");
                    }
                }
                else
                {
                    IEnumerable list = value.GetType().GetMethod("ToList").Invoke(value, new object[] { false }) as IEnumerable;

                    IEnumerable expectedList = ctv.ExpectedValue.GetType().GetMethod("ToList").Invoke(ctv.ExpectedValue, new object[] { false }) as IEnumerable;

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
                                Assert.True(expected.Equals(actual), $"Expected value does not match actual value for {ctv.Name} at {i}");
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
    }
}

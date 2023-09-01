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
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    [TestClass]
    public class TypeTests
    {
        /// <summary>
        /// Verify the types and values for the arrays
        /// </summary>
        [TestMethod]
        public void VerifyTypesAndValues()
        {
            List<RecordBatch> recordBatches = Utils.LoadTestRecordBatches("resources/snowflake.arrow");

            RecordBatch recordBatch = recordBatches[0];

            Assert.AreEqual(1, recordBatches.Count);
            Assert.AreEqual(1, recordBatch.Length);

            var actualArrays = recordBatch.Arrays.ToList();

            List<ColumnNetTypeArrowTypeValue> expectedTypesAndValues = SampleData.GetSampleData();

            List<Type> expectedArrayTypes = new List<Type>();
            List<object> expectedValues = new List<object>();
            List<object> actualValues = new List<object>()
            {
                ((Int64Array)actualArrays[0]).GetValue(0),

                ((DoubleArray)actualArrays[1]).GetValue(0),
                ((DoubleArray)actualArrays[2]).GetValue(0),

                ((Int64Array)actualArrays[3]).GetValue(0),
                ((Int64Array)actualArrays[4]).GetValue(0),
                ((Int64Array)actualArrays[5]).GetValue(0),
                ((Int64Array)actualArrays[6]).GetValue(0),
                ((Int64Array)actualArrays[7]).GetValue(0),
                ((Int64Array)actualArrays[8]).GetValue(0),

                ((DoubleArray)actualArrays[9]).GetValue(0),
                ((DoubleArray)actualArrays[10]).GetValue(0),
                ((DoubleArray)actualArrays[11]).GetValue(0),
                ((DoubleArray)actualArrays[12]).GetValue(0),
                ((DoubleArray)actualArrays[13]).GetValue(0),
                ((DoubleArray)actualArrays[14]).GetValue(0),

                ((StringArray)actualArrays[15]).GetString(0),
                ((StringArray)actualArrays[16]).GetString(0),
                ((StringArray)actualArrays[17]).GetString(0),
                ((StringArray)actualArrays[18]).GetString(0),
                ((StringArray)actualArrays[19]).GetString(0),

                ((BinaryArray)actualArrays[20]).GetBytes(0).ToArray(),
                ((BinaryArray)actualArrays[21]).GetBytes(0).ToArray(),

                ((BooleanArray)actualArrays[22]).GetValue(0),

                ((Date32Array)actualArrays[23]).GetDateTime(0),

                ((TimestampArray)actualArrays[24]).GetTimestamp(0),
                ((TimestampArray)actualArrays[25]).GetTimestamp(0),
                ((TimestampArray)actualArrays[26]).GetTimestamp(0),
                ((TimestampArray)actualArrays[27]).GetTimestamp(0),
                ((TimestampArray)actualArrays[28]).GetTimestamp(0),

                ((Time64Array)actualArrays[29]).GetValue(0)
            };

            foreach(ColumnNetTypeArrowTypeValue ctv in expectedTypesAndValues)
            {
                expectedArrayTypes.Add(ctv.ExpectedArrowArrayType);
                expectedValues.Add(ctv.ExpectedValue);
            }

            Adbc.Tests.TypeTests.VerifyTypesAndValues(actualArrays, expectedArrayTypes, actualValues, expectedValues);
        }
    }
}

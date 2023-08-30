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

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
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
            List<RecordBatch> recordBatches = Utils.LoadTestRecordBatches("snowflake.arrow");

            RecordBatch recordBatch = recordBatches[0];

            Assert.AreEqual(1, recordBatches.Count);
            Assert.AreEqual(993, recordBatch.Length);

            var actualArrays = recordBatch.Arrays.ToList();

            List<Type> expectedArrayTypes = new List<Type>()
            {
                typeof(Int64Array),
                typeof(Int64Array),
                typeof(Int64Array),
                typeof(Int64Array),
                typeof(Int64Array),

                typeof(DoubleArray),
                typeof(DoubleArray),
                typeof(DoubleArray),

                typeof(StringArray),
                typeof(StringArray),

                typeof(TimestampArray),
                typeof(TimestampArray),
                typeof(TimestampArray),

                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray),
            };

            List<object> actualValues = new List<object>()
            {
                ((Int64Array)actualArrays[0]).GetValue(0),
                ((Int64Array)actualArrays[1]).GetValue(0),
                ((Int64Array)actualArrays[2]).GetValue(0),
                ((Int64Array)actualArrays[3]).GetValue(0),
                ((Int64Array)actualArrays[4]).GetValue(0),

                ((DoubleArray)actualArrays[5]).GetValue(0),
                ((DoubleArray)actualArrays[6]).GetValue(0),
                ((DoubleArray)actualArrays[7]).GetValue(0),

                ((StringArray)actualArrays[8]).GetString(0),
                ((StringArray)actualArrays[9]).GetString(0),

                ((TimestampArray)actualArrays[10]).GetTimestamp(0),
                ((TimestampArray)actualArrays[11]).GetTimestamp(0),
                ((TimestampArray)actualArrays[12]).GetTimestamp(0),

                ((StringArray)actualArrays[13]).GetString(0),
                ((StringArray)actualArrays[14]).GetString(0),
                ((StringArray)actualArrays[15]).GetString(0),
                ((StringArray)actualArrays[16]).GetString(0)

            };

            List<object> expectedValues = new List<object>()
            {
                1867361L,
                167019L,
                9536L,
                6L,
                40L,

                43440.4d,
                0d,
                0.06d,

                "A",
                "F",

                new DateTimeOffset(628845984000000000, TimeSpan.Zero),
                new DateTimeOffset(628819200000000000, TimeSpan.Zero),
                new DateTimeOffset(628847712000000000, TimeSpan.Zero),

                "TAKE BACK RETURN",
                "TRUCK",
                ". furiously bold depende",
                null

            };

            Adbc.Tests.TypeTests.VerifyTypesAndValues(actualArrays, expectedArrayTypes, actualValues, expectedValues);
        }
    }
}

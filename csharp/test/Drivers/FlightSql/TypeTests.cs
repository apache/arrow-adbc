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

namespace Apache.Arrow.Adbc.Tests.Drivers.FlightSql
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
            List<RecordBatch> recordBatches = Utils.LoadTestRecordBatches("resources/flightsql.arrow");

            RecordBatch recordBatch = recordBatches[0];

            Assert.AreEqual(1, recordBatches.Count);
            Assert.AreEqual(50, recordBatch.Length);

            var actualArrays = recordBatch.Arrays.ToList();

            List<Type> expectedArrayTypes = new List<Type>()
            {
                typeof(TimestampArray),
                typeof(Int64Array),
                typeof(DoubleArray),
                typeof(DoubleArray),
                typeof(DoubleArray),
                typeof(DoubleArray)
            };

            List<object> actualValues = new List<object>()
            {
                ((TimestampArray)actualArrays[0]).GetValue(0),
                ((Int64Array)actualArrays[1]).GetValue(0),
                ((DoubleArray)actualArrays[2]).GetValue(0),
                ((DoubleArray)actualArrays[3]).GetValue(0),
                ((DoubleArray)actualArrays[4]).GetValue(0),
                ((DoubleArray)actualArrays[5]).GetValue(0),
            };

            List<object> expectedValues = new List<object>()
            {
                1369682100000L,
                1L,
                1.26d,
                7.5d,
                0d,
                8d
            };

            Adbc.Tests.TypeTests.VerifyTypesAndValues(actualArrays, expectedArrayTypes, actualValues, expectedValues);
        }
    }
}

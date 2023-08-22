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

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache
{
    [TestClass]
    public class SparkTypeTests
    {
        /// <summary>
        /// Verify the types and values for the arrays
        /// </summary>
        [TestMethod]
        public void VerifyTypesAndValues()
        {
            List<RecordBatch> recordBatches = Utils.LoadTestRecordBatches("spark.arrow");

            RecordBatch recordBatch = recordBatches[0];

            int totalRecords = recordBatches.Select(x => x.Length).Sum();

            Assert.AreEqual(19, recordBatches.Count);
            Assert.AreEqual(46236, totalRecords);

            var actualArrays = recordBatch.Arrays.ToList();

            List<Type> expectedArrayTypes = new List<Type>()
            {
                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray),
                typeof(DoubleArray),
                typeof(DoubleArray),
                typeof(DoubleArray),
                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray),
                typeof(StringArray)
            };

            List<object> actualValues = new List<object>()
            {
                ((StringArray)actualArrays[0]).GetString(0),
                ((StringArray)actualArrays[1]).GetString(0),
                ((StringArray)actualArrays[2]).GetString(0),
                ((DoubleArray)actualArrays[3]).GetValue(0),
                ((DoubleArray)actualArrays[4]).GetValue(0),
                ((DoubleArray)actualArrays[5]).GetValue(0),
                ((StringArray)actualArrays[6]).GetString(0),
                ((StringArray)actualArrays[7]).GetString(0),
                ((StringArray)actualArrays[8]).GetString(0),
                ((StringArray)actualArrays[9]).GetString(0),
                ((StringArray)actualArrays[10]).GetString(0),
                ((StringArray)actualArrays[11]).GetString(0),
                ((StringArray)actualArrays[12]).GetString(0),
            };

            List<object> expectedValues = new List<object>()
            {
                "CA-0020",
                "closed",
                "Algar Tower Airport",
                56.11666489,
                -111.7666702,
                null,
                "NA",
                "CA",
                "CA-AB",
                null,
                null,
                null,
                null
            };

            Adbc.Tests.TypeTests.VerifyTypesAndValues(actualArrays, expectedArrayTypes, actualValues, expectedValues);
        }
    }
}

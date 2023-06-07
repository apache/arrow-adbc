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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Performs verification for data types and values.
    /// </summary>
    public class TypeTests
    {
        /// <summary>
        /// Verifies that the arrays passed as actual match the expected and
        /// that the values match actual and expected.
        /// </summary>
        /// <param name="actualArrays">
        /// The actual arrays
        /// </param>
        /// <param name="expectedArrays">
        /// The expected array types
        /// </param>
        /// <param name="actualFirstValues">
        /// The actual values
        /// </param>
        /// <param name="expectedFirstValues">
        /// The expected values
        /// </param>
        public static void VerifyTypesAndValues(List<IArrowArray> actualArrays, List<Type> expectedArrays, List<object> actualFirstValues, List<object> expectedFirstValues)
        {
            Assert.IsTrue(actualArrays.Count == expectedArrays.Count, "The actual and expected array lengths must be the same length");

            Assert.IsTrue(actualArrays.Count == actualFirstValues.Count, "actualArrays and actualFirstValues must be the same length");

            Assert.IsTrue(expectedArrays.Count == expectedFirstValues.Count, "expectedArrays and expectedFirstValues must be the same length");

            for (int i = 0; i < actualArrays.Count; i++)
            {
                IArrowArray actualArray = actualArrays[i];
                Type expectedArrayType = expectedArrays[i];

                Assert.IsTrue(actualArray.GetType() == expectedArrayType, $"{actualArray.GetType()} != {expectedArrayType} at position {i}");

                object actualValue = actualFirstValues[i];
                object expectedValue = expectedFirstValues[i];

                Assert.IsTrue(actualValue.Equals(expectedValue), $"{actualValue} != {expectedValue} at position {i}");
            }
        }
    }
}

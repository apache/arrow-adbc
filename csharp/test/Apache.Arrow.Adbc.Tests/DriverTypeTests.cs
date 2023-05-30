
using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Abstract class for the ADBC connection tests.
    /// </summary>
    public class AdbcDriverTypeTests
    {
        /// <summary>
        /// Verifies that the arrays passed as actual match the expected and 
        /// that the values match actual and expected.
        /// </summary>
        /// <param name="actualArrays">The actual arrays</param>
        /// <param name="expectedArrays">The expected array types</param>
        /// <param name="actualFirstValues">The actual values</param>
        /// <param name="expectedFirstValues">The expected values</param>
        public static void VerifyTypesAndValues(List<IArrowArray> actualArrays, List<Type> expectedArrays, List<object> actualFirstValues, List<object> expectedFirstValues)
        {
            Assert.IsTrue(actualArrays.Count == expectedArrays.Count, "The actual and expected array lengths must be the same length");

            Assert.IsTrue(actualArrays.Count == actualFirstValues.Count, "actualArrays and actualFirstValues must be the same length");

            Assert.IsTrue(expectedArrays.Count == expectedFirstValues.Count, "expectedArrays and expectedFirstValues must be the same length");

            for(int i =0; i < actualArrays.Count; i++) 
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

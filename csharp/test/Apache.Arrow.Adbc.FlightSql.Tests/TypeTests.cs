using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Apache.Arrow.Adbc.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.FlightSql.Tests
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
            List<RecordBatch> recordBatches = Utils.LoadTestRecordBatches();

            RecordBatch recordBatch = recordBatches[0];

            Assert.AreEqual(1, recordBatches.Count);
            Assert.AreEqual(50, recordBatch.Length);

            var actualArrays =  recordBatch.Arrays.ToList();

            List<Type> expectedArrayTypes = new List<Type>()
            {
                typeof(Int64Array),
                typeof(Int64Array),
                typeof(DoubleArray),
                typeof(DoubleArray),
                typeof(DoubleArray),
                typeof(DoubleArray)
            };

            List<object> actualValues = new List<object>()
            {
                ((Int64Array)actualArrays[0]).GetValue(0),
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests
{
    public class ConnectionTests
    {
        public static void CanDriverConnect(QueryResult queryResult, long expectedNumberOfResults)
        {
            long count = 0;

            while (true)
            {
                var nextBatch = queryResult.Stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                count += nextBatch.Length;
            }

            Assert.AreEqual(expectedNumberOfResults, count);
        }

        public static void VerifyBadQueryGeneratesError(Exception ex)
        {
            Assert.IsTrue(ex is AdbcException, "Can only validate AdbcException types");
        }
    }
}

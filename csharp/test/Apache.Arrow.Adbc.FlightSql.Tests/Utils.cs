using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.FlightSql.Tests
{
    internal class Utils
    {
        public static List<RecordBatch> LoadTestRecordBatches()
        {
            // this file was generated from the Flight SQL data source
            string file = "flightsql.parquet";

            Assert.IsTrue(File.Exists(file), $"Cannot find {file}");

            List<RecordBatch> recordBatches = ParquetParser.ParseParquetFile(file);

            return recordBatches;
        }
    }
}

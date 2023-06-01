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

using System.Collections.Generic;
using System.IO;
using Apache.Arrow.Ipc;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.FlightSql.Tests
{
    internal class Utils
    {
        /// <summary>
        /// Loads record batches from an arrow file.
        /// </summary>
        /// <returns></returns>
        public static List<RecordBatch> LoadTestRecordBatches()
        {
            // this file was generated from the Flight SQL data source
            string file = "flightsql.arrow";

            Assert.IsTrue(File.Exists(file), $"Cannot find {file}");

            List<RecordBatch> recordBatches = new List<RecordBatch>();

            using (FileStream fs = new FileStream(file, FileMode.Open))
            using (ArrowFileReader reader = new ArrowFileReader(fs))
            {
                int batches = reader.RecordBatchCountAsync().Result;

                for(int i = 0; i < batches; i++) 
                {
                    recordBatches.Add(reader.ReadNextRecordBatch());
                }
            }

            return recordBatches;
        }
    }
}

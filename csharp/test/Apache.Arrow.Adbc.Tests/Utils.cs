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
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text.Json;
using Apache.Arrow.Ipc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Apache.Arrow.Adbc.Tests
{
    public class Utils
    {
        /// <summary>
        /// Writes record batches to an arrow file.
        /// </summary>
        /// <param name="file">The path of the arrow file.</param>
        /// <remarks>
        /// This method can be used during the generation of Arrow files for use in tests, but may
        /// not have references to it in the solution.
        /// </remarks>
        public static void WriteTestRecordBatches(List<RecordBatch> recordBatches, string file)
        {
            if(recordBatches == null || recordBatches.Count == 0)
            {
                throw new ArgumentException("recordBatches must have at least one batch");
            }

            Schema schema = recordBatches[0].Schema;

            Assert.IsFalse(File.Exists(file), $"Cannot overwrite {file}");

            using (FileStream fs = new FileStream(file, FileMode.CreateNew))
            using (ArrowFileWriter writer = new ArrowFileWriter(fs, schema))
            {
                writer.WriteStart();

                foreach (RecordBatch batch in recordBatches)
                { 
                    writer.WriteRecordBatch(batch);
                }

                writer.WriteEnd();
            }
        }

        /// <summary>
        /// Loads record batches from an arrow file.
        /// </summary>
        /// <param name="file">The path of the arrow file.</param>
        public static List<RecordBatch> LoadTestRecordBatches(string file)
        {
            Assert.IsTrue(File.Exists(file), $"Cannot find {file}");

            List<RecordBatch> recordBatches = new List<RecordBatch>();

            using (FileStream fs = new FileStream(file, FileMode.Open))
            using (ArrowFileReader reader = new ArrowFileReader(fs))
            {
                int batches = reader.RecordBatchCountAsync().Result;

                for (int i = 0; i < batches; i++)
                {
                    recordBatches.Add(reader.ReadNextRecordBatch());
                }
            }

            return recordBatches;
        }

        /// <summary>
        /// Loads a Statement with mocked results.
        /// </summary>
        public static Mock<IAdbcStatement> GetMockStatement(string file, int expectedResults)
        {
            List<RecordBatch> recordBatches = LoadTestRecordBatches(file);

            Schema s = recordBatches.First().Schema;
            QueryResult mockQueryResult = new QueryResult(expectedResults, new MockArrayStream(s, recordBatches));
            
            Mock<IAdbcStatement> mockSqlStatement = new Mock<IAdbcStatement>();
            mockSqlStatement.Setup(s => s.ExecuteQuery()).Returns(mockQueryResult);

            return mockSqlStatement;
        }

        /// <summary>
        /// Loads a test configuration
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fileName">The path of the configuration file</param>
        /// <returns>T</returns>
        public static T GetTestConfiguration<T>(string fileName)
            where T : TestConfiguration
        {
            // use a JSON file vs. setting up environment variables
            string json = File.ReadAllText(fileName);

            T testConfiguration = JsonSerializer.Deserialize<T>(json);

            return testConfiguration;
        }
    }
}

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
using Apache.Arrow.Adbc.Tests.Metadata;
using Apache.Arrow.Ipc;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Class for testing the metadata capability of the Snowflake ADBC driver.
    /// </summary>
    [TestClass]
    public class MetadataTests
    {
        /// <summary>
        /// Validates if the driver can call GetInfo.
        /// </summary>
        [TestMethod]
        public void CanGetInfo()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration metadataTestConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeconfig.json");

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(metadataTestConfiguration, out parameters);

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetInfo(new List<AdbcInfoCode>() { AdbcInfoCode.DriverName, AdbcInfoCode.DriverVersion, AdbcInfoCode.VendorName });

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;
            UInt32Array infoNameArray = (UInt32Array)recordBatch.Column("info_name");

            List<string> expectedValues = new List<string>() { "DriverName", "DriverVersion", "VendorName" };

            for (int i = 0; i < infoNameArray.Length; i++)
            {
                AdbcInfoCode value = (AdbcInfoCode)infoNameArray.GetValue(i);
                DenseUnionArray valueArray = (DenseUnionArray)recordBatch.Column("info_value");

                Assert.IsTrue(expectedValues.Contains(value.ToString()));

                StringArray stringArray = (StringArray)valueArray.Fields[0];
                Console.WriteLine($"{value}={stringArray.GetString(i)}");
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects.
        /// </summary>
        [TestMethod]
        public void CanGetObjects()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration metadataTestConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeconfig.json");

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(metadataTestConfiguration, out parameters);

            // need to add the database
            string databaseName = metadataTestConfiguration.Metadata.Database;
            string schemaName = metadataTestConfiguration.Metadata.Schema;
            string tableName = metadataTestConfiguration.Metadata.Table;
            string columnName = null;

            parameters["adbc.snowflake.sql.db"] = databaseName;
            parameters["adbc.snowflake.sql.schema"] = schemaName;

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: databaseName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: columnName);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, databaseName, schemaName);

            List<AdbcColumn> columns = catalogs
                .Select(s => s.DbSchemas)
                .FirstOrDefault()
                .Select(t => t.Tables)
                .FirstOrDefault()
                .Select(c => c.Columns)
                .FirstOrDefault();

            Assert.AreEqual(metadataTestConfiguration.Metadata.ExpectedColumnCount, columns.Count);
        }

        /// <summary>
        /// Validates if the driver can call GetTableSchema.
        /// </summary>
        [TestMethod]
        public void CanGetTableSchema()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration metadataTestConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeconfig.json");

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(metadataTestConfiguration, out parameters);

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            string databaseName = metadataTestConfiguration.Metadata.Database;
            string schemaName = metadataTestConfiguration.Metadata.Schema;
            string tableName = metadataTestConfiguration.Metadata.Table;

            Schema schema = adbcConnection.GetTableSchema(databaseName, schemaName, tableName);

            int numberOfFields = schema.FieldsList.Count;

            Assert.AreEqual(metadataTestConfiguration.Metadata.ExpectedColumnCount, numberOfFields);
        }

        /// <summary>
        /// Validates if the driver can call GetTableTypes.
        /// </summary>
        [TestMethod]
        public void CanGetTableTypes()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            SnowflakeTestConfiguration metadataTestConfiguration = Utils.GetTestConfiguration<SnowflakeTestConfiguration>("resources/snowflakeconfig.json");

            AdbcDriver driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(metadataTestConfiguration, out parameters);

            AdbcDatabase adbcDatabase = driver.Open(parameters);
            AdbcConnection adbcConnection = adbcDatabase.Connect(new Dictionary<string, string>());

            IArrowArrayStream arrowArrayStream = adbcConnection.GetTableTypes();

            RecordBatch recordBatch = arrowArrayStream.ReadNextRecordBatchAsync().Result;

            StringArray stringArray = (StringArray)recordBatch.Column("table_type");

            List<string> known_types = new List<string>
            {
                "BASE TABLE", "TEMPORARY TABLE", "VIEW"
            };

            int results = 0;

            for (int i = 0; i < stringArray.Length; i++)
            {
                string value = stringArray.GetString(i);

                if (known_types.Contains(value))
                {
                    results++;
                }
            }

            Assert.AreEqual(known_types.Count, results);
        }
    }
}

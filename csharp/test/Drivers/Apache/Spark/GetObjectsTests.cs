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
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Class for testing the Snowflake ADBC driver connection tests.
    /// </summary>
    /// <remarks>
    /// Tests are ordered to ensure data is created for the other
    /// queries to run.
    /// </remarks>
    [TestCaseOrderer("Apache.Arrow.Adbc.Tests.Xunit.TestOrderer", "Apache.Arrow.Adbc.Tests")]
    public class GetObjectsTests : SparkTestBase
    {
        private bool _disposed = false;
        private const string _catalogName = "hive_metastore";
        private readonly string _schemaPrefix = null;
        private readonly List<string> _schemaNames = new List<string>();
        private const int _schemaCount = 3;

        public GetObjectsTests(ITestOutputHelper outputHelper) : base(outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
            _schemaPrefix = Guid.NewGuid().ToString().Replace("-", "");
            for (int i = 0; i < _schemaCount; i++)
            {
                string schemaName = _schemaPrefix + "_" + Guid.NewGuid().ToString().Replace("-", "");
                Statement.SqlQuery = $"CREATE SCHEMA IF NOT EXISTS {_catalogName}.{schemaName}";
                Statement.ExecuteUpdate();
                _schemaNames.Add(schemaName);
            }
        }

        /// <summary>
        /// Validates if the driver can call GetObjects and return a specific schema.
        /// </summary>
        [SkippableFact]
        public void CanGetObjectsSingleSchema()
        {
            // need to add the database
            string catalogName = _catalogName;
            string schemaName = _schemaNames[0];
            string tableName = null;
            string columnName = null;

            AdbcConnection adbcConnection = NewConnection();

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: catalogName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: columnName);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, catalogName, schemaName);
            Assert.Single(catalogs);
            Assert.Equal(_catalogName, catalogs[0].Name);
            Assert.NotNull(catalogs[0].DbSchemas);
            var dbSchemas = catalogs[0].DbSchemas;
            Assert.Single(dbSchemas);
            Assert.Equal(_schemaNames[0], dbSchemas[0].Name);
        }

        /// <summary>
        /// Validates if the driver can call GetObjects and return schema using a prefix pattern.
        /// </summary>
        [SkippableFact]
        public void CanGetObjectsMultipleSchema()
        {
            // need to add the database
            string catalogName = _catalogName;
            string schemaName = _schemaPrefix + "%";
            string tableName = null;
            string columnName = null;

            AdbcConnection adbcConnection = NewConnection();

            IArrowArrayStream stream = adbcConnection.GetObjects(
                    depth: AdbcConnection.GetObjectsDepth.All,
                    catalogPattern: catalogName,
                    dbSchemaPattern: schemaName,
                    tableNamePattern: tableName,
                    tableTypes: new List<string> { "BASE TABLE", "VIEW" },
                    columnNamePattern: columnName);

            RecordBatch recordBatch = stream.ReadNextRecordBatchAsync().Result;

            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(recordBatch, catalogName, schemaName);
            Assert.Single(catalogs);
            Assert.Equal(_catalogName, catalogs[0].Name);
            Assert.NotNull(catalogs[0].DbSchemas);
            var dbSchemas = catalogs[0].DbSchemas;
            Assert.Equal(_schemaCount, dbSchemas.Count);
            for (int i = 0; i < dbSchemas.Count; i++)
            {
                Assert.True(dbSchemas.Exists(s => s.Name == _schemaNames[i]));
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (!_disposed)
                {
                    for (int i = 0; i < _schemaCount; i++)
                    {
                        Statement.SqlQuery = $"DROP SCHEMA IF EXISTS {_catalogName}.{_schemaNames[i]}";
                        Statement.ExecuteUpdate();
                    }
                    _disposed = true;
                }
            }
            base.Dispose(disposing);
        }
    }
}

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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class DriverTests : Common.DriverTests<SparkTestConfiguration, SparkTestEnvironment>
    {
        public DriverTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new SparkTestEnvironment.Factory())
        {
        }

        [SkippableTheory]
        [MemberData(nameof(CatalogNamePatternData))]
        public override void CanGetObjectsCatalogs(string? pattern)
        {
            GetObjectsCatalogsTest(pattern);
        }

        [SkippableTheory]
        [MemberData(nameof(DbSchemasNamePatternData))]
        public override void CanGetObjectsDbSchemas(string dbSchemaPattern)
        {
            GetObjectsDbSchemasTest(dbSchemaPattern);
        }

        [SkippableTheory]
        [MemberData(nameof(TableNamePatternData))]
        public override void CanGetObjectsTables(string tableNamePattern)
        {
            GetObjectsTablesTest(tableNamePattern);
        }

        protected override IReadOnlyList<int> GetUpdateExpectedResults()
        {
            int affectedRows = ValidateAffectedRows ? 1 : -1;
            return ClientTests.GetUpdateExpecteResults(affectedRows, TestEnvironment.ServerType == SparkServerType.Databricks);
        }


        public static IEnumerable<object[]> CatalogNamePatternData()
        {
            string? catalogName = new DriverTests(null).TestConfiguration?.Metadata?.Catalog;
            return GetPatterns(catalogName);
        }

        public static IEnumerable<object[]> DbSchemasNamePatternData()
        {
            string? dbSchemaName = new DriverTests(null).TestConfiguration?.Metadata?.Schema;
            return GetPatterns(dbSchemaName);
        }

        public static IEnumerable<object[]> TableNamePatternData()
        {
            string? tableName = new DriverTests(null).TestConfiguration?.Metadata?.Table;
            return GetPatterns(tableName);
        }
    }
}

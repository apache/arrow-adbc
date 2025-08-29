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
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Impala
{
    public class StatementTests : Common.StatementTests<ApacheTestConfiguration, ImpalaTestEnvironment>
    {
        private const string LongRunningQuery = "SELECT COUNT(*) AS total_count\nFROM (\n  SELECT t1.id AS id1, t2.id AS id2\n  FROM RANGE(1000000) t1\n  CROSS JOIN RANGE(100000) t2\n) subquery\nWHERE MOD(id1 + id2, 2) = 0";

        public StatementTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new ImpalaTestEnvironment.Factory())
        {
        }

        [SkippableFact]
        public async Task CanGetPrimaryKeysImpala()
        {
            await base.CanGetPrimaryKeys(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceParentTableImpala()
        {
            await base.CanGetCrossReferenceFromParentTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceChildTableImpala()
        {
            await base.CanGetCrossReferenceFromChildTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableTheory(Skip = "Untested")]
        [InlineData(LongRunningQuery)]
        internal override async Task CanCancelStatementTest(string query)
        {
            await base.CanCancelStatementTest(query);
        }

        protected override void PrepareCreateTableWithForeignKeys(string fullTableNameParent, out string sqlUpdate, out string tableNameChild, out string fullTableNameChild, out IReadOnlyList<string> foreignKeys)
        {
            CreateNewTableName(out tableNameChild, out fullTableNameChild);
            sqlUpdate = $"CREATE TABLE IF NOT EXISTS {fullTableNameChild} \n"
                + "  (INDEX INT, USERINDEX INT, USERNAME STRING, ADDRESS STRING, \n"
                + "  PRIMARY KEY (INDEX), \n"
                + $"  FOREIGN KEY (USERINDEX, USERNAME) REFERENCES {fullTableNameParent} (INDEX, NAME))";
            foreignKeys = ["userindex", "username"];
        }

        protected override void PrepareCreateTableWithPrimaryKeys(out string sqlUpdate, out string tableNameParent, out string fullTableNameParent, out IReadOnlyList<string> primaryKeys)
        {
            CreateNewTableName(out tableNameParent, out fullTableNameParent);
            sqlUpdate = $"CREATE TABLE IF NOT EXISTS {fullTableNameParent} (INDEX INT, NAME STRING, PRIMARY KEY (INDEX, NAME))";
            primaryKeys = ["index", "name"];
        }
    }
}

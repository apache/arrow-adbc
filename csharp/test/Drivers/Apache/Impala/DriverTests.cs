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
using Apache.Arrow.Adbc.Drivers.Apache.Impala;
using Apache.Arrow.Adbc.Tests.Metadata;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Impala
{
    public class DriverTests : Common.DriverTests<ApacheTestConfiguration, ImpalaTestEnvironment>
    {
        public DriverTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new ImpalaTestEnvironment.Factory())
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

        public override void CanDetectInvalidServer()
        {
            AdbcDriver driver = NewDriver;
            Assert.NotNull(driver);
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);

            bool hasUri = parameters.TryGetValue(AdbcOptions.Uri, out var uri) && !string.IsNullOrEmpty(uri);
            bool hasHostName = parameters.TryGetValue(ImpalaParameters.HostName, out var hostName) && !string.IsNullOrEmpty(hostName);
            if (hasUri)
            {
                parameters[AdbcOptions.Uri] = "http://unknownhost.azure.com/cliservice";
            }
            else if (hasHostName)
            {
                parameters[ImpalaParameters.HostName] = "unknownhost.azure.com";
            }
            else
            {
                Assert.Fail($"Unexpected configuration. Must provide '{AdbcOptions.Uri}' or '{ImpalaParameters.HostName}'.");
            }

            AdbcDatabase database = driver.Open(parameters);
            AggregateException exception = Assert.ThrowsAny<AggregateException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine(exception.Message);
        }

        public override void CanDetectInvalidAuthentication()
        {
            AdbcDriver driver = NewDriver;
            Assert.NotNull(driver);
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);

            bool hasUsername = parameters.TryGetValue(AdbcOptions.Username, out var username) && !string.IsNullOrEmpty(username);
            bool hasPassword = parameters.TryGetValue(AdbcOptions.Password, out var password) && !string.IsNullOrEmpty(password);
            if (hasUsername && hasPassword)
            {
                parameters[AdbcOptions.Password] = "invalid-password";
            }
            else
            {
                Assert.Fail($"Unexpected configuration. Must provide '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.");
            }

            AdbcDatabase database = driver.Open(parameters);
            AggregateException exception = Assert.ThrowsAny<AggregateException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine(exception.Message);
        }

        protected override IReadOnlyList<int> GetUpdateExpectedResults()
        {
            int affectedRows = ValidateAffectedRows ? 1 : -1;
            return ClientTests.GetUpdateExpectedResults(affectedRows);
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

        protected override bool TypeHasColumnSize(AdbcColumn column) => true;

        protected override bool TypeHasDecimalDigits(AdbcColumn column) => true;
    }
}

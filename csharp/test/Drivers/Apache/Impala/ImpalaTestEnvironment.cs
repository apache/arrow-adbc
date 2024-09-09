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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Impala;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Impala
{
    public class ImpalaTestEnvironment : TestEnvironment<ApacheTestConfiguration>
    {
        public class Factory : Factory<ImpalaTestEnvironment>
        {
            public override ImpalaTestEnvironment Create(Func<AdbcConnection> getConnection) => new(getConnection);
        }

        private ImpalaTestEnvironment(Func<AdbcConnection> getConnection) : base(getConnection) { }

        public override string TestConfigVariable => "IMPALA_TEST_CONFIG_FILE";

        public override string SqlDataResourceLocation => "Impala/Resources/ImpalaData.sql";

        public override int ExpectedColumnCount => 17;

        public override AdbcDriver CreateNewDriver() => new ImpalaDriver();

        public override string GetCreateTemporaryTableStatement(string tableName, string columns)
        {
            return string.Format("CREATE TABLE {0} ({1})", tableName, columns);
        }

        public override string Delimiter => "`";

        public override Dictionary<string, string> GetDriverParameters(ApacheTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrEmpty(testConfiguration.HostName))
            {
                parameters.Add("HostName", testConfiguration.HostName!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Port))
            {
                parameters.Add("Port", testConfiguration.Port!);
            }
            return parameters;
        }

        public override string VendorVersion => ((HiveServer2Connection)Connection).VendorVersion;

        public override bool SupportsDelete => false;

        public override bool SupportsUpdate => false;

        public override bool SupportCatalogName => false;

        public override bool ValidateAffectedRows => false;

        public override string GetInsertStatement(string tableName, string columnName, string? value) =>
            string.Format("INSERT INTO {0} ({1}) SELECT {2};", tableName, columnName, value ?? "NULL");
    }
}

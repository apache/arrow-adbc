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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class SparkTestBase : TestBase<SparkTestConfiguration>
    {
        public SparkTestBase(ITestOutputHelper outputHelper) : base(outputHelper) { }

        protected override string TestConfigVariable => "SPARK_TEST_CONFIG_FILE";

        protected override string SqlDataResourceLocation => "Spark/Resources/SparkData.sql";

        protected override AdbcDriver NewDriver => new SparkDriver();

        protected override TemporaryTable NewTemporaryTable(AdbcStatement statement, string columns) {
            string tableName = NewTableName();
            // Note: Databricks/Spark doesn't support TEMPORARY table.
            string sqlUpdate = string.Format("CREATE TABLE {0} ({1})", tableName, columns);
            OutputHelper.WriteLine(sqlUpdate);
            return TemporaryTable.NewTemporaryTable(statement, tableName, sqlUpdate);
        }

        protected override Dictionary<string, string> GetDriverParameters(SparkTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new(StringComparer.OrdinalIgnoreCase);

            // TODO: make these parameters that are passed in
            if (!string.IsNullOrEmpty(testConfiguration.HostName))
            {
                parameters.Add("HostName", testConfiguration.HostName!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Path))
            {
                parameters.Add("Path", testConfiguration.Path!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Token))
            {
                parameters.Add("Token", testConfiguration.Token!);
            }

            return parameters;
        }
    }
}

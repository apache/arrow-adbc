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
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class ClientTests : Common.ClientTests<SparkTestConfiguration, SparkTestEnvironment>
    {
        public ClientTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new SparkTestEnvironment.Factory())
        {
        }

        protected override IReadOnlyList<int> GetUpdateExpectedResults()
        {
            int affectedRows = ValidateAffectedRows ? 1 : -1;
            return GetUpdateExpecteResults(affectedRows, TestEnvironment.ServerType == SparkServerType.Databricks);
        }

        internal static IReadOnlyList<int> GetUpdateExpecteResults(int affectedRows, bool isDatabricks)
        {
            return !isDatabricks
                ? [
                    -1, // CREATE TABLE
                    affectedRows,  // INSERT
                    affectedRows,  // INSERT
                    affectedRows,  // INSERT
                  ]
                : [
                    -1, // CREATE TABLE
                    affectedRows,  // INSERT
                    affectedRows,  // INSERT
                    affectedRows,  // INSERT
                    affectedRows,  // UPDATE
                    affectedRows,  // DELETE
                  ];
        }
    }
}

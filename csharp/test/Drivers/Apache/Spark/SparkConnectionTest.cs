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
using System.Globalization;
using System.Net;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Class for testing the Spark ADBC connection tests.
    /// </summary>
    public class SparkConnectionTest : SparkTestBase
    {
        public SparkConnectionTest(ITestOutputHelper? outputHelper) : base(outputHelper)
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Validates database can detect invalid connection parameter combinations.
        /// </summary>
        [SkippableTheory]
        [ClassData(typeof(InvalidConnectionParametersTestData))]
        internal void CanDetectConnectionParameterErrors(IReadOnlyDictionary<string, string> parameters)
        {
            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(parameters);
            Exception exeption = Assert.ThrowsAny<ArgumentException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine(exeption.Message);
        }

        internal class InvalidConnectionParametersTestData : TheoryData<Dictionary<string, string>>
        {
            public InvalidConnectionParametersTestData()
            {
                Add([]);
                Add(new() { [SparkParameters.HostName] = " " });
                Add(new() { [SparkParameters.HostName] = "invalid!server.com" });
                Add(new() { [SparkParameters.HostName] = "http://valid.server.com" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.AuthTypeBasic}" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.AuthTypeToken}" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.AuthTypeBasic}", [SparkParameters.Token] = "abcdef" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.AuthTypeToken}", [SparkParameters.Username] = "user", [SparkParameters.Password] = "myPassword" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Username] = "user" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Password] = "myPassword" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = "-1" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = IPEndPoint.MinPort.ToString(CultureInfo.InvariantCulture) });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = (IPEndPoint.MaxPort + 1).ToString(CultureInfo.InvariantCulture) });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Scheme] = "http.xxz" });
                Add(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Scheme] = "httpxxz" });
            }
        }
    }
}

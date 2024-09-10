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
    public class SparkConnectionTest : TestBase<SparkTestConfiguration, SparkTestEnvironment>
    {
        public SparkConnectionTest(ITestOutputHelper? outputHelper) : base(outputHelper, new SparkTestEnvironment.Factory())
        {
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Validates database can detect invalid connection parameter combinations.
        /// </summary>
        [SkippableTheory]
        [ClassData(typeof(InvalidConnectionParametersTestData))]
        internal void CanDetectConnectionParameterErrors(ParametersWithExceptions test)
        {
            AdbcDriver driver = NewDriver;
            AdbcDatabase database = driver.Open(test.Parameters);
            Exception exeption = Assert.Throws(test.ExceptionType, () => database.Connect(test.Parameters));
            OutputHelper?.WriteLine(exeption.Message);
        }

        internal class ParametersWithExceptions
        {
            public ParametersWithExceptions(Dictionary<string, string> parameters, Type exceptionType)
            {
                Parameters = parameters;
                ExceptionType = exceptionType;
            }

            public IReadOnlyDictionary<string, string> Parameters { get; }
            public Type ExceptionType { get; }
        }

        internal class InvalidConnectionParametersTestData : TheoryData<ParametersWithExceptions>
        {
            public InvalidConnectionParametersTestData()
            {
                Add(new([], typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = " " }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = "xxx" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Standard }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.HDInsight }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = " " }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "invalid!server.com" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "http://valid.server.com" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"unknown_auth_type" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.Basic}" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.Token}" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.Basic}", [SparkParameters.Token] = "abcdef" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.AuthType] = $"{SparkAuthTypeConstants.Token}", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Password] = "myPassword" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Databricks, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = "-1" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Databricks, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = IPEndPoint.MinPort.ToString(CultureInfo.InvariantCulture) }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Databricks, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = (IPEndPoint.MaxPort + 1).ToString(CultureInfo.InvariantCulture) }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Databricks, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [AdbcOptions.Uri] = "httpxxz://hostname.com" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Databricks, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [AdbcOptions.Uri] = "http-//hostname.com" }, typeof(UriFormatException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Databricks, [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [AdbcOptions.Uri] = "httpxxz://hostname.com:1234567890" }, typeof(UriFormatException)));
            }
        }
    }
}

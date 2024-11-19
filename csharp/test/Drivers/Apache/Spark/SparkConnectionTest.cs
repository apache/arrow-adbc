﻿/*
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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Thrift.Protocol.Entities;
using Thrift.Transport;
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

        [SkippableTheory]
        [InlineData(-1, typeof(TimeoutException))]
        [InlineData(10, typeof(TimeoutException))]
        [InlineData(30000, null)]
        [InlineData(null, null)]
        [InlineData(-1, null)]
        public void ConnectionTimeoutTest(int? connectTimeoutMilliseconds, Type? exceptionType)
        {
            SparkTestConfiguration testConfiguration = (SparkTestConfiguration)TestConfiguration.Clone();

            if (connectTimeoutMilliseconds.HasValue)
                testConfiguration.ConnectTimeoutMilliseconds = connectTimeoutMilliseconds.Value.ToString();

            OutputHelper?.WriteLine($"ConnectTimeoutMilliseconds: {testConfiguration.ConnectTimeoutMilliseconds}. ShouldSucceed: {exceptionType == null}");

            try
            {
                NewConnection(testConfiguration);
            }
            catch(AggregateException aex)
            {
                if (exceptionType != null)
                {
                    Assert.IsType(exceptionType, aex.InnerException);
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Tests the various metadata calls on a SparkConnection
        /// </summary>
        /// <param name="metadataWithException"></param>
        [SkippableTheory]
        [ClassData(typeof(MetadataTimeoutTestData))]
        internal void MetadataTimeoutTest(MetadataWithExceptions metadataWithException)
        {
            SparkTestConfiguration testConfiguration = (SparkTestConfiguration)TestConfiguration.Clone();

            if (metadataWithException.QueryTimeoutSeconds.HasValue)
                testConfiguration.QueryTimeoutSeconds = metadataWithException.QueryTimeoutSeconds.Value.ToString();

            OutputHelper?.WriteLine($"Action: {metadataWithException.ActionName}. QueryTimeoutSeconds: {testConfiguration.QueryTimeoutSeconds}. ShouldSucceed: {metadataWithException.ExceptionType == null}");

            try
            {
                metadataWithException.MetadataAction(testConfiguration);
            }
            catch (AggregateException aex)
            {
                if (metadataWithException.ExceptionType != null)
                {
                    if (metadataWithException.AlternateExceptionType != null && aex.InnerException?.GetType() != metadataWithException.ExceptionType)
                    {
                        Assert.IsType(metadataWithException.AlternateExceptionType, aex.InnerException);
                    }
                    else
                    {
                        Assert.IsType(metadataWithException.ExceptionType, aex.InnerException);
                    }
                }
                else
                {
                    throw;
                }
            }
        }

        internal class MetadataWithExceptions
        {
            public MetadataWithExceptions(int? queryTimeoutSeconds, string actionName, Action<SparkTestConfiguration> action, Type? exceptionType, Type? alternateExceptionType)
            {
                QueryTimeoutSeconds = queryTimeoutSeconds;
                ActionName = actionName;
                MetadataAction = action;
                ExceptionType = exceptionType;
                AlternateExceptionType = alternateExceptionType;
            }

            /// <summary>
            /// If null, uses the default timeout.
            /// </summary>
            public int? QueryTimeoutSeconds { get; }

            public string ActionName { get; }

            /// <summary>
            /// If null, expected to succeed.
            /// </summary>
            public Type? ExceptionType { get; }

            /// <summary>
            /// Sometimes you can expect one but may get another.
            /// For example, on GetObjectsAll, sometimes a TTransportException is expected but a TaskCanceledException is received during the test.
            /// </summary>
            public Type? AlternateExceptionType { get; }

            /// <summary>
            /// The metadata action to perform.
            /// </summary>
            public Action<SparkTestConfiguration> MetadataAction { get; }
        }

        /// <summary>
        /// Used for testing timeouts on metadata calls.
        /// </summary>
        internal class MetadataTimeoutTestData : TheoryData<MetadataWithExceptions>
        {
            public MetadataTimeoutTestData()
            {
                SparkConnectionTest sparkConnectionTest = new SparkConnectionTest(null);

                Action<SparkTestConfiguration> getObjectsAll = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.All, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Table, null, null);
                };

                Action<SparkTestConfiguration> getObjectsCatalogs = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.Catalogs, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Schema, null, null);
                };

                Action<SparkTestConfiguration> getObjectsDbSchemas = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.DbSchemas, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Schema, null, null);
                };

                Action<SparkTestConfiguration> getObjectsTables = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.Tables, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Schema, null, null);
                };

                AddAction("getObjectsAll", getObjectsAll, new List<Type?>() { null, typeof(TaskCanceledException), null, null, null } );
                AddAction("getObjectsCatalogs", getObjectsCatalogs);
                AddAction("getObjectsDbSchemas", getObjectsDbSchemas);
                AddAction("getObjectsTables", getObjectsTables);

                Action<SparkTestConfiguration> getTableTypes = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetTableTypes();
                };

                AddAction("getTableTypes", getTableTypes);
                
                Action<SparkTestConfiguration> getTableSchema = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetTableSchema(testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Table);
                };

                AddAction("getTableSchema", getTableSchema);
            }

            private void AddAction(string name, Action<SparkTestConfiguration> action, List<Type?>? alternateExceptions = null)
            {
                List<Type?> expectedExceptions = new List<Type?>()
                {
                    null, // QueryTimeout = -1
                    typeof(TTransportException), // QueryTimeout = 1
                    typeof(TimeoutException), // QueryTimeout = 10
                    null, // QueryTimeout = default
                    null // QueryTimeout = 300
                };

                AddAction(name, action, expectedExceptions, alternateExceptions);
            }

            /// <summary>
            /// Adds the action with the default timeouts.
            /// </summary>
            /// <param name="action">The action to perform.</param>
            /// <param name="expectedExceptions">The expected exceptions.</param>
            /// <remarks>
            /// For List<Type?> the position is based on the behavior when: 
            ///    [0] QueryTimeout = -1
            ///    [1] QueryTimeout = 1
            ///    [2] QueryTimeout = 10
            ///    [3] QueryTimeout = default
            ///    [4] QueryTimeout = 300
            /// </remarks>
            private void AddAction(string name, Action<SparkTestConfiguration> action, List<Type?> expectedExceptions, List<Type?>? alternateExceptions)
            {
                Assert.True(expectedExceptions.Count == 5);

                Add(new(-1, name, action, expectedExceptions[0], alternateExceptions?[0]));
                Add(new(1, name, action, expectedExceptions[1], alternateExceptions?[1]));
                Add(new(10, name, action, expectedExceptions[2], alternateExceptions?[2]));
                Add(new(null, name, action, expectedExceptions[3], alternateExceptions?[3]));
                Add(new(300, name, action, expectedExceptions[4], alternateExceptions?[4]));
            }
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
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword", [SparkParameters.ConnectTimeoutMilliseconds] = "0" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword", [SparkParameters.ConnectTimeoutMilliseconds] = ((long)int.MaxValue + 1).ToString() }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword", [SparkParameters.ConnectTimeoutMilliseconds] = "non-numeric" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword", [SparkParameters.ConnectTimeoutMilliseconds] = "" }, typeof(ArgumentOutOfRangeException)));
            }
        }
    }
}

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
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Thrift.Transport;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// Class for testing the Databricks ADBC connection tests.
    /// </summary>
    public class DatabricksConnectionTest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public DatabricksConnectionTest(ITestOutputHelper? outputHelper) : base(outputHelper, new DatabricksTestEnvironment.Factory())
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

        /// <summary>
        /// Tests connection timeout to establish a session with the backend.
        /// </summary>
        /// <param name="connectTimeoutMilliseconds">The timeout (in ms)</param>
        /// <param name="exceptionType">The exception type to expect (if any)</param>
        /// <param name="alternateExceptionType">An alternate exception that may occur (if any)</param>
        [SkippableTheory]
        [InlineData(0, null, null)]
        [InlineData(1, typeof(TimeoutException), typeof(TTransportException))]
        [InlineData(10, typeof(TimeoutException), typeof(TTransportException))]
        [InlineData(30000, null, null)]
        [InlineData(null, null, null)]
        public void ConnectionTimeoutTest(int? connectTimeoutMilliseconds, Type? exceptionType, Type? alternateExceptionType)
        {
            DatabricksTestConfiguration testConfiguration = (DatabricksTestConfiguration)TestConfiguration.Clone();

            if (connectTimeoutMilliseconds.HasValue)
                testConfiguration.ConnectTimeoutMilliseconds = connectTimeoutMilliseconds.Value.ToString();

            OutputHelper?.WriteLine($"ConnectTimeoutMilliseconds: {testConfiguration.ConnectTimeoutMilliseconds}. ShouldSucceed: {exceptionType == null}");

            try
            {
                NewConnection(testConfiguration);
            }
            catch (AggregateException aex)
            {
                if (exceptionType != null)
                {
                    if (alternateExceptionType != null && aex.InnerException?.GetType() != exceptionType)
                    {
                        if (aex.InnerException?.GetType() == typeof(HiveServer2Exception))
                        {
                            // a TTransportException is inside a HiveServer2Exception
                            Assert.IsType(alternateExceptionType, aex.InnerException!.InnerException);
                        }
                        else
                        {
                            throw;
                        }
                    }
                    else
                    {
                        Assert.IsType(exceptionType, aex.InnerException);
                    }
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Tests the various metadata calls on a DatabricksConnection
        /// </summary>
        /// <param name="metadataWithException"></param>
        [SkippableTheory]
        [ClassData(typeof(MetadataTimeoutTestData))]
        internal void MetadataTimeoutTest(MetadataWithExceptions metadataWithException)
        {
            DatabricksTestConfiguration testConfiguration = (DatabricksTestConfiguration)TestConfiguration.Clone();

            if (metadataWithException.QueryTimeoutSeconds.HasValue)
                testConfiguration.QueryTimeoutSeconds = metadataWithException.QueryTimeoutSeconds.Value.ToString();

            OutputHelper?.WriteLine($"Action: {metadataWithException.ActionName}. QueryTimeoutSeconds: {testConfiguration.QueryTimeoutSeconds}. ShouldSucceed: {metadataWithException.ExceptionType == null}");

            try
            {
                metadataWithException.MetadataAction(testConfiguration);
            }
            catch (Exception ex) when (ApacheUtility.ContainsException(ex, metadataWithException.ExceptionType, out Exception? containedException))
            {
                Assert.IsType(metadataWithException.ExceptionType!, containedException);
            }
            catch (Exception ex) when (ApacheUtility.ContainsException(ex, metadataWithException.AlternateExceptionType, out Exception? containedException))
            {
                Assert.IsType(metadataWithException.AlternateExceptionType!, containedException);
            }
        }

        /// <summary>
        /// Data type used for metadata timeout tests.
        /// </summary>
        internal class MetadataWithExceptions
        {
            public MetadataWithExceptions(int? queryTimeoutSeconds, string actionName, Action<DatabricksTestConfiguration> action, Type? exceptionType, Type? alternateExceptionType)
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
            public Action<DatabricksTestConfiguration> MetadataAction { get; }
        }

        /// <summary>
        /// Used for testing timeouts on metadata calls.
        /// </summary>
        internal class MetadataTimeoutTestData : TheoryData<MetadataWithExceptions>
        {
            public MetadataTimeoutTestData()
            {
                DatabricksConnectionTest sparkConnectionTest = new DatabricksConnectionTest(null);

                Action<DatabricksTestConfiguration> getObjectsAll = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.All, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Table, null, null);
                };

                Action<DatabricksTestConfiguration> getObjectsCatalogs = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.Catalogs, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Schema, null, null);
                };

                Action<DatabricksTestConfiguration> getObjectsDbSchemas = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.DbSchemas, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Schema, null, null);
                };

                Action<DatabricksTestConfiguration> getObjectsTables = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetObjects(AdbcConnection.GetObjectsDepth.Tables, testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Schema, null, null);
                };

                AddAction("getObjectsAll", getObjectsAll, new List<Type?>() { null, typeof(TimeoutException), null, null, null });
                AddAction("getObjectsCatalogs", getObjectsCatalogs);
                AddAction("getObjectsDbSchemas", getObjectsDbSchemas);
                AddAction("getObjectsTables", getObjectsTables);

                Action<DatabricksTestConfiguration> getTableTypes = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetTableTypes();
                };

                AddAction("getTableTypes", getTableTypes);

                Action<DatabricksTestConfiguration> getTableSchema = (testConfiguration) =>
                {
                    AdbcConnection cn = sparkConnectionTest.NewConnection(testConfiguration);
                    cn.GetTableSchema(testConfiguration.Metadata.Catalog, testConfiguration.Metadata.Schema, testConfiguration.Metadata.Table);
                };

                AddAction("getTableSchema", getTableSchema);
            }

            /// <summary>
            /// Adds the action with the default timeouts.
            /// </summary>
            /// <param name="name">The friendly name of the action.</param>
            /// <param name="action">The action to perform.</param>
            /// <param name="alternateExceptions">Optional list of alternate exceptions that are possible. Must have 5 items if present.</param>
            private void AddAction(string name, Action<DatabricksTestConfiguration> action, List<Type?>? alternateExceptions = null)
            {
                List<Type?> expectedExceptions = new List<Type?>()
                {
                    null, // QueryTimeout = 0
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
            ///    [0] QueryTimeout = 0
            ///    [1] QueryTimeout = 1
            ///    [2] QueryTimeout = 10
            ///    [3] QueryTimeout = default
            ///    [4] QueryTimeout = 300
            /// </remarks>
            private void AddAction(string name, Action<DatabricksTestConfiguration> action, List<Type?> expectedExceptions, List<Type?>? alternateExceptions)
            {
                Assert.True(expectedExceptions.Count == 5);

                if (alternateExceptions != null)
                {
                    Assert.True(alternateExceptions.Count == 5);
                }

                Add(new(0, name, action, expectedExceptions[0], alternateExceptions?[0]));
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
                Add(new(new() { [SparkParameters.Type] = "xxx" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [DatabricksParameters.UseCloudFetch] = "notabool" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [DatabricksParameters.CanDecompressLz4] = "notabool"}, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [DatabricksParameters.MaxBytesPerFile] = "notanumber" }, typeof(ArgumentException)));
                Add(new(new() { [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [DatabricksParameters.MaxBytesPerFile] = "-100" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { /*[SparkParameters.Type] = SparkServerTypeConstants.Databricks,*/ [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = "-1" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { /*[SparkParameters.Type] = SparkServerTypeConstants.Databricks,*/ [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = IPEndPoint.MinPort.ToString(CultureInfo.InvariantCulture) }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { /*[SparkParameters.Type] = SparkServerTypeConstants.Databricks,*/ [SparkParameters.HostName] = "valid.server.com", [SparkParameters.Token] = "abcdef", [SparkParameters.Port] = (IPEndPoint.MaxPort + 1).ToString(CultureInfo.InvariantCulture) }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { /*[SparkParameters.Type] = SparkServerTypeConstants.Databricks,*/ [SparkParameters.Token] = "abcdef", [AdbcOptions.Uri] = "httpxxz://hostname.com" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { /*[SparkParameters.Type] = SparkServerTypeConstants.Databricks,*/ [SparkParameters.Token] = "abcdef", [AdbcOptions.Uri] = "http-//hostname.com" }, typeof(ArgumentException)));
                Add(new(new() { /*[SparkParameters.Type] = SparkServerTypeConstants.Databricks,*/ [SparkParameters.Token] = "abcdef", [AdbcOptions.Uri] = "httpxxz://hostname.com:1234567890" }, typeof(ArgumentException)));
                Add(new(new() { /*[SparkParameters.Type] = SparkServerTypeConstants.Databricks,*/ [SparkParameters.Token] = "abcdef", [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Uri] = "http://valid.hostname.com" }, typeof(ArgumentOutOfRangeException)));

                // Tests for the new retry configuration parameters
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword", [DatabricksParameters.TemporarilyUnavailableRetry] = "invalid" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword", [DatabricksParameters.TemporarilyUnavailableRetryTimeout] = "invalid" }, typeof(ArgumentOutOfRangeException)));
                Add(new(new() { [SparkParameters.Type] = SparkServerTypeConstants.Http, [SparkParameters.HostName] = "valid.server.com", [AdbcOptions.Username] = "user", [AdbcOptions.Password] = "myPassword", [DatabricksParameters.TemporarilyUnavailableRetryTimeout] = "-1" }, typeof(ArgumentOutOfRangeException)));
            }
        }
    }
}

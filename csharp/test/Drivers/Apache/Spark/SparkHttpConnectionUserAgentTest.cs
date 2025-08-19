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
using System.Net.Http;
using System.Reflection;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Tests for the SparkHttpConnection user agent functionality.
    /// </summary>
    public class SparkHttpConnectionUserAgentTest
    {
        [Fact]
        public void UserAgentEntry_WhenNotProvided_UsesBaseUserAgentWithThrift()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.Type] = SparkServerTypeConstants.Http,
                [SparkParameters.HostName] = "valid.server.com",
                [SparkParameters.Path] = "/path",
                [SparkParameters.AuthType] = SparkAuthTypeConstants.None
            };

            // Act
            string userAgent = GetUserAgentFromConnection(properties);

            // Assert
            Assert.Matches(@"ADBCSparkDriver/[\d\.]+ Thrift(/[\d\.]+)?", userAgent);
        }

        [Fact]
        public void UserAgentEntry_WhenProvided_AppendsToBaseUserAgent()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.Type] = SparkServerTypeConstants.Http,
                [SparkParameters.HostName] = "valid.server.com",
                [SparkParameters.Path] = "/path",
                [SparkParameters.AuthType] = SparkAuthTypeConstants.None,
                [SparkParameters.UserAgentEntry] = "PowerBI"
            };

            // Act
            string userAgent = GetUserAgentFromConnection(properties);

            // Assert
            Assert.Matches(@"ADBCSparkDriver/[\d\.]+ Thrift(/[\d\.]+)? PowerBI", userAgent);
        }

        [Fact]
        public void UserAgentEntry_WhenEmpty_UsesBaseUserAgent()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.Type] = SparkServerTypeConstants.Http,
                [SparkParameters.HostName] = "valid.server.com",
                [SparkParameters.Path] = "/path",
                [SparkParameters.AuthType] = SparkAuthTypeConstants.None,
                [SparkParameters.UserAgentEntry] = ""
            };

            // Act
            string userAgent = GetUserAgentFromConnection(properties);

            // Assert
            Assert.Matches(@"ADBCSparkDriver/[\d\.]+ Thrift(/[\d\.]+)?", userAgent);
        }

        [Fact]
        public void UserAgentEntry_WhenWhitespace_UsesBaseUserAgent()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.Type] = SparkServerTypeConstants.Http,
                [SparkParameters.HostName] = "valid.server.com",
                [SparkParameters.Path] = "/path",
                [SparkParameters.AuthType] = SparkAuthTypeConstants.None,
                [SparkParameters.UserAgentEntry] = "   "
            };

            // Act
            string userAgent = GetUserAgentFromConnection(properties);

            // Assert
            Assert.Matches(@"ADBCSparkDriver/[\d\.]+ Thrift(/[\d\.]+)?", userAgent);
        }

        [Fact]
        public void UserAgent_IncludesThriftComponent()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.Type] = SparkServerTypeConstants.Http,
                [SparkParameters.HostName] = "valid.server.com",
                [SparkParameters.Path] = "/path",
                [SparkParameters.AuthType] = SparkAuthTypeConstants.None
            };

            // Act
            string userAgent = GetUserAgentFromConnection(properties);

            // Assert
            Assert.Contains("Thrift", userAgent);
        }

        private string GetUserAgentFromConnection(Dictionary<string, string> properties)
        {
            // Create the connection
            var connection = new SparkHttpConnection(properties);

            // Use reflection to access the private GetUserAgent method
            var getUserAgentMethod = typeof(SparkHttpConnection).GetMethod("GetUserAgent", BindingFlags.NonPublic | BindingFlags.Instance);

            if (getUserAgentMethod == null)
            {
                throw new InvalidOperationException("GetUserAgent method not found");
            }

            // Invoke the method
            return (string)getUserAgentMethod.Invoke(connection, null)!;
        }
    }
}

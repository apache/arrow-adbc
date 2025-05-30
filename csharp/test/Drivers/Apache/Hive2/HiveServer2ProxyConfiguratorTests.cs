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
using System.Net;
using System.Net.Http;
using Xunit;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2.Tests
{
    public class HiveServer2ProxyConfiguratorTests
    {
        [Fact]
        public void ConfigureProxy_NoProxySettings_DisablesProxy()
        {
            // Arrange
            var properties = new Dictionary<string, string>();
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            // Act
            configurator.ConfigureProxy(handler);

            // Assert
            Assert.False(handler.UseProxy);
        }

        [Fact]
        public void ConfigureProxy_UseProxyWithInvalidPort_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "1" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "99999" } // Invalid port
            };

            // Act & Assert
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() => HiveServer2ProxyConfigurator.FromProperties(properties));
            Assert.Equal(HttpProxyOptions.ProxyPort, ex.ParamName);
        }

        [Fact]
        public void ConfigureProxy_ValidProxySettings_ConfiguresProxy()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "1" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            // Act
            configurator.ConfigureProxy(handler);

            // Assert
            Assert.True(handler.UseProxy);
            Assert.NotNull(handler.Proxy);
            Assert.IsType<WebProxy>(handler.Proxy);
        }

        [Fact]
        public void ConfigureProxy_ProxyWithAuthentication_ConfiguresProxyCredentials()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyAuth, "true" },
                { HttpProxyOptions.ProxyUID, "username" },
                { HttpProxyOptions.ProxyPWD, "password" }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            // Act
            configurator.ConfigureProxy(handler);

            // Assert
            Assert.True(handler.UseProxy);
            Assert.NotNull(handler.Proxy);
            Assert.IsType<WebProxy>(handler.Proxy);

            var proxy = (WebProxy)handler.Proxy;
            var credentials = proxy.Credentials;
            Assert.NotNull(credentials);
            Assert.IsType<NetworkCredential>(credentials);

            var networkCredential = (NetworkCredential)credentials;
            Assert.Equal("username", networkCredential.UserName);
            Assert.Equal("password", networkCredential.Password);
        }

        [Fact]
        public void ConfigureProxy_ProxyWithBypassList_ConfiguresProxyBypassList()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "1" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyIgnoreList, "localhost,127.0.0.1,*.internal.domain.com" }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            // Act
            configurator.ConfigureProxy(handler);

            // Assert
            Assert.True(handler.UseProxy);
            Assert.NotNull(handler.Proxy);
            Assert.IsType<WebProxy>(handler.Proxy);

            // We can't directly check the bypass list, but we can use reflection to verify it's set
            var proxy = (WebProxy)handler.Proxy;
            var bypassList = proxy.BypassList;
            Assert.NotNull(bypassList);
            Assert.NotEmpty(bypassList);

            // Check if the bypass list contains the expected patterns
            Assert.Contains("^localhost$", bypassList);
            Assert.Contains("^127\\.0\\.0\\.1$", bypassList);
            Assert.Contains("^.*\\.internal\\.domain\\.com$", bypassList);
        }
    }
}

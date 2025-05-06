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
using System.Text.RegularExpressions;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    /// <summary>
    /// Interface for proxy configuration
    /// </summary>
    public interface IProxyConfigurator
    {
        /// <summary>
        /// Configures proxy settings on an HttpClientHandler
        /// </summary>
        /// <param name="handler">The HttpClientHandler to configure</param>
        void ConfigureProxy(HttpClientHandler handler);
    }

    /// <summary>
    /// Default implementation of proxy configuration for Spark connections
    /// </summary>
    public class SparkProxyConfigurator : IProxyConfigurator
    {
        private readonly IReadOnlyDictionary<string, string> _properties;

        /// <summary>
        /// Initializes a new instance of the <see cref="SparkProxyConfigurator"/> class.
        /// </summary>
        /// <param name="properties">The connection properties containing proxy settings</param>
        public SparkProxyConfigurator(IReadOnlyDictionary<string, string> properties)
        {
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));
        }

        /// <summary>
        /// Configures proxy settings on an HttpClientHandler based on stored properties
        /// </summary>
        /// <param name="handler">The HttpClientHandler to configure</param>
        public void ConfigureProxy(HttpClientHandler handler)
        {
            // Check if proxy is enabled
            if (_properties.TryGetValue(SparkParameters.UseProxy, out string? useProxyStr) &&
                useProxyStr == "1")
            {
                ConfigureExplicitProxy(handler);
            }
            else
            {
                // No proxy configuration
                handler.UseProxy = false;
            }
        }
        
        private void ConfigureExplicitProxy(HttpClientHandler handler)
        {
            // Get proxy host and port
            if (!_properties.TryGetValue(SparkParameters.ProxyHost, out string? proxyHost) ||
                string.IsNullOrEmpty(proxyHost))
            {
                throw new ArgumentException($"Parameter '{SparkParameters.UseProxy}' is set to '1' but '{SparkParameters.ProxyHost}' is not specified");
            }

            int proxyPort = 8080; // Default port
            if (_properties.TryGetValue(SparkParameters.ProxyPort, out string? proxyPortStr))
            {
                if (!int.TryParse(proxyPortStr, out proxyPort) || proxyPort <= 0 || proxyPort > 65535)
                {
                    throw new ArgumentOutOfRangeException(
                        SparkParameters.ProxyPort,
                        $"Invalid proxy port: {proxyPortStr}. Must be between 1 and 65535.");
                }
            }

            // Create and configure the proxy
            var proxy = new WebProxy(proxyHost, proxyPort);

            // Configure authentication if needed
            ConfigureProxyAuthentication(proxy);

            // Configure bypass list
            ConfigureProxyBypassList(proxy);

            // Apply proxy to handler
            handler.Proxy = proxy;
            handler.UseProxy = true;
        }

        private void ConfigureProxyAuthentication(WebProxy proxy)
        {
            if (_properties.TryGetValue(SparkParameters.ProxyAuth, out string? proxyAuthStr) &&
                proxyAuthStr == "1")
            {
                string? proxyUid;
                _properties.TryGetValue(SparkParameters.ProxyUID, out proxyUid);

                string? proxyPwd;
                _properties.TryGetValue(SparkParameters.ProxyPWD, out proxyPwd);

                if (string.IsNullOrEmpty(proxyUid))
                {
                    throw new ArgumentException($"Parameter '{SparkParameters.ProxyAuth}' is set to '1' but '{SparkParameters.ProxyUID}' is not specified");
                }

                if (string.IsNullOrEmpty(proxyPwd))
                {
                    throw new ArgumentException($"Parameter '{SparkParameters.ProxyAuth}' is set to '1' but '{SparkParameters.ProxyPWD}' is not specified");
                }

                proxy.Credentials = new NetworkCredential(proxyUid, proxyPwd);
            }
        }

        private void ConfigureProxyBypassList(WebProxy proxy)
        {
            if (_properties.TryGetValue(SparkParameters.ProxyIgnoreList, out string? proxyIgnoreList) &&
                !string.IsNullOrEmpty(proxyIgnoreList))
            {
                proxy.BypassList = ParseProxyIgnoreList(proxyIgnoreList);
            }
        }

        private string[] ParseProxyIgnoreList(string proxyIgnoreList)
        {
            if (string.IsNullOrEmpty(proxyIgnoreList))
                return [];

            string[] rawHosts = proxyIgnoreList.Split(',');
            List<string> regexPatterns = new List<string>();

            foreach (string rawHost in rawHosts)
            {
                string host = rawHost.Trim();
                if (string.IsNullOrEmpty(host))
                    continue;

                // Escape regex special characters and replace wildcard '*' with '.*'
                string pattern = Regex.Escape(host).Replace("\\*", ".*");

                // Anchor the pattern to match the full host
                regexPatterns.Add($"^{pattern}$");
            }

            return regexPatterns.ToArray();
        }
    }
}

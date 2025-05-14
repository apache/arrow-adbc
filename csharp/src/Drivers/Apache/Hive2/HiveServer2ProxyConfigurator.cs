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

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    /// <summary>
    /// Default implementation of proxy configuration for HTTP connections
    /// </summary>
    internal class HiveServer2ProxyConfigurator
    {
        private readonly bool _useProxy;
        private readonly string? _proxyHost;
        private readonly int? _proxyPort;
        private readonly bool _proxyAuth;
        private readonly string? _proxyUid;
        private readonly string? _proxyPwd;
        private readonly string[]? _proxyBypassList;

        /// <summary>
        /// Initializes a new instance of the <see cref="HiveServer2ProxyConfigurator"/> class.
        /// </summary>
        /// <param name="useProxy">Whether to use a proxy</param>
        /// <param name="proxyPort">The proxy port</param>
        /// <param name="proxyHost">The proxy host</param>
        /// <param name="proxyAuth">Whether to use proxy authentication</param>
        /// <param name="proxyUid">The proxy username</param>
        /// <param name="proxyPwd">The proxy password</param>
        /// <param name="proxyIgnoreList">Comma-separated list of hosts to bypass the proxy</param>
        internal HiveServer2ProxyConfigurator(
            bool useProxy,
            int? proxyPort = null,
            string? proxyHost = null,
            bool proxyAuth = false,
            string? proxyUid = null,
            string? proxyPwd = null,
            string? proxyIgnoreList = null)
        {
            if (useProxy)
            {
                if (proxyHost == null)
                    throw new ArgumentNullException(nameof(proxyHost));
                if (proxyPort == null)
                    throw new ArgumentNullException(nameof(proxyPort));
            }

            if (proxyAuth)
            {
                if (proxyUid == null)
                    throw new ArgumentNullException(nameof(proxyUid));
                if (proxyPwd == null)
                    throw new ArgumentNullException(nameof(proxyPwd));
            }

            _useProxy = useProxy;
            _proxyHost = proxyHost;
            _proxyPort = proxyPort;
            _proxyAuth = proxyAuth;
            _proxyUid = proxyUid;
            _proxyPwd = proxyPwd;

            if (!string.IsNullOrEmpty(proxyIgnoreList))
            {
                _proxyBypassList = ParseProxyIgnoreList(proxyIgnoreList);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HiveServer2ProxyConfigurator"/> class from connection properties.
        /// </summary>
        /// <param name="properties">The connection properties</param>
        internal static HiveServer2ProxyConfigurator FromProperties(IReadOnlyDictionary<string, string> properties)
        {
            bool useProxy = properties.TryGetValue(HttpProxyOptions.UseProxy, out string? useProxyStr) && bool.TryParse(useProxyStr, out bool useProxyBool) && useProxyBool;

            if (!useProxy)
            {
                return new HiveServer2ProxyConfigurator(false, 0);
            }

            // Get proxy host
            if (!properties.TryGetValue(HttpProxyOptions.ProxyHost, out string? proxyHost) ||
                string.IsNullOrEmpty(proxyHost))
            {
                throw new ArgumentException($"Parameter '{HttpProxyOptions.UseProxy}' is set to 'true' but '{HttpProxyOptions.ProxyHost}' is not specified");
            }

            // Get proxy port
            if (!properties.TryGetValue(HttpProxyOptions.ProxyPort, out string? proxyPortStr))
            {
                throw new ArgumentException($"Parameter '{HttpProxyOptions.ProxyPort}' is required when '{HttpProxyOptions.UseProxy}' is set to 'true'");
            }

            if (!int.TryParse(proxyPortStr, out int proxyPort) || proxyPort <= 0 || proxyPort > 65535)
            {
                throw new ArgumentOutOfRangeException(
                    HttpProxyOptions.ProxyPort,
                    $"Invalid proxy port: {proxyPortStr}. Must be between 1 and 65535.");
            }

            // Get proxy authentication settings
            bool proxyAuth = properties.TryGetValue(HttpProxyOptions.ProxyAuth, out string? proxyAuthStr) && bool.TryParse(proxyAuthStr, out bool proxyAuthBool) && proxyAuthBool;

            string? proxyUid = null;
            string? proxyPwd = null;

            if (proxyAuth)
            {
                properties.TryGetValue(HttpProxyOptions.ProxyUID, out proxyUid);
                properties.TryGetValue(HttpProxyOptions.ProxyPWD, out proxyPwd);

                if (string.IsNullOrEmpty(proxyUid))
                {
                    throw new ArgumentException($"Parameter '{HttpProxyOptions.ProxyAuth}' is set to 'true' but '{HttpProxyOptions.ProxyUID}' is not specified");
                }

                if (string.IsNullOrEmpty(proxyPwd))
                {
                    throw new ArgumentException($"Parameter '{HttpProxyOptions.ProxyAuth}' is set to 'true' but '{HttpProxyOptions.ProxyPWD}' is not specified");
                }
            }

            // Get proxy bypass list
            string? proxyIgnoreList;
            properties.TryGetValue(HttpProxyOptions.ProxyIgnoreList, out proxyIgnoreList);

            return new HiveServer2ProxyConfigurator(
                useProxy,
                proxyPort,
                proxyHost,
                proxyAuth,
                proxyUid,
                proxyPwd,
                proxyIgnoreList);
        }

        /// <summary>
        /// Configures proxy settings on an HttpClientHandler
        /// </summary>
        /// <param name="handler">The HttpClientHandler to configure</param>
        internal void ConfigureProxy(HttpClientHandler handler)
        {
            if (_useProxy)
            {
                // Create and configure the proxy
                var proxy = new WebProxy(_proxyHost!, _proxyPort!.Value);
                // Configure authentication if needed
                if (_proxyAuth && !string.IsNullOrEmpty(_proxyUid))
                {
                    proxy.Credentials = new NetworkCredential(_proxyUid, _proxyPwd);
                }

                // Configure bypass list
                if (_proxyBypassList != null && _proxyBypassList.Length > 0)
                {
                    proxy.BypassList = _proxyBypassList;
                }

                // Apply proxy to handler
                handler.Proxy = proxy;
                handler.UseProxy = true;
            }
            else
            {
                // No proxy configuration
                handler.UseProxy = false;
            }
        }

        // http client bypass list in c# expects regex strings, hence why some handling is done to make hosts in regex format.
        // I assume we don't want to expect users to pass in regex strings (though we still allow for wildcard pattern here)
        private static string[] ParseProxyIgnoreList(string? proxyIgnoreList)
        {
            if (string.IsNullOrEmpty(proxyIgnoreList))
                return [];

            string[] rawHosts = proxyIgnoreList!.Split(',');
            string[] patterns = new string[rawHosts.Length];

            for (int i = 0; i < rawHosts.Length; i++)
            {
                string host = rawHosts[i].Trim();
                if (string.IsNullOrEmpty(host))
                    continue;

                // Convert wildcard pattern to regex pattern
                string pattern = "^" + Regex.Escape(host)
                    .Replace("\\*", ".*")
                    .Replace("\\?", ".") + "$";
                patterns[i] = pattern;
            }

            return patterns;
        }
    }
}

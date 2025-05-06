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

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    /// <summary>
    /// Factory for creating HTTP clients with proper configuration
    /// </summary>
    public class SparkHttpMessageHandlerFactory
    {
        private readonly IProxyConfigurator _proxyConfigurator;

        /// <summary>
        /// Creates a new instance of SparkHttpMessasgeHandlerFactory with the specified properties
        /// </summary>
        /// <param name="properties">The connection properties</param>
        public SparkHttpMessageHandlerFactory(IReadOnlyDictionary<string, string> properties)
        {
            _proxyConfigurator = new SparkProxyConfigurator(properties);
        }

        /// <summary>
        /// Creates a new instance of SparkHttpMessasgeHandlerFactory with the specified proxy configurator
        /// </summary>
        /// <param name="proxyConfigurator">The proxy configurator to use</param>
        public SparkHttpMessageHandlerFactory(IProxyConfigurator proxyConfigurator)
        {
            _proxyConfigurator = proxyConfigurator ?? throw new ArgumentNullException(nameof(proxyConfigurator));
        }


        /// <summary>
        /// Creates a new HttpClientHandler configured with the factory's properties
        /// </summary>
        /// <returns>A new HttpClientHandler</returns>
        public HttpMessageHandler CreateHandler()
        {
            var handler = new HttpClientHandler();

            // Configure proxy settings
            _proxyConfigurator.ConfigureProxy(handler);

            return handler;
        }
    }
}


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

using System.Net;
using System.Net.Http;
using Google.Apis.Http;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    static class ProxyManager
    {
        public static HttpClientFactory? GetHttpClientFactory(string? proxy)
        {
            if (string.IsNullOrEmpty(proxy))
            {
                return null;
            }

            return HttpClientFactory.ForProxy(GetWebProxy(proxy!));
        }

        public static HttpClient GetHttpClient(string? proxy)
        {
            if (string.IsNullOrEmpty(proxy))
            {
                return new HttpClient();
            }

            HttpMessageHandler? handler;

#if NET6_0_OR_GREATER
            handler = new SocketsHttpHandler()
            {
                UseProxy = true,
                Proxy = GetWebProxy(proxy!)
            };
#else
            handler = new HttpClientHandler()
            {
                UseProxy = true,
                Proxy = GetWebProxy(proxy!)
            };
#endif

            return new HttpClient(handler);
        }

        private static WebProxy GetWebProxy(string proxy)
        {
            WebProxy webProxy = new WebProxy(proxy);

            // TODO: credentials

            return webProxy;
        }
    }
}

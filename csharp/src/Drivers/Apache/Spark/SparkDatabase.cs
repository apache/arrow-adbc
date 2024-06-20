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

using System.Collections.Generic;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    public class SparkDatabase : AdbcDatabase, IProxyDatabase<TCLIService.IAsync>
    {
        readonly IReadOnlyDictionary<string, string> properties;

        public SparkDatabase(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? properties)
        {
            return Connect(properties, proxy: default);
        }

        //public ProxyConnection<TCLIService.IAsync> Connect<TMock>(IReadOnlyDictionary<string, string>? properties, TMock? proxy = default)
        //    where TMock : MockServerBase<TCLIService.IAsync>, TCLIService.IAsync
        //{
        //    IReadOnlyDictionary<string, string> combinedProperties = MergeDictionaries(this.properties, properties);
        //    SparkConnection connection = new(combinedProperties, proxy);
        //    proxy?.SetNewServer(connection.NewLiveServerAsync);
        //    connection.OpenAsync().Wait();
        //    return connection;
        //}

        public ProxyConnection<TCLIService.IAsync> Connect(IReadOnlyDictionary<string, string>? properties, MockServerBase<TCLIService.IAsync>? proxy)
        {
            IReadOnlyDictionary<string, string> combinedProperties = MergeDictionaries(this.properties, properties);
            SparkConnection connection = new(combinedProperties, proxy);
            connection.OpenAsync().Wait();
            return connection;
        }

        //public ProxyConnection<TCLIService.IAsync> Connect(IReadOnlyDictionary<string, string>? properties, TCLIService.IAsync? proxy)
        //{
        //}

        private static IReadOnlyDictionary<TKey, TValue> MergeDictionaries<TKey, TValue>(params IReadOnlyDictionary<TKey, TValue>?[] dictionaries)
            where TKey : notnull
        {
            var mergedDictionary = new Dictionary<TKey, TValue>();
            foreach (IReadOnlyDictionary<TKey, TValue>? dictionary in dictionaries)
            {
                if (dictionary == null) continue;
                foreach (KeyValuePair<TKey, TValue> kvp in dictionary)
                {
                    mergedDictionary[kvp.Key] = kvp.Value;
                }
            }
            return mergedDictionary;
        }
    }
}

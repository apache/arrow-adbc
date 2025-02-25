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
using System.Diagnostics;
using System.Linq;
using Apache.Arrow.Adbc.Tracing;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkDatabase : AdbcDatabase
    {
        readonly IReadOnlyDictionary<string, string> properties;

        internal SparkDatabase(IReadOnlyDictionary<string, string> properties, ActivityTrace trace)
        {
            Trace = trace;
            this.properties = properties;
        }

        protected ActivityTrace Trace { get; }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            // connection options takes precedence over database properties for the same option
            IReadOnlyDictionary<string, string> mergedProperties = options == null
                ? properties
                : options
                    .Concat(properties.Where(x => !options.Keys.Contains(x.Key, StringComparer.OrdinalIgnoreCase)))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            SparkConnection connection = SparkConnectionFactory.NewConnection(mergedProperties, Trace);
            connection.OpenAsync().Wait();
            return connection;
        }
    }
}

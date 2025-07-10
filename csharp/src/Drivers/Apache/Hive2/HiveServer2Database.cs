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
using System.Linq;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2Database : AdbcDatabase
    {
        private readonly HiveServer2ConfigurationManager _configManager;

        internal HiveServer2Database(IReadOnlyDictionary<string, string> properties)
        {
            _configManager = new HiveServer2ConfigurationManager(properties);
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            // Set connection options in the configuration manager
            _configManager.SetConnectionOptions(options);
            
            // Get merged properties from the configuration manager
            var mergedProperties = _configManager.GetMergedProperties();
            
            HiveServer2Connection connection = HiveServer2ConnectionFactory.NewConnection(mergedProperties);
            connection.OpenAsync().Wait();
            return connection;
        }
    }
}
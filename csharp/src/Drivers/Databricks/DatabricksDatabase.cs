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
using Apache.Arrow.Adbc.Drivers.Apache;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Databricks-specific implementation of <see cref="AdbcDatabase"/>
    /// </summary>
    public class DatabricksDatabase : AdbcDatabase
    {
        readonly IReadOnlyDictionary<string, string> properties;

        /// <summary>
        /// RecyclableMemoryStreamManager for LZ4 decompression output streams.
        /// Shared across all connections from this database to enable memory pooling.
        /// This manager is instance-based to allow cleanup when the database is disposed.
        /// </summary>
        internal readonly Microsoft.IO.RecyclableMemoryStreamManager RecyclableMemoryStreamManager =
            new Microsoft.IO.RecyclableMemoryStreamManager();

        public DatabricksDatabase(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            try
            {
                IReadOnlyDictionary<string, string> mergedProperties = options == null
                    ? properties
                    : options
                        .Concat(properties.Where(x => !options.Keys.Contains(x.Key, StringComparer.OrdinalIgnoreCase)))
                        .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                DatabricksConnection connection = new DatabricksConnection(mergedProperties);
                // Share the RecyclableMemoryStreamManager with this connection
                connection.RecyclableMemoryStreamManager = this.RecyclableMemoryStreamManager;
                connection.OpenAsync().Wait();
                connection.ApplyServerSidePropertiesAsync().Wait();
                return connection;
            }
            catch (AggregateException ae)
            {
                // Unwrap AggregateException to AdbcException if possible
                // to better conform to the ADBC standard
                if (ApacheUtility.ContainsException(ae, out AdbcException? adbcException) && adbcException != null)
                {
                    // keep the entire chain, but throw the AdbcException
                    throw new AdbcException(adbcException.Message, adbcException.Status, ae);
                }

                throw;
            }
        }
    }
}

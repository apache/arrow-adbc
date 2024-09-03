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

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Clients first initialize a database, then create a connection.
    /// This gives the implementation a place to initialize and own any
    /// common connection state.
    /// For example, in-memory databases can place ownership of the actual
    /// database in this object.
    /// </summary>
    public abstract class AdbcDatabase : IDisposable
    {
        /// <summary>
        /// Options are generally set before opening a database.  Some drivers may
        /// support setting options after opening as well.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, string value)
        {
            throw AdbcException.NotImplemented("Connection does not support setting options");
        }

        /// <summary>
        /// Options are generally set before opening a database.  Some drivers may
        /// support setting options after opening as well.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, object value)
        {
            throw AdbcException.NotImplemented("Connection does not support setting options");
        }

        /// <summary>
        /// Create a new connection to the database.
        /// </summary>
        /// <param name="options">
        /// Additional options to use when connecting.
        /// </param>
        public abstract AdbcConnection Connect(IReadOnlyDictionary<string, string>? options);

        public virtual void Dispose() { }
    }
}

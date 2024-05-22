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
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Clients first initialize a database, then create a connection.
    /// This gives the implementation a place to initialize and own any
    /// common connection state.
    /// For example, in-memory databases can place ownership of the actual
    /// database in this object.
    /// </summary>
    public abstract class AdbcDatabase11 : IDisposable
    {
        ~AdbcDatabase11() => Dispose(false);

        /// <summary>
        /// Opens a new connection to the database.
        /// </summary>
        /// <param name="options">Additional options to use when connecting.</param>
        /// <returns>The database connection.</returns>
        public virtual AdbcConnection11 Connect(IReadOnlyDictionary<string, object>? options = default)
        {
            return Task.Run(() => Connect(options)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Opens a new connection to the database
        /// </summary>
        /// <param name="options">Additional options to use when connecting.</param>
        /// <returns>The database connection.</returns>
        public abstract Task<AdbcConnection11> ConnectAsync(IReadOnlyDictionary<string, object>? options = default);

        /// <summary>
        /// Options are generally set before opening a database.
        /// Throws an exception for unsupported options.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>Option value.</returns>
        public virtual object GetOption(string key)
        {
            throw AdbcException.NotImplemented("Connection does not support getting options");
        }

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

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }
    }
}

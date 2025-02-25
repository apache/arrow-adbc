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
    /// This provides a common interface for vendor-specific driver
    /// initialization routines.
    /// </summary>
    public abstract class AdbcDriver11 : IDisposable
    {
        ~AdbcDriver11() => Dispose(false);

        /// <summary>
        /// Returns the version of the ADBC spec supported by this driver.
        /// </summary>
        public virtual int AdbcVersion => Adbc.AdbcVersion.Version_1_0_0;

        /// <summary>
        /// Creates a database reference via this driver.
        /// </summary>
        /// <param name="parameters">Driver-specific parameters.</param>
        /// <returns>An object representing a reference to a specific database.</returns>
        public abstract AdbcDatabase11 Create(IReadOnlyDictionary<string, string> parameters);

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

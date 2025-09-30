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

using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// A container for all state needed to execute a database
    /// query, such as the query itself, parameters for prepared
    /// statements, driver parameters, etc.
    ///
    /// Statements may represent queries or prepared statements.
    ///
    /// Statements may be used multiple times and can be reconfigured
    /// (e.g. they can be reused to execute multiple different queries).
    /// However, executing a statement (and changing certain other state)
    /// will invalidate result sets obtained prior to that execution.
    ///
    /// Multiple statements may be created from a single connection.
    /// However, the driver may block or error if they are used
    /// concurrently (whether from a single thread or multiple threads).
    ///
    /// Statements are not required to be thread-safe, but they can be
    /// used from multiple threads so long as clients take care to
    /// serialize accesses to a statement.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CAdbcStatement
    {
        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is uninitialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// The associated driver (used by the driver manager to help
        /// track state).
        /// </summary>
        public CAdbcDriver* private_driver;
    }
}

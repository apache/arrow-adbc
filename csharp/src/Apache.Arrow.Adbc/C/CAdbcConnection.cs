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
    /// An active database connection.
    ///
    /// Provides methods for query execution, managing prepared
    /// statements, using transactions, and so on.
    ///
    /// Connections are not required to be thread-safe, but they can be
    /// used from multiple threads so long as clients take care to
    /// serialize accesses to a connection.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CAdbcConnection
    {
        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is uninitialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// The associated driver (used by the driver manager to help
        ///   track state).
        /// </summary>
        public CAdbcDriver* private_driver;
    }
}

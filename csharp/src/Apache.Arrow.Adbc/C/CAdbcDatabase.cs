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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// Clients first initialize a database, then create a connection.
    /// This gives the implementation a place to initialize and
    /// own any common connection state.  For example, in-memory databases
    /// can place ownership of the actual database in this object.
    /// </summary>
    /// <remarks>
    /// An instance of a database.
    ///
    /// Must be kept alive as long as any connections exist.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CAdbcDatabase
    {
        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is unintialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// The associated driver (used by the driver manager to help track
        /// state).
        /// </summary>
        public CAdbcDriver* private_driver;

        public static CAdbcDatabase* Create()
        {
            var ptr = (CAdbcDatabase*)Marshal.AllocHGlobal(sizeof(CAdbcDatabase));

            ptr->private_data = null;
            ptr->private_driver = null;

            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="Create"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void Free(CAdbcDatabase* database)
        {
            Marshal.FreeHGlobal((IntPtr)database);
        }
    }
}

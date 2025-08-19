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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// The partitions of a distributed/partitioned result set.
    /// </summary>
    /// <remarks>
    /// Some backends may internally partition the results. These
    /// partitions are exposed to clients who may wish to integrate them
    /// with a threaded or distributed execution model, where partitions
    /// can be divided among threads or machines and fetched in parallel.
    ///
    /// To use partitioning, execute the statement with
    /// AdbcStatementExecutePartitions to get the partition descriptors.
    /// Call AdbcConnectionReadPartition to turn the individual
    /// descriptors into ArrowArrayStream instances.  This may be done on
    /// a different connection than the one the partition was created
    /// with, or even in a different process on another machine.
    ///
    /// Drivers are not required to support partitioning.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CAdbcPartitions
    {
        /// <summary>
        /// The number of partitions.
        /// </summary>
        public int num_partitions;

        /// <summary>
        /// The partitions of the result set, where each entry (up to
        /// num_partitions entries) is an opaque identifier that can be
        /// passed to AdbcConnectionReadPartition.
        /// </summary>
        public byte** partitions;

        /// <summary>
        /// The length of each corresponding entry in partitions.
        /// </summary>
        public nuint* partition_lengths;

        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is uninitialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// Release the contained partitions.
        ///
        /// Unlike other structures, this is an embedded callback to make it
        /// easier for the driver manager and driver to cooperate.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcPartitions*, void> release;
#else
        internal IntPtr release;
#endif
    }
}

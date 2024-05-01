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

using System.Collections.Generic;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Represents a query result.
    /// </summary>
    public sealed class QueryResult
    {
        /// <summary>
        /// Initializes an AdbcQueryResult
        /// </summary>
        /// <param name="rowCount">
        /// The number of records in the result
        /// </param>
        /// <param name="stream">
        /// The <see cref="IArrowArrayStream"/> for reading
        /// </param>
        public QueryResult(long rowCount, IArrowArrayStream stream)
        {
            RowCount = rowCount;
            Stream = stream;
        }

        /// <summary>
        /// The number of records in the result.
        /// </summary>
        public long RowCount { get; set; }

        /// <summary>
        /// The <see cref="IArrowArrayStream"/> for reading.
        /// </summary>
        public IArrowArrayStream? Stream { get; set; }
    }

    /// <summary>
    /// The result of executing a query without a result set.
    /// </summary>
    public sealed class UpdateResult
    {
        private readonly long _affectedRows = -1;

        public UpdateResult(long affectedRows)
        {
            _affectedRows = affectedRows;
        }

        /// <summary>
        /// The number of records in the result or -1 if not known.
        /// </summary>
        public long AffectedRows { get => _affectedRows; }
    }

    /// <summary>
    /// The partitions of a result set.
    /// </summary>
    public sealed class PartitionedResult
    {
        private readonly Schema _schema;
        private readonly long _affectedRows = -1;
        private readonly IReadOnlyList<PartitionDescriptor> _partitionDescriptors;

        public PartitionedResult(Schema schema, long affectedRows, IReadOnlyList<PartitionDescriptor> partitionDescriptors)
        {
            _schema = schema;
            _affectedRows = affectedRows;
            _partitionDescriptors = partitionDescriptors;
        }

        /// <summary>
        /// Get the schema of the eventual result set.
        /// </summary>
        public Schema Schema { get => _schema; }

        /// <summary>
        /// Get the number of affected rows, or -1 if not known.
        /// </summary>
        public long AffectedRows { get => _affectedRows; }

        /// <summary>
        /// Get partitions.
        /// </summary>
        public IReadOnlyList<PartitionDescriptor> PartitionDescriptors { get => _partitionDescriptors; }
    }
}

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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// Simple implementation of IArrowArrayStream that returns a predefined list of batches.
    /// </summary>
    internal class ListArrayStream : IArrowArrayStream
    {
        private readonly Schema _schema;
        private readonly IEnumerator<RecordBatch> _enumerator;
        private bool _disposed;

        public ListArrayStream(Schema schema, IEnumerable<RecordBatch> batches)
        {
            _schema = schema;
            _enumerator = batches.GetEnumerator();
        }

        public Schema Schema => _schema;

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                return new ValueTask<RecordBatch?>((RecordBatch?)null);
            }

            if (_enumerator.MoveNext())
            {
                return new ValueTask<RecordBatch?>(_enumerator.Current);
            }

            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _enumerator.Dispose();
                _disposed = true;
            }
        }
    }
}
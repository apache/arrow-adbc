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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DuckDB.NET.Data;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// Simplified GetObjects implementation for DuckDB that returns empty results.
    /// This avoids the complex nested structure building issues.
    /// </summary>
    internal class GetObjectsReaderSimple : IArrowArrayStream
    {
        private readonly Schema _schema;
        private bool _hasRead = false;

        public GetObjectsReaderSimple()
        {
            _schema = StandardSchemas.GetObjectsSchema;
        }

        public Schema Schema => _schema;

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_hasRead)
            {
                return new ValueTask<RecordBatch?>((RecordBatch?)null);
            }

            _hasRead = true;

            // Return an empty result set with the correct schema
            var catalogNameBuilder = new StringArray.Builder();
            var catalogDbSchemasBuilder = new ListArray.Builder(new StructType(StandardSchemas.DbSchemaSchema));

            // Build empty arrays
            var batch = new RecordBatch(_schema, new IArrowArray[]
            {
                catalogNameBuilder.Build(),
                catalogDbSchemasBuilder.Build()
            }, 0);

            return new ValueTask<RecordBatch?>(batch);
        }

        public void Dispose()
        {
        }
    }
}
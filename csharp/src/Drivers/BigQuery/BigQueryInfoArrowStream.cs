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

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    /// <summary>
    /// Stream used for metadata calls
    /// </summary>
    internal class BigQueryInfoArrowStream : IArrowArrayStream
    {
        private Schema schema;
        private IEnumerable<IArrowArray> data;
        private int length;

        public BigQueryInfoArrowStream(Schema schema, IEnumerable<IArrowArray> data, int length)
        {
            this.schema = schema;
            this.data = data;
            this.length = length;
        }

        public Schema Schema { get { return this.schema; } }

        public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            return new ValueTask<RecordBatch>(new RecordBatch(schema, data, length));
        }

        public void Dispose()
        {
        }
    }
}

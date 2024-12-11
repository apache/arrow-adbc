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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal sealed class SparkDatabricksReader : IArrowArrayStream
    {
        HiveServer2Statement? statement;
        Schema schema;
        List<TSparkArrowBatch>? batches;
        int index;
        IArrowReader? reader;

        public SparkDatabricksReader(HiveServer2Statement statement, Schema schema)
        {
            this.statement = statement;
            this.schema = schema;
        }

        public Schema Schema { get { return schema; } }

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                if (this.reader != null)
                {
                    RecordBatch? next = await this.reader.ReadNextRecordBatchAsync(cancellationToken);
                    if (next != null)
                    {
                        return next;
                    }
                    this.reader = null;
                }

                if (this.batches != null && this.index < this.batches.Count)
                {
                    this.reader = new ArrowStreamReader(new ChunkStream(this.schema, this.batches[this.index++].Batch));
                    continue;
                }

                this.batches = null;
                this.index = 0;

                if (this.statement == null)
                {
                    return null;
                }

                TFetchResultsReq request = new TFetchResultsReq(this.statement.OperationHandle, TFetchOrientation.FETCH_NEXT, this.statement.BatchSize);
                TFetchResultsResp response = await this.statement.Connection.Client!.FetchResults(request, cancellationToken);
                this.batches = response.Results.ArrowBatches;

                if (!response.HasMoreRows)
                {
                    this.statement = null;
                }
            }
        }

        public void Dispose()
        {
        }
    }
}

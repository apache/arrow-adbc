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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal sealed class SparkDatabricksReader : IArrowArrayStream
    {
        HiveServer2Statement? statement;
        Schema schema;
        List<TSparkArrowBatch>? batches;
        int index;
        IArrowReader? reader;
        bool isLz4Compressed;

        public SparkDatabricksReader(HiveServer2Statement statement, Schema schema)
            : this(statement, schema, false)
        {
        }

        public SparkDatabricksReader(HiveServer2Statement statement, Schema schema, bool isLz4Compressed)
        {
            this.statement = statement;
            this.schema = schema;
            this.isLz4Compressed = isLz4Compressed;
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
                    ProcessFetchedBatches();
                    continue;
                }

                this.batches = null;
                this.index = 0;

                if (this.statement == null)
                {
                    return null;
                }

                TFetchResultsReq request = new TFetchResultsReq(this.statement.OperationHandle!, TFetchOrientation.FETCH_NEXT, this.statement.BatchSize);
                TFetchResultsResp response = await this.statement.Connection.Client!.FetchResults(request, cancellationToken);

                // Make sure we get the arrowBatches
                this.batches = response.Results.ArrowBatches;

                if (!response.HasMoreRows)
                {
                    this.statement = null;
                }
            }
        }

        private void ProcessFetchedBatches()
        {
            var batch = this.batches![this.index];

            // Ensure batch data exists
            if (batch.Batch == null || batch.Batch.Length == 0)
            {
                this.index++;
                return;
            }

            try
            {
                ReadOnlyMemory<byte> dataToUse = new ReadOnlyMemory<byte>(batch.Batch);

                // If LZ4 compression is enabled, decompress the data
                if (isLz4Compressed)
                {
                    dataToUse = Lz4Utilities.DecompressLz4(batch.Batch);
                }

                // Always use ChunkStream which ensures proper schema handling
                this.reader = new ArrowStreamReader(new ChunkStream(this.schema, dataToUse));
            }
            catch (Exception ex)
            {
                // Create concise error message based on exception type
                string errorMessage = ex switch
                {
                    _ when ex.GetType().Name.Contains("LZ4") => $"Batch {this.index}: LZ4 decompression failed - Data may be corrupted",
                    _ => $"Batch {this.index}: Processing failed - {ex.Message}" // Default case for any other exception
                };
                throw new AdbcException(errorMessage, ex);
            }
            this.index++;
        }

        public void Dispose()
        {
        }
    }
}

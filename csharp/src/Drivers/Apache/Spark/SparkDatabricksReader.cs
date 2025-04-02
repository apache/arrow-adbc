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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal sealed class SparkDatabricksReader : IArrowArrayStream
    {
        private HiveServer2Statement? statement;
        private readonly Schema schema;
        private List<TSparkArrowBatch>? batches;
        private int index;
        private IArrowReader? reader;
        private Task<TFetchResultsResp>? prefetchTask;
        private bool isEndOfData = false;
        private long totalRowsRead = 0;
        private readonly Stopwatch fetchTimer = new Stopwatch();

        public SparkDatabricksReader(HiveServer2Statement statement, Schema schema)
        {
            this.statement = statement;
            this.schema = schema;
        }

        public Schema Schema { get { return schema; } }

        // Start prefetching the next batch of results
        private void StartPrefetch(CancellationToken cancellationToken)
        {
            if (this.statement == null || this.isEndOfData || this.prefetchTask != null)
            {
                return; // We're done or already prefetching
            }

            // Create the fetch request
            TFetchResultsReq request = new TFetchResultsReq(this.statement.OperationHandle, TFetchOrientation.FETCH_NEXT, this.statement.BatchSize);

            // Start the async fetch
            this.fetchTimer.Restart();
            this.prefetchTask = this.statement.Connection.Client!.FetchResults(request, cancellationToken);
        }

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                var nextBatch = await TryReadFromCurrentReader(cancellationToken);
                if (nextBatch != null)
                {
                    return nextBatch;
                }

                if (ProcessFetchedBatches(cancellationToken))
                {
                    continue;
                }

                if (CheckEndOfData())
                {
                    return null;
                }

                if (this.statement == null)
                {
                    return null;
                }

                await FetchNextBatchAsync(cancellationToken);
            }
        }

        private async Task<RecordBatch?> TryReadFromCurrentReader(CancellationToken cancellationToken)
        {
            if (this.reader != null)
            {
                var nextBatch = await this.reader.ReadNextRecordBatchAsync(cancellationToken);
                if (nextBatch != null)
                {
                    this.totalRowsRead += nextBatch.Length;
                    return nextBatch;
                }
                //this.reader.Dispose();
                this.reader = null;
            }
            return null;
        }

        private bool ProcessFetchedBatches(CancellationToken cancellationToken)
        {
            if (this.batches != null && this.index < this.batches.Count)
            {
                // If we're at the start of processing a new set of batches, and no prefetch is in progress,
                // and we haven't reached the end of data, start prefetching the next batch.
                if (this.index == 0 && this.prefetchTask == null && !this.isEndOfData)
                {
                    StartPrefetch(cancellationToken);
                }

                var batch = this.batches[this.index++];
                try
                {
                    // Create a new ArrowStreamReader for the current batch.
                    this.reader = new ArrowStreamReader(new ChunkStream(this.schema, batch.Batch));
                    return true;
                }
                catch (Exception ex)
                {
                    // Handle any exceptions that occur while creating the reader.
                    throw new AdbcException($"Error creating reader for batch: {ex.Message}", ex);
                }
            }
            // Reset batches and index if no more batches are available.
            this.batches = null;
            this.index = 0;
            return false;
        }

        private bool CheckEndOfData()
        {
            if (this.isEndOfData)
            {
                return true;
            }
            return false;
        }

        private async Task FetchNextBatchAsync(CancellationToken cancellationToken)
        {
            try
            {
                TFetchResultsResp response;
                if (this.prefetchTask != null)
                {
                    response = await this.prefetchTask;
                    this.prefetchTask = null;
                    this.fetchTimer.Stop();
                }
                else
                {
                    if (this.statement == null)
                    {
                        return;
                    }
                    this.fetchTimer.Restart();
                    TFetchResultsReq request = new TFetchResultsReq(this.statement.OperationHandle, TFetchOrientation.FETCH_NEXT, this.statement.BatchSize);
                    response = await this.statement.Connection.Client!.FetchResults(request, cancellationToken);
                    this.fetchTimer.Stop();
                }

                this.batches = response.Results.ArrowBatches;

                if (!response.HasMoreRows)
                {
                    this.isEndOfData = true;
                    this.statement = null;
                }

                if (this.batches == null || this.batches.Count == 0)
                {
                    this.isEndOfData = true;
                }
            }
            catch (Exception ex)
            {
                this.isEndOfData = true;
                this.statement = null;
                throw new AdbcException($"Error fetching results: {ex.Message}", ex);
            }
        }

        public void Dispose()
        {
            //this.reader?.Dispose();
            this.reader = null;
            this.prefetchTask = null;
            this.batches = null;
            this.statement = null;
        }
    }
}

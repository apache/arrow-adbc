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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    public class SparkStatement : HiveServer2Statement
    {
        private const string NumberOfAffectedRowsColumnName = "num_affected_rows";
        private const string NumberOfInsertedRowsColumnName = "num_inserted_rows";

        internal SparkStatement(SparkConnection connection)
            : base(connection)
        {
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            statement.EnforceResultPersistenceMode = false;
            statement.ResultPersistenceMode = 2;

            statement.CanReadArrowResult = true;
            statement.CanDownloadResult = true;
            statement.ConfOverlay = SparkConnection.timestampConfig;
            statement.UseArrowNativeTypes = new TSparkArrowTypes
            {
                TimestampAsArrow = true,
                DecimalAsArrow = true,
                ComplexTypesAsArrow = true,
                IntervalTypesAsArrow = false,
            };
        }

        public override QueryResult ExecuteQuery()
        {
            ExecuteStatement();
            PollForResponse();
            Schema schema = GetSchema();

            return new QueryResult(-1, new SparkReader(this, schema));
            //return new QueryResult(-1, new CloudFetchReader(this, schema));
        }

        public override UpdateResult ExecuteUpdate()
        {

            QueryResult queryResult = ExecuteQuery();
            using IArrowArrayStream stream = queryResult.Stream;

            // Check if the affected/inserted rows columns are returned in the result.
            List<string> columnsToCheck = new();
            Field affectedRowsField = stream.Schema.GetFieldByName(NumberOfAffectedRowsColumnName);
            if (affectedRowsField != null)
            {
                if (affectedRowsField.DataType.TypeId != Types.ArrowTypeId.Int64)
                {
                    throw new AdbcException($"Unexpected data type for column: '{NumberOfAffectedRowsColumnName}'", new ArgumentException(NumberOfAffectedRowsColumnName));
                }
                columnsToCheck.Add(NumberOfAffectedRowsColumnName);
            }

            Field insertedRowsField = stream.Schema.GetFieldByName(NumberOfInsertedRowsColumnName);
            if (insertedRowsField != null)
            {
                if (insertedRowsField.DataType.TypeId != Types.ArrowTypeId.Int64)
                {
                    throw new AdbcException($"Unexpected data type for column: '{NumberOfInsertedRowsColumnName}'", new ArgumentException(NumberOfInsertedRowsColumnName));
                }
                columnsToCheck.Add(NumberOfInsertedRowsColumnName);
            }

            // If no altered rows, i.e. DDC statements, then -1 is the default.
            long affectedRows = -1;
            while (true)
            {
                using RecordBatch nextBatch = stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                foreach (var columnName in columnsToCheck)
                {
                    Int64Array numOfModifiedArray = (Int64Array)nextBatch.Column(columnName);
                    // Note: should only have one item, but iterate for completeness
                    for (int i = 0; i < numOfModifiedArray.Length; i++)
                    {
                        affectedRows += numOfModifiedArray.GetValue(i).GetValueOrDefault(0);
                    }

                }
            }

            return new UpdateResult(affectedRows);
        }

        public override object GetValue(IArrowArray arrowArray, int index)
        {
            return base.GetValue(arrowArray, index);
        }

        sealed class SparkReader : IArrowArrayStream
        {
            SparkStatement statement;
            Schema schema;
            List<TSparkArrowBatch> batches;
            int index;
            IArrowReader reader;

            public SparkReader(SparkStatement statement, Schema schema)
            {
                this.statement = statement;
                this.schema = schema;
            }

            public Schema Schema { get { return schema; } }

            public async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                while (true)
                {
                    if (this.reader != null)
                    {
                        RecordBatch next = await this.reader.ReadNextRecordBatchAsync(cancellationToken);
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

                    TFetchResultsReq request = new TFetchResultsReq(this.statement.operationHandle, TFetchOrientation.FETCH_NEXT, 50000);
                    TFetchResultsResp response = await this.statement.connection.client.FetchResults(request, cancellationToken);
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


        sealed class CloudFetchReader : IArrowArrayStream
        {
            SparkStatement statement;
            Schema schema;
            ChunkDownloader chunkDownloader;
            IArrowReader reader;

            public CloudFetchReader(SparkStatement statement, Schema schema)
            {
                this.statement = statement;
                this.schema = schema;
                TFetchResultsReq request = new TFetchResultsReq(this.statement.operationHandle, TFetchOrientation.FETCH_NEXT, 500000);
                TFetchResultsResp response = this.statement.connection.client.FetchResults(request, cancellationToken: default).Result;
                this.chunkDownloader = new ChunkDownloader(response.Results.ResultLinks);
            }

            public Schema Schema { get { return schema; } }

            public async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                while (true)
                {
                    if (this.reader != null)
                    {
                        RecordBatch next = await this.reader.ReadNextRecordBatchAsync(cancellationToken);
                        if (next != null)
                        {
                            return next;
                        }
                        this.reader = null;
                        if (this.chunkDownloader.currentChunkIndex >= this.chunkDownloader.chunks.Count)
                        {
                            this.statement = null;
                        }
                    }

                    if (this.statement == null)
                    {
                        return null;
                    }

                    if (this.reader == null)
                    {
                        var currentChunk = this.chunkDownloader.chunks[this.chunkDownloader.currentChunkIndex];
                        while (!currentChunk.isDownloaded)
                        {
                            Thread.Sleep(500);
                        }
                        this.chunkDownloader.currentChunkIndex++;
                        this.reader = currentChunk.reader;
                    }
                }
            }

            public void Dispose()
            {
            }
        }
    }

    internal class ChunkDownloader
    {
        public Dictionary<int, Chunk> chunks;

        public int currentChunkIndex = 0;
        HttpClient client;

        public ChunkDownloader(List<TSparkArrowResultLink> links)
        {
            this.chunks = new Dictionary<int, Chunk>();
            for (int i = 0; i < links.Count; i++)
            {
                var currentChunk = new Chunk(i, links[i].FileLink);
                this.chunks.Add(i, currentChunk);
            }
            this.client = new HttpClient();
            initialize();
        }

        public ChunkDownloader(Dictionary<string, Dictionary<string, string>> links)
        {
            //this.links = links;
            this.client = new HttpClient();
        }

        void initialize()
        {
            int workerThreads, completionPortThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            ThreadPool.SetMinThreads(5, completionPortThreads);
            ThreadPool.SetMaxThreads(10, completionPortThreads);
            foreach (KeyValuePair<int, Chunk> chunk in chunks)
            {
                ThreadPool.QueueUserWorkItem(async _ =>
                {
                    try
                    {
                        await chunk.Value.downloadData(this.client);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                });

            }

        }


    }

    public class Chunk
    {
        int chunkId;
        string chunkUrl;
        Dictionary<string, string> headers;
        public bool isDownloaded = false;
        public bool isFailed = false;
        public IArrowReader reader;

        public Chunk(int chunkId, string chunkUrl) : this(chunkId, chunkUrl, new Dictionary<string, string>())
        {

        }

        public Chunk(int chunkId, string chunkUrl, Dictionary<string, string> headers)
        {
            this.chunkId = chunkId;
            this.chunkUrl = chunkUrl;
            this.headers = headers;
            this.reader = null;
        }

        public async Task downloadData(HttpClient client)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, chunkUrl);
            foreach (KeyValuePair<string, string> pair in headers)
            {
                request.Headers.Add(pair.Key, pair.Value);
            }
            HttpResponseMessage response = await client.SendAsync(request);
            this.reader = new ArrowStreamReader(response.Content.ReadAsStreamAsync().Result);
            isDownloaded = true;
        }
    }
}

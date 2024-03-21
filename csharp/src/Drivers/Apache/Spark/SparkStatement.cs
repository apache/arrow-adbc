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
        internal SparkStatement(SparkConnection connection)
            : base(connection)
        {
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            // TODO: Ensure this is set dynamically depending on server capabilities.
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

            // TODO: Ensure this is set dynamically based on server capabilities
            return new QueryResult(-1, new SparkReader(this, schema));
            //return new QueryResult(-1, new CloudFetchReader(this, schema));
        }

        public override UpdateResult ExecuteUpdate()
        {
            const string NumberOfAffectedRowsColumnName = "num_affected_rows";

            QueryResult queryResult = ExecuteQuery();
            using IArrowArrayStream stream = queryResult.Stream;

            // Check if the affected rows columns are returned in the result.
            Field affectedRowsField = stream.Schema.GetFieldByName(NumberOfAffectedRowsColumnName);
            if (affectedRowsField != null && affectedRowsField.DataType.TypeId != Types.ArrowTypeId.Int64)
            {
                throw new AdbcException($"Unexpected data type for column: '{NumberOfAffectedRowsColumnName}'", new ArgumentException(NumberOfAffectedRowsColumnName));
            }

            long affectedRows = 0;
            while (true)
            {
                using RecordBatch nextBatch = stream.ReadNextRecordBatchAsync().Result;
                if (nextBatch == null) { break; }
                Int64Array numOfModifiedArray = (Int64Array)nextBatch.Column(NumberOfAffectedRowsColumnName);
                // Note: should only have one item, but iterate for completeness
                for (int i = 0; i < numOfModifiedArray.Length; i++)
                {
                    affectedRows += numOfModifiedArray.GetValue(i).GetValueOrDefault(0);
                }
            }

            // If no altered rows, i.e. DDC statements, then -1 is the default.
            return new UpdateResult(affectedRows == 0 ? -1 : affectedRows);
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
            SparkStatement _statement;
            Schema _schema;
            ChunkDownloader _chunkDownloader;
            IArrowReader _reader;

            public CloudFetchReader(SparkStatement statement, Schema schema)
            {
                _statement = statement;
                _schema = schema;
                TFetchResultsReq request = new TFetchResultsReq(_statement.operationHandle, TFetchOrientation.FETCH_NEXT, 500000);
                TFetchResultsResp response = this._statement.connection.client.FetchResults(request, cancellationToken: default).Result;
                _chunkDownloader = new ChunkDownloader(response.Results.ResultLinks);
            }

            public Schema Schema { get { return _schema; } }

            public async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                while (true)
                {
                    if (_reader != null)
                    {
                        RecordBatch next = await _reader.ReadNextRecordBatchAsync(cancellationToken);
                        if (next != null)
                        {
                            return next;
                        }
                        _reader = null;
                        if (_chunkDownloader.currentChunkIndex >= _chunkDownloader.chunks.Count)
                        {
                            _statement = null;
                        }
                    }

                    if (_statement == null)
                    {
                        return null;
                    }

                    if (_reader == null)
                    {
                        var currentChunk = _chunkDownloader.chunks[_chunkDownloader.currentChunkIndex];
                        while (!currentChunk.isDownloaded)
                        {
                            await Task.Delay(500, cancellationToken);
                        }
                        _chunkDownloader.currentChunkIndex++;
                        _reader = currentChunk.reader;
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

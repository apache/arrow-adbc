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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public abstract class HiveServer2Connection : AdbcConnection
    {
        const string userAgent = "AdbcExperimental/0.0";

        protected TOperationHandle operationHandle;
        protected IReadOnlyDictionary<string, string> properties;
        internal TTransport transport;
        internal TCLIService.Client client;
        internal TSessionHandle sessionHandle;

        internal HiveServer2Connection() : this(null)
        {

        }

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public void Open()
        {
            TProtocol protocol = CreateProtocol();
            this.transport = protocol.Transport;
            this.client = new TCLIService.Client(protocol);

            var s0 = this.client.OpenSession(CreateSessionRequest()).Result;
            this.sessionHandle = s0.SessionHandle;
        }

        protected abstract TProtocol CreateProtocol();
        protected abstract TOpenSessionReq CreateSessionRequest();

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string catalogPattern, string dbSchemaPattern, string tableNamePattern, List<string> tableTypes, string columnNamePattern)
        {
            Dictionary<string, Dictionary<string, Dictionary<string, List<string>>>> catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, List<string>>>>();
            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
            {
                throw new NotImplementedException($"Unsupported depth: {nameof(GetObjectsDepth.Catalogs)}");
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.DbSchemas)
            {
                throw new NotImplementedException($"Unsupported depth: {nameof(GetObjectsDepth.DbSchemas)}");
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Tables)
            {
                throw new NotImplementedException($"Unsupported depth: {nameof(GetObjectsDepth.Tables)}");
            }

            if (depth == GetObjectsDepth.All)
            {
                TGetColumnsReq columnsReq = new TGetColumnsReq(this.sessionHandle);
                columnsReq.CatalogName = catalogPattern;
                columnsReq.SchemaName = dbSchemaPattern;
                columnsReq.TableName = tableNamePattern;

                if (!string.IsNullOrEmpty(columnNamePattern))
                    columnsReq.ColumnName = columnNamePattern;

                var columnsResponse = this.client.GetColumns(columnsReq).Result;
                if (columnsResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new HiveServer2Exception(columnsResponse.Status.ErrorMessage)
                        .SetSqlState(columnsResponse.Status.SqlState)
                        .SetNativeError(columnsResponse.Status.ErrorCode);
                }

                this.operationHandle = columnsResponse.OperationHandle;
            }

            PollForResponse();

            Schema schema = GetSchema();

            return new GetObjectsReader(this, schema);
        }

        public override IArrowArrayStream GetInfo(List<int> codes)
        {
            throw new NotImplementedException();
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new NotImplementedException();
        }

        protected void PollForResponse()
        {
            TGetOperationStatusResp statusResponse = null;
            do
            {
                if (statusResponse != null) { Thread.Sleep(500); }
                TGetOperationStatusReq request = new TGetOperationStatusReq(this.operationHandle);
                statusResponse = this.client.GetOperationStatus(request).Result;
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
        }


        public override void Dispose()
        {
            if (this.client != null)
            {
                TCloseSessionReq r6 = new TCloseSessionReq(this.sessionHandle);
                this.client.CloseSession(r6).Wait();

                this.transport.Close();
                this.client.Dispose();
                this.transport = null;
                this.client = null;
            }
        }

        protected Schema GetSchema()
        {
            TGetResultSetMetadataReq request = new TGetResultSetMetadataReq(this.operationHandle);
            TGetResultSetMetadataResp response = this.client.GetResultSetMetadata(request).Result;
            return SchemaParser.GetArrowSchema(response.Schema);
        }

        sealed class GetObjectsReader : IArrowArrayStream
        {
            HiveServer2Connection connection;
            Schema schema;
            List<TSparkArrowBatch> batches;
            int index;
            IArrowReader reader;

            public GetObjectsReader(HiveServer2Connection connection, Schema schema)
            {
                this.connection = connection;
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

                    if (this.connection == null)
                    {
                        return null;
                    }

                    TFetchResultsReq request = new TFetchResultsReq(this.connection.operationHandle, TFetchOrientation.FETCH_NEXT, 50000);
                    TFetchResultsResp response = await this.connection.client.FetchResults(request, cancellationToken);
                    this.batches = response.Results.ArrowBatches;

                    if (!response.HasMoreRows)
                    {
                        this.connection = null;
                    }
                }
            }

            public void Dispose()
            {
            }
        }
    }
}

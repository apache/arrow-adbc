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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    public class ImpalaStatement : HiveServer2Statement
    {
        internal ImpalaStatement(ImpalaConnection connection)
            : base(connection)
        {
        }

        public override QueryResult ExecuteQuery()
        {
            ExecuteStatement();
            PollForResponse();

            Schema schema = GetSchema();

            return new QueryResult(-1, new HiveServer2Reader(this, schema));
        }

        public override UpdateResult ExecuteUpdate()
        {
            throw new NotImplementedException();
        }

        public override object GetValue(IArrowArray arrowArray, int index)
        {
            throw new NotSupportedException();
        }

        class HiveServer2Reader : IArrowArrayStream
        {
            ImpalaStatement? statement;
            Schema schema;
            int counter;

            public HiveServer2Reader(ImpalaStatement statement, Schema schema)
            {
                this.statement = statement;
                this.schema = schema;
            }

            public Schema Schema { get { return schema; } }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (this.statement == null)
                {
                    return new ValueTask<RecordBatch?>((RecordBatch?)null);
                }

                TFetchResultsReq request = new TFetchResultsReq(this.statement.operationHandle, TFetchOrientation.FETCH_NEXT, 50000);
                TFetchResultsResp response = this.statement.connection.Client.FetchResults(request).Result;

                var buffer = new System.IO.MemoryStream();
                response.WriteAsync(new TBinaryProtocol(new TStreamTransport(null, buffer, new TConfiguration())), cancellationToken).Wait();
                System.IO.File.WriteAllBytes(string.Format("d:/src/buffer{0}.bin", this.counter++), buffer.ToArray());

                RecordBatch result = new RecordBatch(this.schema, response.Results.Columns.Select(GetArray), GetArray(response.Results.Columns[0]).Length);

                if (!response.HasMoreRows)
                {
                    this.statement = null;
                }

                return new ValueTask<RecordBatch?>(result);
            }

            public void Dispose()
            {
            }

            static IArrowArray GetArray(TColumn column)
            {
                return
                    (IArrowArray?)column.BoolVal?.Values ??
                    (IArrowArray?)column.ByteVal?.Values ??
                    (IArrowArray?)column.I16Val?.Values ??
                    (IArrowArray?)column.I32Val?.Values ??
                    (IArrowArray?)column.I64Val?.Values ??
                    (IArrowArray?)column.DoubleVal?.Values ??
                    (IArrowArray?)column.StringVal?.Values ??
                    (IArrowArray?)column.BinaryVal?.Values ??
                    throw new InvalidOperationException("unsupported data type");
            }
        }
    }
}

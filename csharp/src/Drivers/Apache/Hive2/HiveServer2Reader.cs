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
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport.Client;
using Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2Reader : IArrowArrayStream
    {
        HiveServer2Statement? _statement;
        private readonly long _batchSize;
        //int _counter;

        public HiveServer2Reader(HiveServer2Statement statement, Schema schema, long batchSize = HiveServer2Connection.BatchSizeDefault)
        {
            _statement = statement;
            Schema = schema;
            _batchSize = batchSize;
        }

        public Schema Schema { get; }

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_statement == null)
            {
                return null;
            }

            TFetchResultsReq request = new TFetchResultsReq(_statement.operationHandle, TFetchOrientation.FETCH_NEXT, _batchSize);
            TFetchResultsResp response = await _statement.connection.Client.FetchResults(request, cancellationToken);

            //var buffer = new System.IO.MemoryStream();
            //await response.WriteAsync(new TBinaryProtocol(new TStreamTransport(null, buffer, new TConfiguration())), cancellationToken);
            //System.IO.File.WriteAllBytes(string.Format("d:/src/buffer{0}.bin", this.counter++), buffer.ToArray());

            int length = response.Results.Columns.Count > 0 ? GetArray(response.Results.Columns[0]).Length : 0;
            RecordBatch result = new(
                Schema,
                response.Results.Columns.Select(GetArray),
                length);

            if (!response.HasMoreRows)
            {
                _statement = null;
            }

            return result;
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

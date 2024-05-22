﻿/*
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

                // set to false so they return as string
                // otherwise, they return as ARRAY_TYPE but you can't determine
                // the object type of the items in the array
                ComplexTypesAsArrow = false,
                IntervalTypesAsArrow = false,
            };
        }

        protected override IArrowArrayStream NewReader<T>(T statement, Schema schema) => new SparkReader(statement, schema);

        /// <summary>
        /// Provides the constant string key values to the <see cref="AdbcStatement.SetOption(string, string)" /> method.
        /// </summary>
        public new sealed class Options : HiveServer2Statement.Options
        {
            // options specific to Spark go here
        }

        sealed class SparkReader : IArrowArrayStream
        {
            HiveServer2Statement? statement;
            Schema schema;
            List<TSparkArrowBatch>? batches;
            int index;
            IArrowReader? reader;

            public SparkReader(HiveServer2Statement statement, Schema schema)
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

                    TFetchResultsReq request = new TFetchResultsReq(this.statement.operationHandle, TFetchOrientation.FETCH_NEXT, this.statement.BatchSize);
                    TFetchResultsResp response = await this.statement.connection.client!.FetchResults(request, cancellationToken);
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
}

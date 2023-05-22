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

using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.FlightSql
{
    /// <summary>
    /// A Flight SQL implementation of <see cref="IArrowArrayStream"/>.
    /// </summary>
    internal class FlightSqlResult : IArrowArrayStream
    {
        private FlightClientRecordBatchStreamReader recordBatchStreamReader;

        private readonly FlightInfo flightInfo;
        private readonly FlightSqlConnection flightSqlConnection;
        private readonly int maxEndPoints = 0;
        private int currentEndPointIndex = -1;

        public FlightSqlResult(FlightSqlConnection flightSqlConnection, FlightInfo flightInfo)
        {
            this.flightSqlConnection = flightSqlConnection;
            this.flightInfo = flightInfo;
            this.maxEndPoints = flightInfo.Endpoints.Count;
        }

        public Schema Schema { get { return this.flightInfo.Schema; } }

        public async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if(this.recordBatchStreamReader is null)
            {
                RefreshRecordBatchStreamReader();
            }

            if (await this.recordBatchStreamReader.MoveNext(cancellationToken))
            {
                return this.recordBatchStreamReader.Current;
            }
            else if (this.currentEndPointIndex < this.maxEndPoints - 1)
            {
                this.recordBatchStreamReader = default;

                return await ReadNextRecordBatchAsync(cancellationToken);
            }
            return default;
        }

        public void Dispose()
        {
            this.recordBatchStreamReader?.Dispose();
            this.flightSqlConnection.Dispose();
        }

        private void RefreshRecordBatchStreamReader()
        {
            this.currentEndPointIndex += 1;
            this.recordBatchStreamReader = this.flightSqlConnection.FlightClient.GetStream(this.flightInfo.Endpoints[currentEndPointIndex].Ticket, this.flightSqlConnection.Metadata).ResponseStream;
        }
    }
}

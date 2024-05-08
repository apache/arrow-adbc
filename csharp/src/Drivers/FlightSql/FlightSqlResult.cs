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

namespace Apache.Arrow.Adbc.Drivers.FlightSql
{
    /// <summary>
    /// A Flight SQL implementation of <see cref="IArrowArrayStream"/>.
    /// </summary>
    internal class FlightSqlResult : IArrowArrayStream
    {
        private FlightClientRecordBatchStreamReader? _recordBatchStreamReader;

        private readonly FlightInfo _flightInfo;
        private readonly FlightSqlConnection _flightSqlConnection;
        private readonly int _maxEndPoints = 0;
        private int _currentEndPointIndex = -1;

        public FlightSqlResult(FlightSqlConnection flightSqlConnection, FlightInfo flightInfo)
        {
            _flightSqlConnection = flightSqlConnection;
            _flightInfo = flightInfo;
            _maxEndPoints = flightInfo.Endpoints.Count;
        }

        public Schema Schema { get { return _flightInfo.Schema; } }

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_recordBatchStreamReader is null)
            {
                RefreshRecordBatchStreamReader();
            }

            if (await _recordBatchStreamReader!.MoveNext(cancellationToken))
            {
                return _recordBatchStreamReader.Current;
            }
            else if (_currentEndPointIndex < _maxEndPoints - 1)
            {
                _recordBatchStreamReader = default;

                return await ReadNextRecordBatchAsync(cancellationToken);
            }
            return default;
        }

        public void Dispose()
        {
            _recordBatchStreamReader?.Dispose();
            _flightSqlConnection.Dispose();
        }

        private void RefreshRecordBatchStreamReader()
        {
            _currentEndPointIndex += 1;
            _recordBatchStreamReader = _flightSqlConnection.FlightClient.GetStream(_flightInfo.Endpoints[_currentEndPointIndex].Ticket, _flightSqlConnection.Metadata).ResponseStream;
        }
    }
}

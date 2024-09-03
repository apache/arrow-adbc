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
using System.Threading.Tasks;
using Apache.Arrow.Flight;
using Grpc.Core;

namespace Apache.Arrow.Adbc.Drivers.FlightSql
{
    /// <summary>
    /// A Flight SQL implementation of <see cref="AdbcStatement"/>.
    /// </summary>
    public class FlightSqlStatement : AdbcStatement
    {
        private FlightSqlConnection _flightSqlConnection;

        public FlightSqlStatement(FlightSqlConnection flightSqlConnection)
        {
            _flightSqlConnection = flightSqlConnection;
        }

        public override async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            if (SqlQuery == null)
                throw new ArgumentNullException(nameof(SqlQuery));

            FlightInfo info = await GetInfo(SqlQuery, _flightSqlConnection.Metadata);

            return new QueryResult(info.TotalRecords, new FlightSqlResult(_flightSqlConnection, info));
        }

        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryAsync().Result;
        }

        public override UpdateResult ExecuteUpdate()
        {
            throw new NotImplementedException();
        }

        public async ValueTask<FlightInfo> GetInfo(string query, Metadata headers)
        {
            if (_flightSqlConnection.FlightClient == null)
                throw new ArgumentNullException(nameof(_flightSqlConnection.FlightClient));

            FlightDescriptor commandDescripter = FlightDescriptor.CreateCommandDescriptor(query);

            return await _flightSqlConnection.FlightClient.GetInfo(commandDescripter, headers).ResponseAsync;
        }
    }
}

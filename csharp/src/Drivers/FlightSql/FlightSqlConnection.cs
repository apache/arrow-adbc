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
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Ipc;
using Grpc.Core;
using Grpc.Net.Client;

namespace Apache.Arrow.Adbc.Drivers.FlightSql
{
    /// <summary>
    /// A Flight SQL implementation of <see cref="AdbcConnection"/>.
    /// </summary>
    public class FlightSqlConnection : AdbcConnection
    {
        private FlightClient? _flightClientInternal = null;
        private readonly IReadOnlyDictionary<string, string>? _metadata;

        private Metadata? headers = null;

        public FlightSqlConnection() : this(null)
        {

        }

        public FlightSqlConnection(IReadOnlyDictionary<string, string>? metadata)
        {
            _metadata = metadata;
        }

        internal FlightClient FlightClient
        {
            get => _flightClientInternal ?? throw new ArgumentNullException(nameof(FlightClient));
        }

        internal Metadata Metadata
        {
            get => GetMetaData();
        }

        private Metadata GetMetaData()
        {
            if (headers is null)
            {
                headers = new Metadata();

                if (_metadata is not null)
                {
                    foreach (KeyValuePair<string, string> pair in _metadata)
                    {
                        headers.Add(pair.Key, pair.Value);
                    }
                }
            }

            return headers;
        }

        public void Open(string uri)
        {
#if NETSTANDARD // and Win11 or later --> https://learn.microsoft.com/en-us/aspnet/core/grpc/netstandard?view=aspnetcore-7.0#net-framework

                var channel = GrpcChannel.ForAddress(uri, new GrpcChannelOptions
                    {
                        HttpHandler = new System.Net.Http.WinHttpHandler()
                        {
                            ReceiveDataTimeout = TimeSpan.FromMinutes(5),
                            SendTimeout = TimeSpan.FromMinutes(5),
                            ReceiveHeadersTimeout = TimeSpan.FromMinutes(5)
                        },

                    });

                _flightClientInternal = new FlightClient(channel);
#else
            _flightClientInternal = new FlightClient(GrpcChannel.ForAddress(uri));
#endif
        }

        public override AdbcStatement CreateStatement()
        {
            return new FlightSqlStatement(this);
        }

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern) => throw new NotImplementedException();
        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName) => throw new NotImplementedException();
        public override IArrowArrayStream GetTableTypes() => throw new NotImplementedException();
    }
}

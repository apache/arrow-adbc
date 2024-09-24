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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal class ImpalaConnection : HiveServer2Connection
    {
        // https://impala.apache.org/docs/build/html/topics/impala_ports.html
        // https://impala.apache.org/docs/build/html/topics/impala_client.html
        private const int DefaultSocketTransportPort = 21050;
        private const int DefaultHttpTransportPort = 28000;

        internal ImpalaConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
        }

        protected override Task<TTransport> CreateTransportAsync()
        {
            string hostName = Properties["HostName"];
            string? tmp;
            int port = DefaultSocketTransportPort; // default?
            if (Properties.TryGetValue("Port", out tmp))
            {
                port = int.Parse(tmp);
            }

            TConfiguration config = new TConfiguration();
            TTransport transport = new ThriftSocketTransport(hostName, port, config);
            return Task.FromResult(transport);
        }

        protected override Task<TProtocol> CreateProtocolAsync(TTransport transport)
        {
            return Task.FromResult<TProtocol>(new TBinaryProtocol(transport));
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            return new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7)
            {
                CanUseMultipleCatalogs = true,
            };
        }

        public override AdbcStatement CreateStatement()
        {
            return new ImpalaStatement(this);
        }

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            throw new System.NotImplementedException();
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new System.NotImplementedException();
        }

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName) => throw new System.NotImplementedException();

        internal override SchemaParser SchemaParser => throw new NotImplementedException();

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema) => throw new NotImplementedException();
    }
}

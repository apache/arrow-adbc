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
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkStandardConnection : SparkHttpConnection
    {
        public SparkStandardConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        protected override Task<TTransport> CreateTransportAsync()
        {
            // Assumption: hostName and port have already been validated.
            Properties.TryGetValue(SparkParameters.HostName, out string? hostName);
            Properties.TryGetValue(SparkParameters.Port, out string? port);

            // Delay the open connection until later.
            bool connectClient = false;
            ThriftSocketTransport transport = new(hostName!, int.Parse(port!), connectClient, config: new());
            return Task.FromResult<TTransport>(transport);
        }

        protected override async Task<TProtocol> CreateProtocolAsync(TTransport transport)
        {
            return await base.CreateProtocolAsync(transport);

            //Trace.TraceError($"create protocol with {Properties.Count} properties.");
            //if (!transport.IsOpen) await transport.OpenAsync(CancellationToken.None);
            //return new TBinaryProtocol(transport);
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            // Assumption: user name and password have already been validated.
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            TOpenSessionReq request = base.CreateSessionRequest();
            request.Username = username!;
            request.Password = password!;
            return request;
        }
        public override SparkServerType ServerType => SparkServerType.Standard;
    }
}

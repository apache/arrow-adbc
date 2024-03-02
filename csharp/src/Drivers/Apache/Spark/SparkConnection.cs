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
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    public class SparkConnection : HiveServer2Connection
    {
        const string userAgent = "AdbcExperimental/0.0";

        internal static readonly Dictionary<string, string> timestampConfig = new Dictionary<string, string>
        {
            { "spark.thriftserver.arrowBasedRowSet.timestampAsString", "false" }
        };

        public SparkConnection() : this(null)
        {

        }

        internal SparkConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
        }

        protected override TProtocol CreateProtocol()
        {
            string hostName = properties["HostName"];
            string path = properties["Path"];
            string token = properties["Token"];

            string uri = "https://" + hostName + "/" + path;

            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(uri);
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
            httpClient.DefaultRequestHeaders.AcceptEncoding.Clear();
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
            httpClient.DefaultRequestHeaders.ExpectContinue = false;

            TConfiguration config = new TConfiguration();

            THttpTransport transport = new THttpTransport(httpClient, config);
            // can switch to the one below if want to use the experimental one with IPeekableTransport
            // ThriftHttpTransport transport = new ThriftHttpTransport(httpClient, config);
            transport.OpenAsync(CancellationToken.None).Wait();
            return new TBinaryProtocol(transport);
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            return new TOpenSessionReq(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7)
            {
                CanUseMultipleCatalogs = true,
                Configuration = timestampConfig,
            };
        }

        public override AdbcStatement CreateStatement()
        {
            return new SparkStatement(this);
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

        public override Schema GetTableSchema(string catalog, string dbSchema, string tableName) => throw new NotImplementedException();
    }
}

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using Apache.Arrow.Adbc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public abstract class HiveServer2Connection : AdbcConnection
    {
        const string userAgent = "PowerBiExperimental/0.0";

        protected IReadOnlyDictionary<string, string> properties;
        internal TTransport transport;
        internal TCLIService.Client client;
        internal TSessionHandle sessionHandle;

        internal HiveServer2Connection() : this(null)
        {
            
        }

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public void Open()
        {
            TProtocol protocol = CreateProtocol();
            this.transport = protocol.Transport;
            this.client = new TCLIService.Client(protocol);

            var s0 = this.client.OpenSession(CreateSessionRequest()).Result;
            this.sessionHandle = s0.SessionHandle;
        }

        protected abstract TProtocol CreateProtocol();
        protected abstract TOpenSessionReq CreateSessionRequest();

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
    }
}

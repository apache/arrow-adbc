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
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading;
using Thrift;
using Thrift.Transport;
using Thrift.Transport.Client;
using System.Reflection;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    class ThriftSocketTransport : TSocketTransport, IPeekableTransport
    {
        public ThriftSocketTransport(string host, int port, TConfiguration config, int timeout = 0)
            : base(host, port, config, timeout)
        {
        }

        public Stream Input { get { return this.InputStream; } }
        public Stream Output { get { return this.OutputStream; } }
    }

    // TODO: Experimental
    class ThriftHttpTransport : THttpTransport, IPeekableTransport
    {
        public ThriftHttpTransport(HttpClient httpClient, TConfiguration config)
            : base(httpClient, config)
        {

        }

        public Stream Input
        {
            get
            {
                // not advocating for this, but it works
                Stream stream = ((FieldInfo[])((TypeInfo)this.GetType().BaseType).DeclaredFields)[4].GetValue(this) as Stream;
                return stream;
            }
        }
        public Stream Output
        {
            get
            {
                Stream stream = this.GetType().BaseType.GetField("_outputStream", BindingFlags.NonPublic).GetValue(this) as Stream;
                return stream;
            }
        }
    }
}

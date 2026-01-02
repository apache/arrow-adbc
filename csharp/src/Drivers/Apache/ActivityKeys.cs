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

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    internal static class ActivityKeys
    {
        public const string AuthType = "auth_type";
        public const string Encrypted = "encrypted";
        public const string TransportType = "transport_type";
        public const string Host = "host";
        public const string Port = "port";

        internal static class Http
        {
            public const string Key = "http";
            public const string UserAgent = Key + ".user_agent";
            public const string Uri = Key + ".uri";
            public const string AuthScheme = Key + ".auth_scheme";
        }

        internal static class  Thrift
        {
            public const string Key = "thrift";
            public const string MaxMessageSize = Key + ".max_message_size";
            public const string MaxFrameSize = Key + ".max_frame_size";
        }
    }
}

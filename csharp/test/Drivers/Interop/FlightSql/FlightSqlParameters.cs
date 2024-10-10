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

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.FlightSql
{
    internal class FlightSqlParameters
    {
        public const string Uri = "uri";
        public const string OptionAuthorizationHeader = "adbc.flight.sql.authorization_header";
        public const string OptionRPCCallHeaderPrefix = "adbc.flight.sql.rpc.call_header.";
        public const string OptionTimeoutFetch = "adbc.flight.sql.rpc.timeout_seconds.fetch";
        public const string OptionTimeoutQuery = "adbc.flight.sql.rpc.timeout_seconds.query";
        public const string OptionTimeoutUpdate = "adbc.flight.sql.rpc.timeout_seconds.update";
        public const string OptionSSLSkipVerify = "adbc.flight.sql.client_option.tls_skip_verify";
        public const string OptionAuthority = "adbc.flight.sql.client_option.authority";
        public const string Username = "username";
        public const string Password = "password";

        // not used, but also available:
        //public const string OptionMTLSCertChain = "adbc.flight.sql.client_option.mtls_cert_chain";
        //public const string OptionMTLSPrivateKey = "adbc.flight.sql.client_option.mtls_private_key";
        //public const string OptionSSLOverrideHostname = "adbc.flight.sql.client_option.tls_override_hostname";
        //public const string OptionSSLRootCerts = "adbc.flight.sql.client_option.tls_root_certs";
        //public const string OptionWithBlock = "adbc.flight.sql.client_option.with_block";
        //public const string OptionWithMaxMsgSize = "adbc.flight.sql.client_option.with_max_msg_size";
        //public const string OptionCookieMiddleware = "adbc.flight.sql.rpc.with_cookie_middleware";
    }
}

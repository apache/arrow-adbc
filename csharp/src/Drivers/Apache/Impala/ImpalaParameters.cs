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

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    /// <summary>
    /// Parameters used for connecting to Impala data sources.
    /// </summary>
    public static class ImpalaParameters
    {
        public const string HostName = "adbc.impala.host";
        public const string Port = "adbc.impala.port";
        public const string Path = "adbc.impala.path";
        public const string AuthType = "adbc.impala.auth_type";
        public const string DataTypeConv = "adbc.impala.data_type_conv";
    }

    public static class AuthTypeOptions
    {
        public const string None = "none";
        public const string UsernameOnly = "username_only";
        public const string Basic = "basic";
    }
}

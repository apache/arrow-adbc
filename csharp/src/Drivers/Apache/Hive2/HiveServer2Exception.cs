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

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public class HiveServer2Exception : AdbcException
    {
        private string _sqlState;
        private int _nativeError;

        public HiveServer2Exception()
        {
        }

        public HiveServer2Exception(string message) : base(message)
        {
        }

        public HiveServer2Exception(string message, AdbcStatusCode statusCode) : base(message, statusCode)
        {
        }

        public HiveServer2Exception(string message, Exception innerException) : base(message, innerException)
        {
        }

        public HiveServer2Exception(string message, AdbcStatusCode statusCode, Exception innerException) : base(message, statusCode, innerException)
        {
        }

        public override string SqlState
        {
            get { return _sqlState; }
        }

        public override int NativeError
        {
            get { return _nativeError; }
        }

        internal HiveServer2Exception SetSqlState(string sqlState)
        {
            _sqlState = sqlState;
            return this;
        }

        internal HiveServer2Exception SetNativeError(int nativeError)
        {
            _nativeError = nativeError;
            return this;
        }
    }
}

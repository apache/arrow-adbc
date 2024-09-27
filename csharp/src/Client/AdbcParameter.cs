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

using System.Data.Common;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System;

namespace Apache.Arrow.Adbc.Client
{
    sealed public class AdbcParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction
        {
            get => ParameterDirection.Input;
            set { if (value != ParameterDirection.Input) { throw new NotSupportedException(); } }
        }
        public override bool IsNullable { get; set; } = true;
#if NET5_0_OR_GREATER
        [AllowNull]
#endif
        public override string ParameterName { get; set; } = string.Empty;
        public override int Size { get; set; }
#if NET5_0_OR_GREATER
        [AllowNull]
#endif
        public override string SourceColumn { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public override bool SourceColumnNullMapping { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public override object? Value { get; set; }

        public override void ResetDbType() => throw new NotImplementedException();
    }
}

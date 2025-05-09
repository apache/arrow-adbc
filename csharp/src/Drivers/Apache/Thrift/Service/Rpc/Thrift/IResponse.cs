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

using Thrift.Protocol;

namespace Apache.Hive.Service.Rpc.Thrift
{
    internal interface IResponse
    {
        TStatus? Status { get; set; }
        TOperationHandle? OperationHandle { get; set; }
        TSparkDirectResults? DirectResults { get; set; }
    }

    internal partial class TExecuteStatementResp : TBase, IResponse { }
    internal partial class TGetCatalogsResp : TBase, IResponse { }
    internal partial class TGetColumnsResp : TBase, IResponse { }
    internal partial class TGetCrossReferenceResp : TBase, IResponse { }
    internal partial class TGetPrimaryKeysResp : TBase, IResponse { }
    internal partial class TGetSchemasResp : TBase, IResponse { }
    internal partial class TGetTablesResp : TBase, IResponse { }
    internal partial class TGetTableTypesResp : TBase, IResponse { }
}

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

using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkStatement : HiveServer2Statement
    {
        internal SparkStatement(SparkConnection connection)
            : base(connection)
        {
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            // TODO: Ensure this is set dynamically depending on server capabilities.
            statement.EnforceResultPersistenceMode = false;
            statement.ResultPersistenceMode = TResultPersistenceMode.ALL_RESULTS;
            // This seems like a good idea to have the server timeout so it doesn't keep processing unnecessarily.
            // Set in combination with a CancellationToken.
            statement.QueryTimeout = QueryTimeoutSeconds;
            statement.CanReadArrowResult = true;

#pragma warning disable CS0618 // Type or member is obsolete
            statement.ConfOverlay = SparkConnection.timestampConfig;
#pragma warning restore CS0618 // Type or member is obsolete
            statement.UseArrowNativeTypes = new TSparkArrowTypes
            {
                TimestampAsArrow = true,
                DecimalAsArrow = true,

                // set to false so they return as string
                // otherwise, they return as ARRAY_TYPE but you can't determine
                // the object type of the items in the array
                ComplexTypesAsArrow = false,
                IntervalTypesAsArrow = false,
            };
        }
    }
}

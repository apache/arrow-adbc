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

namespace Apache.Arrow.Adbc.Tracing
{
    public static class SemanticConventions
    {
        public const string Namespace = "db.namespace";

        public static class Db
        {
            public static class Client
            {
                public static class Connection
                {
                    public const string State = "db.client.connection.state";
                    public const string SessionId = "db.client.connection.session_id";
                }
            }

            public static class Collection
            {
                public const string Name = "db.collection.name";
            }

            public static class Operation
            {
                public static string Parameter(string name)
                {
                    return $"db.operation.parameter.{name}";
                }
                public const string OperationId = "db.operation.operation_id";
            }

            public static class Query
            {
                public static string Parameter(string name)
                {
                    return $"db.query.parameter.{name}";
                }
                public const string Summary = "db.query.summary";
                public const string Text = "db.query.text";
            }

            public static class Response
            {
                public const string ReturnedRows = "db.response.returned_rows";
                public const string StatusCode = "db.response.status_code";
                public const string InfoMessages = "db.response.info_messages";
                public const string OperationId = "db.response.operation_id";
            }
        }

        public static class Messaging
        {
            public static class Batch
            {
                public const string Response = "messaging.batch.response";
            }
        }
    }
}

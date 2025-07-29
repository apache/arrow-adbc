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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums
{
    public enum OperationType
    {
        TYPE_UNSPECIFIED = 0,
        CREATE_SESSION = 1,
        DELETE_SESSION = 2,
        EXECUTE_STATEMENT = 3,
        EXECUTE_STATEMENT_ASYNC = 4,
        CLOSE_STATEMENT = 5,
        CANCEL_STATEMENT = 6,
        LIST_TYPE_INFO = 7,
        LIST_CATALOGS = 8,
        LIST_SCHEMAS = 9,
        LIST_TABLES = 10,
        LIST_TABLE_TYPES = 11,
        LIST_COLUMNS = 12,
        LIST_FUNCTIONS = 13,
        LIST_PRIMARY_KEYS = 14,
        LIST_IMPORTED_KEYS = 15,
        LIST_EXPORTED_KEYS = 16,
        LIST_CROSS_REFERENCES = 17
    }
}

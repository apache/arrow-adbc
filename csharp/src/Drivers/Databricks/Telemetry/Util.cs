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
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class Util
    {
        private static string DRIVER_VERSION = "1.0.0";

        private static string DRIVER_NAME = "oss-adbc-driver";
        
        public static string GetDriverVersion()
        {
            return DRIVER_VERSION;
        }

        public static string GetDriverName()
        {
            return DRIVER_NAME;
        }

        public static AuthMech StringToAuthMech(String authType)
        {
            switch (authType)
            {
                case "none":
                    return AuthMech.OTHER;
                case "basic":
                    return AuthMech.OTHER;
                case "token":
                    return AuthMech.PAT;
                case "oauth":
                    return AuthMech.OAUTH;
                default:
                    return AuthMech.TYPE_UNSPECIFIED;
            }
        }

        public static OperationType StringToOperationType(String? operationType)
        {
            switch (operationType)
            {
                case "TYPE_UNSPECIFIED":
                    return OperationType.TYPE_UNSPECIFIED;
                case "CREATE_SESSION":
                    return OperationType.CREATE_SESSION;
                case "DELETE_SESSION":
                    return OperationType.DELETE_SESSION;
                case "EXECUTE_STATEMENT":
                    return OperationType.EXECUTE_STATEMENT;
                case "EXECUTE_STATEMENT_ASYNC":
                    return OperationType.EXECUTE_STATEMENT_ASYNC;
                case "CLOSE_STATEMENT":
                    return OperationType.CLOSE_STATEMENT;
                case "CANCEL_STATEMENT":
                    return OperationType.CANCEL_STATEMENT;
                case "LIST_TYPE_INFO":
                    return OperationType.LIST_TYPE_INFO;
                case "LIST_CATALOGS":
                    return OperationType.LIST_CATALOGS;
                case "LIST_SCHEMAS":
                    return OperationType.LIST_SCHEMAS;
                case "LIST_TABLES":
                    return OperationType.LIST_TABLES;
                case "LIST_TABLE_TYPES":
                    return OperationType.LIST_TABLE_TYPES;
                case "LIST_COLUMNS":
                    return OperationType.LIST_COLUMNS;
                case "LIST_FUNCTIONS":
                    return OperationType.LIST_FUNCTIONS;
                case "LIST_PRIMARY_KEYS":
                    return OperationType.LIST_PRIMARY_KEYS;
                case "LIST_IMPORTED_KEYS":
                    return OperationType.LIST_IMPORTED_KEYS;
                case "LIST_EXPORTED_KEYS":
                    return OperationType.LIST_EXPORTED_KEYS;
                case "LIST_CROSS_REFERENCES":   
                    return OperationType.LIST_CROSS_REFERENCES;
                default:
                    return OperationType.TYPE_UNSPECIFIED;
            }
        }
    }
}
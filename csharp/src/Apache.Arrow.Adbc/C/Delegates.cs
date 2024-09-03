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

using Apache.Arrow.C;

namespace Apache.Arrow.Adbc.C
{
    internal unsafe delegate void ErrorRelease(CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseFn(CAdbcDatabase* database, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionFn(CAdbcConnection* connection, CAdbcError* error);

#if !NET5_0_OR_GREATER
    internal unsafe delegate AdbcStatusCode DriverRelease(CAdbcDriver* driver, CAdbcError* error);
    internal unsafe delegate void PartitionsRelease(CAdbcPartitions* partitions);

    internal unsafe delegate int ErrorGetDetailCount(CAdbcError* error);
    internal unsafe delegate CAdbcErrorDetail ErrorGetDetail(CAdbcError* error, int index);
    internal unsafe delegate CAdbcError* ErrorFromArrayStream(CArrowArrayStream* stream, AdbcStatusCode* status);

    internal unsafe delegate AdbcStatusCode DatabaseGetOption(CAdbcDatabase* database, byte* name, byte* value, nint* length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseGetOptionBytes(CAdbcDatabase* database, byte* name, byte* value, nint* length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseGetOptionDouble(CAdbcDatabase* database, byte* name, double* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseGetOptionInt(CAdbcDatabase* database, byte* name, long* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseSetOption(CAdbcDatabase* database, byte* name, byte* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseSetOptionBytes(CAdbcDatabase* database, byte* name, byte* value, nint length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseSetOptionDouble(CAdbcDatabase* database, byte* name, double value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode DatabaseSetOptionInt(CAdbcDatabase* database, byte* name, long value, CAdbcError* error);

    internal unsafe delegate AdbcStatusCode ConnectionCancel(CAdbcConnection* connection, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionCommit(CAdbcConnection* connection, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetInfo(CAdbcConnection* connection, int* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetObjects(CAdbcConnection* connection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetOption(CAdbcConnection* connection, byte* name, byte* value, nint* length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetOptionBytes(CAdbcConnection* connection, byte* name, byte* value, nint* length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetOptionDouble(CAdbcConnection* connection, byte* name, double* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetOptionInt(CAdbcConnection* connection, byte* name, long* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetStatistics(CAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, byte approximate, CArrowArrayStream* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetStatisticNames(CAdbcConnection* connection, CArrowArrayStream* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetTableSchema(CAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionGetTableTypes(CAdbcConnection* connection, CArrowArrayStream* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionInit(CAdbcConnection* connection, CAdbcDatabase* database, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionReadPartition(CAdbcConnection* connection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionRollback(CAdbcConnection* connection, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionSetOption(CAdbcConnection* connection, byte* name, byte* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionSetOptionBytes(CAdbcConnection* connection, byte* name, byte* value, nint length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionSetOptionDouble(CAdbcConnection* connection, byte* name, double value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode ConnectionSetOptionInt(CAdbcConnection* connection, byte* name, long value, CAdbcError* error);

    internal unsafe delegate AdbcStatusCode StatementBind(CAdbcStatement* statement, CArrowArray* array, CArrowSchema* schema, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementBindStream(CAdbcStatement* statement, CArrowArrayStream* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementCancel(CAdbcStatement* statement, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementExecuteQuery(CAdbcStatement* statement, CArrowArrayStream* stream, long* rows, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementExecutePartitions(CAdbcStatement* statement, CArrowSchema* schema, CAdbcPartitions* partitions, long* rows, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementExecuteSchema(CAdbcStatement* statement, CArrowSchema* stream, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementGetParameterSchema(CAdbcStatement* statement, CArrowSchema* schema, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementGetOption(CAdbcStatement* statement, byte* name, byte* value, nint* length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementGetOptionBytes(CAdbcStatement* statement, byte* name, byte* value, nint* length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementGetOptionDouble(CAdbcStatement* statement, byte* name, double* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementGetOptionInt(CAdbcStatement* statement, byte* name, long* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementNew(CAdbcConnection* connection, CAdbcStatement* statement, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementPrepare(CAdbcStatement* statement, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementSetOption(CAdbcStatement* statement, byte* name, byte* value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementSetOptionBytes(CAdbcStatement* statement, byte* name, byte* value, nint length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementSetOptionDouble(CAdbcStatement* statement, byte* name, double value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementSetOptionInt(CAdbcStatement* statement, byte* name, long value, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementSetSqlQuery(CAdbcStatement* statement, byte* text, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementSetSubstraitPlan(CAdbcStatement* statement, byte* plan, int length, CAdbcError* error);
    internal unsafe delegate AdbcStatusCode StatementRelease(CAdbcStatement* statement, CAdbcError* error);
#endif
}

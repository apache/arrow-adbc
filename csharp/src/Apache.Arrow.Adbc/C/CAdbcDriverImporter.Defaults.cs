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
using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Apache.Arrow.Adbc.C
{
    public static partial class CAdbcDriverImporter
    {
        #region ADBC API Revision 1.0.0

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseSetOptionDefault = NativeDelegate<DatabaseSetOption>.AsNativePointer(DatabaseSetOptionDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> DatabaseSetOptionDefault => &DatabaseSetOptionDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseSetOptionDefaultImpl(CAdbcDatabase* database, byte* key, byte* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseSetOption));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionCommitDefault = NativeDelegate<ConnectionCommit>.AsNativePointer(ConnectionCommitDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionCommitDefault => &ConnectionCommitDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionCommitDefaultImpl(CAdbcConnection* connection, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionCommit));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetInfoDefault = NativeDelegate<ConnectionGetInfo>.AsNativePointer(ConnectionGetInfoDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, int*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetInfoDefault => &ConnectionGetInfoDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetInfoDefaultImpl(CAdbcConnection* connection, int* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetInfo));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetObjectsDefault = NativeDelegate<ConnectionGetObjects>.AsNativePointer(ConnectionGetObjectsDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetObjectsDefault => &ConnectionGetObjectsDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetObjectsDefaultImpl(CAdbcConnection* connection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetObjects));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetTableSchemaDefault = NativeDelegate<ConnectionGetTableSchema>.AsNativePointer(ConnectionGetTableSchemaDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, CArrowSchema*, CAdbcError*, AdbcStatusCode> ConnectionGetTableSchemaDefault => &ConnectionGetTableSchemaDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetTableSchemaDefaultImpl(CAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetTableSchema));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetTableTypesDefault = NativeDelegate<ConnectionGetTableTypes>.AsNativePointer(ConnectionGetTableTypesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetTableTypesDefault => &ConnectionGetTableTypesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetTableTypesDefaultImpl(CAdbcConnection* connection, CArrowArrayStream* stream, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetTableTypes));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionReadPartitionDefault = NativeDelegate<ConnectionReadPartition>.AsNativePointer(ConnectionReadPartitionDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionReadPartitionDefault => &ConnectionReadPartitionDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionReadPartitionDefaultImpl(CAdbcConnection* connection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionReadPartition));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionRollbackDefault = NativeDelegate<ConnectionRollback>.AsNativePointer(ConnectionRollbackDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionRollbackDefault => &ConnectionRollbackDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionRollbackDefaultImpl(CAdbcConnection* connection, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionRollback));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionSetOptionDefault = NativeDelegate<ConnectionSetOption>.AsNativePointer(ConnectionSetOptionDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> ConnectionSetOptionDefault => &ConnectionSetOptionDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionSetOptionDefaultImpl(CAdbcConnection* connection, byte* name, byte* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionSetOption));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementExecutePartitionsDefault = NativeDelegate<StatementExecutePartitions>.AsNativePointer(StatementExecutePartitionsDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcPartitions*, long*, CAdbcError*, AdbcStatusCode> StatementExecutePartitionsDefault => &StatementExecutePartitionsDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementExecutePartitionsDefaultImpl(CAdbcStatement* statement, CArrowSchema* schema, CAdbcPartitions* partitions, long* rows, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementExecutePartitions));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementBindDefault = NativeDelegate<StatementBind>.AsNativePointer(StatementBindDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArray*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementBindDefault => &StatementBindDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementBindDefaultImpl(CAdbcStatement* statement, CArrowArray* array, CArrowSchema* schema, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementBind));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementBindStreamDefault = NativeDelegate<StatementBindStream>.AsNativePointer(StatementBindStreamDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> StatementBindStreamDefault => &StatementBindStreamDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementBindStreamDefaultImpl(CAdbcStatement* statement, CArrowArrayStream* stream, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementBindStream));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementGetParameterSchemaDefault = NativeDelegate<StatementGetParameterSchema>.AsNativePointer(StatementGetParameterSchemaDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementGetParameterSchemaDefault => &StatementGetParameterSchemaDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementGetParameterSchemaDefaultImpl(CAdbcStatement* statement, CArrowSchema* schema, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementGetParameterSchema));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementPrepareDefault = NativeDelegate<StatementPrepare>.AsNativePointer(StatementPrepareDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementPrepareDefault => &StatementPrepareDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementPrepareDefaultImpl(CAdbcStatement* statement, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementPrepare));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementSetOptionDefault = NativeDelegate<StatementSetOption>.AsNativePointer(StatementSetOptionDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, CAdbcError*, AdbcStatusCode> StatementSetOptionDefault => &StatementSetOptionDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementSetOptionDefaultImpl(CAdbcStatement* statement, byte* name, byte* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementSetOption));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementSetSqlQueryDefault = NativeDelegate<StatementSetSqlQuery>.AsNativePointer(StatementSetSqlQueryDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode> StatementSetSqlQueryDefault => &StatementSetSqlQueryDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementSetSqlQueryDefaultImpl(CAdbcStatement* statement, byte* text, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementSetSqlQuery));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementSetSubstraitPlanDefault = NativeDelegate<StatementSetSubstraitPlan>.AsNativePointer(StatementSetSubstraitPlanDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, int, CAdbcError*, AdbcStatusCode> StatementSetSubstraitPlanDefault => &StatementSetSubstraitPlanDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementSetSubstraitPlanDefaultImpl(CAdbcStatement* statement, byte* plan, int length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementSetSubstraitPlan));
        }

        #endregion

        #region ADBC API Revision 1.1.0

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ErrorGetDetailCountDefault = NativeDelegate<ErrorGetDetailCount>.AsNativePointer(ErrorGetDetailCountDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcError*, int> ErrorGetDetailCountDefault => &ErrorGetDetailCountDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe int ErrorGetDetailCountDefaultImpl(CAdbcError* error)
        {
            return 0;
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ErrorGetDetailDefault = NativeDelegate<ErrorGetDetail>.AsNativePointer(ErrorGetDetailDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcError*, int, CAdbcErrorDetail> ErrorGetDetailDefault => &ErrorGetDetailDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe CAdbcErrorDetail ErrorGetDetailDefaultImpl(CAdbcError* error, int index)
        {
            return new CAdbcErrorDetail();
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ErrorFromArrayStreamDefault = NativeDelegate<ErrorFromArrayStream>.AsNativePointer(ErrorFromArrayStreamDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CArrowArrayStream*, AdbcStatusCode*, CAdbcError*> ErrorFromArrayStreamDefault => &ErrorFromArrayStreamDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe CAdbcError* ErrorFromArrayStreamDefaultImpl(CArrowArrayStream* stream, AdbcStatusCode* status)
        {
            return null;
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseGetOptionDefault = NativeDelegate<DatabaseGetOption>.AsNativePointer(DatabaseGetOptionDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionDefault => &DatabaseGetOptionDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseGetOptionDefaultImpl(CAdbcDatabase* database, byte* key, byte* value, nint* length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseGetOption));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseGetOptionBytesDefault = NativeDelegate<DatabaseGetOptionBytes>.AsNativePointer(DatabaseGetOptionBytesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionBytesDefault => &DatabaseGetOptionBytesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseGetOptionBytesDefaultImpl(CAdbcDatabase* database, byte* key, byte* value, nint* length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseGetOptionBytes));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseGetOptionDoubleDefault = NativeDelegate<DatabaseGetOptionDouble>.AsNativePointer(DatabaseGetOptionDoubleDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, double*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionDoubleDefault => &DatabaseGetOptionDoubleDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseGetOptionDoubleDefaultImpl(CAdbcDatabase* database, byte* key, double* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseGetOptionDouble));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseGetOptionIntDefault = NativeDelegate<DatabaseGetOptionInt>.AsNativePointer(DatabaseGetOptionIntDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, long*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionIntDefault => &DatabaseGetOptionIntDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseGetOptionIntDefaultImpl(CAdbcDatabase* database, byte* key, long* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseGetOptionInt));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseSetOptionBytesDefault = NativeDelegate<DatabaseSetOptionBytes>.AsNativePointer(DatabaseSetOptionBytesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> DatabaseSetOptionBytesDefault => &DatabaseSetOptionBytesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseSetOptionBytesDefaultImpl(CAdbcDatabase* database, byte* key, byte* value, nint length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseSetOptionBytes));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseSetOptionDoubleDefault = NativeDelegate<DatabaseSetOptionDouble>.AsNativePointer(DatabaseSetOptionDoubleDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, double, CAdbcError*, AdbcStatusCode> DatabaseSetOptionDoubleDefault => &DatabaseSetOptionDoubleDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseSetOptionDoubleDefaultImpl(CAdbcDatabase* database, byte* key, double value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseSetOptionDouble));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr DatabaseSetOptionIntDefault = NativeDelegate<DatabaseSetOptionInt>.AsNativePointer(DatabaseSetOptionIntDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, long, CAdbcError*, AdbcStatusCode> DatabaseSetOptionIntDefault => &DatabaseSetOptionIntDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode DatabaseSetOptionIntDefaultImpl(CAdbcDatabase* database, byte* key, long value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.DatabaseSetOptionInt));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionCancelDefault = NativeDelegate<ConnectionCancel>.AsNativePointer(ConnectionCancelDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionCancelDefault => &ConnectionCancelDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionCancelDefaultImpl(CAdbcConnection* connection, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionCancel));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetOptionDefault = NativeDelegate<ConnectionGetOption>.AsNativePointer(ConnectionGetOptionDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionDefault => &ConnectionGetOptionDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetOptionDefaultImpl(CAdbcConnection* connection, byte* key, byte* value, nint* length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetOption));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetOptionBytesDefault = NativeDelegate<ConnectionGetOptionBytes>.AsNativePointer(ConnectionGetOptionBytesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionBytesDefault => &ConnectionGetOptionBytesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetOptionBytesDefaultImpl(CAdbcConnection* connection, byte* key, byte* value, nint* length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetOptionBytes));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetOptionDoubleDefault = NativeDelegate<ConnectionGetOptionDouble>.AsNativePointer(ConnectionGetOptionDoubleDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, double*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionDoubleDefault => &ConnectionGetOptionDoubleDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetOptionDoubleDefaultImpl(CAdbcConnection* connection, byte* key, double* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetOptionDouble));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetOptionIntDefault = NativeDelegate<ConnectionGetOptionInt>.AsNativePointer(ConnectionGetOptionIntDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, long*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionIntDefault => &ConnectionGetOptionIntDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetOptionIntDefaultImpl(CAdbcConnection* connection, byte* key, long* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetOptionInt));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetStatisticsDefault = NativeDelegate<ConnectionGetStatistics>.AsNativePointer(ConnectionGetStatisticsDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, byte, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetStatisticsDefault => &ConnectionGetStatisticsDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetStatisticsDefaultImpl(CAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, byte approximate, CArrowArrayStream* stream, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetStatistics));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionGetStatisticNamesDefault = NativeDelegate<ConnectionGetStatisticNames>.AsNativePointer(ConnectionGetStatisticNamesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetStatisticNamesDefault => &ConnectionGetStatisticNamesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionGetStatisticNamesDefaultImpl(CAdbcConnection* connection, CArrowArrayStream* stream, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionGetStatisticNames));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionSetOptionBytesDefault = NativeDelegate<ConnectionSetOptionBytes>.AsNativePointer(ConnectionSetOptionBytesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> ConnectionSetOptionBytesDefault => &ConnectionSetOptionBytesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionSetOptionBytesDefaultImpl(CAdbcConnection* connection, byte* key, byte* value, nint length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionSetOptionBytes));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionSetOptionDoubleDefault = NativeDelegate<ConnectionSetOptionDouble>.AsNativePointer(ConnectionSetOptionDoubleDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, double, CAdbcError*, AdbcStatusCode> ConnectionSetOptionDoubleDefault => &ConnectionSetOptionDoubleDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionSetOptionDoubleDefaultImpl(CAdbcConnection* connection, byte* key, double value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionSetOptionDouble));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr ConnectionSetOptionIntDefault = NativeDelegate<ConnectionSetOptionInt>.AsNativePointer(ConnectionSetOptionIntDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, long, CAdbcError*, AdbcStatusCode> ConnectionSetOptionIntDefault => &ConnectionSetOptionIntDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode ConnectionSetOptionIntDefaultImpl(CAdbcConnection* connection, byte* key, long value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.ConnectionSetOptionInt));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementCancelDefault = NativeDelegate<StatementCancel>.AsNativePointer(StatementCancelDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementCancelDefault => &StatementCancelDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementCancelDefaultImpl(CAdbcStatement* statement, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementCancel));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementExecuteSchemaDefault = NativeDelegate<StatementExecuteSchema>.AsNativePointer(StatementExecuteSchemaDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementExecuteSchemaDefault => &StatementExecuteSchemaDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementExecuteSchemaDefaultImpl(CAdbcStatement* statement, CArrowSchema* schema, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementExecuteSchema));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementGetOptionDefault = NativeDelegate<StatementGetOption>.AsNativePointer(StatementGetOptionDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> StatementGetOptionDefault => &StatementGetOptionDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementGetOptionDefaultImpl(CAdbcStatement* statement, byte* key, byte* value, nint* length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementGetOption));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementGetOptionBytesDefault = NativeDelegate<StatementGetOptionBytes>.AsNativePointer(StatementGetOptionBytesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> StatementGetOptionBytesDefault => &StatementGetOptionBytesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementGetOptionBytesDefaultImpl(CAdbcStatement* statement, byte* key, byte* value, nint* length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementGetOptionBytes));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementGetOptionDoubleDefault = NativeDelegate<StatementGetOptionDouble>.AsNativePointer(StatementGetOptionDoubleDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, double*, CAdbcError*, AdbcStatusCode> StatementGetOptionDoubleDefault => &StatementGetOptionDoubleDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementGetOptionDoubleDefaultImpl(CAdbcStatement* statement, byte* key, double* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementGetOptionDouble));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementGetOptionIntDefault = NativeDelegate<StatementGetOptionInt>.AsNativePointer(StatementGetOptionIntDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, long*, CAdbcError*, AdbcStatusCode> StatementGetOptionIntDefault => &StatementGetOptionIntDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementGetOptionIntDefaultImpl(CAdbcStatement* statement, byte* key, long* value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementGetOptionInt));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementSetOptionBytesDefault = NativeDelegate<StatementSetOptionBytes>.AsNativePointer(StatementSetOptionBytesDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> StatementSetOptionBytesDefault => &StatementSetOptionBytesDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementSetOptionBytesDefaultImpl(CAdbcStatement* statement, byte* key, byte* value, nint length, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementSetOptionBytes));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementSetOptionDoubleDefault = NativeDelegate<StatementSetOptionDouble>.AsNativePointer(StatementSetOptionDoubleDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, double, CAdbcError*, AdbcStatusCode> StatementSetOptionDoubleDefault => &StatementSetOptionDoubleDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementSetOptionDoubleDefaultImpl(CAdbcStatement* statement, byte* key, double value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementSetOptionDouble));
        }

#if !NET5_0_OR_GREATER
        private static unsafe IntPtr StatementSetOptionIntDefault = NativeDelegate<StatementSetOptionInt>.AsNativePointer(StatementSetOptionIntDefaultImpl);
#else
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, long, CAdbcError*, AdbcStatusCode> StatementSetOptionIntDefault => &StatementSetOptionIntDefaultImpl;
        [UnmanagedCallersOnly]
#endif
        private static unsafe AdbcStatusCode StatementSetOptionIntDefaultImpl(CAdbcStatement* statement, byte* key, long value, CAdbcError* error)
        {
            return NotImplemented(error, nameof(CAdbcDriver.StatementSetOptionInt));
        }

        #endregion
    }
}

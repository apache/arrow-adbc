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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

#if NETSTANDARD
using Apache.Arrow.Adbc.Extensions;
#endif

namespace Apache.Arrow.Adbc.C
{
    public class CAdbcDriverExporter
    {
        internal unsafe delegate void ErrorRelease(CAdbcError* error);
        private static unsafe readonly NativeDelegate<ErrorRelease> s_releaseError = new NativeDelegate<ErrorRelease>(ReleaseError);
        internal unsafe delegate AdbcStatusCode DatabaseFn(CAdbcDatabase* database, CAdbcError* error);
        internal unsafe delegate AdbcStatusCode ConnectionFn(CAdbcConnection* connection, CAdbcError* error);

#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged<CAdbcError*, void> ReleaseErrorPtr => (delegate* unmanaged<CAdbcError*, void>)s_releaseError.Pointer;
        private static unsafe delegate* unmanaged<CAdbcDriver*, CAdbcError*, AdbcStatusCode> ReleaseDriverPtr => &ReleaseDriver;

        private static unsafe delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> DatabaseInitPtr => &InitDatabase;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> DatabaseReleasePtr => &ReleaseDatabase;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> DatabaseSetOptionPtr => &SetDatabaseOption;

        private static unsafe delegate* unmanaged<CAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetObjectsPtr => &GetConnectionObjects;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, CArrowSchema*, CAdbcError*, AdbcStatusCode> ConnectionGetTableSchemaPtr => &GetConnectionTableSchema;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetTableTypesPtr => &GetConnectionTableTypes;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcDatabase*, CAdbcError*, AdbcStatusCode> ConnectionInitPtr => &InitConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionRollbackPtr => &RollbackConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionCommitPtr => &CommitConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionReleasePtr => &ReleaseConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetInfoPtr => &GetConnectionInfo;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionReadPartitionPtr => &ReadConnectionPartition;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> ConnectionSetOptionPtr => &SetConnectionOption;

        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArray*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementBindPtr => &BindStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, long*, CAdbcError*, AdbcStatusCode> StatementExecuteQueryPtr => &ExecuteStatementQuery;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementNewPtr => &NewStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementReleasePtr => &ReleaseStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementPreparePtr => &PrepareStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode> StatementSetSqlQueryPtr => &SetStatementSqlQuery;
#else
        private static IntPtr ReleaseErrorPtr => s_releaseError.Pointer;
        internal unsafe delegate AdbcStatusCode DriverRelease(CAdbcDriver* driver, CAdbcError* error);
        private static unsafe readonly NativeDelegate<DriverRelease> s_releaseDriver = new NativeDelegate<DriverRelease>(ReleaseDriver);
        private static IntPtr ReleaseDriverPtr => s_releaseDriver.Pointer;

        private static unsafe readonly NativeDelegate<DatabaseFn> s_databaseInit = new NativeDelegate<DatabaseFn>(InitDatabase);
        private static IntPtr DatabaseInitPtr => s_databaseInit.Pointer;
        private static unsafe readonly NativeDelegate<DatabaseFn> s_databaseRelease = new NativeDelegate<DatabaseFn>(ReleaseDatabase);
        private static IntPtr DatabaseReleasePtr => s_databaseRelease.Pointer;
        internal unsafe delegate AdbcStatusCode DatabaseSetOption(CAdbcDatabase* database, byte* name, byte* value, CAdbcError* error);
        private static unsafe readonly NativeDelegate<DatabaseSetOption> s_databaseSetOption = new NativeDelegate<DatabaseSetOption>(SetDatabaseOption);
        private static IntPtr DatabaseSetOptionPtr => s_databaseSetOption.Pointer;

        internal unsafe delegate AdbcStatusCode ConnectionGetObjects(CAdbcConnection* connection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, CAdbcError* error);
        private static unsafe readonly NativeDelegate<ConnectionGetObjects> s_connectionGetObjects = new NativeDelegate<ConnectionGetObjects>(GetConnectionObjects);
        private static IntPtr ConnectionGetObjectsPtr => s_connectionGetObjects.Pointer;
        internal unsafe delegate AdbcStatusCode ConnectionGetTableSchema(CAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, CAdbcError* error);
        private static unsafe readonly NativeDelegate<ConnectionGetTableSchema> s_connectionGetTableSchema = new NativeDelegate<ConnectionGetTableSchema>(GetConnectionTableSchema);
        private static IntPtr ConnectionGetTableSchemaPtr => s_connectionGetTableSchema.Pointer;
        internal unsafe delegate AdbcStatusCode ConnectionGetTableTypes(CAdbcConnection* connection, CArrowArrayStream* stream, CAdbcError* error);
        private static unsafe readonly NativeDelegate<ConnectionGetTableTypes> s_connectionGetTableTypes = new NativeDelegate<ConnectionGetTableTypes>(GetConnectionTableTypes);
        private static IntPtr ConnectionGetTableTypesPtr => s_connectionGetTableTypes.Pointer;
        internal unsafe delegate AdbcStatusCode ConnectionInit(CAdbcConnection* connection, CAdbcDatabase* database, CAdbcError* error);
        private static unsafe readonly NativeDelegate<ConnectionInit> s_connectionInit = new NativeDelegate<ConnectionInit>(InitConnection);
        private static IntPtr ConnectionInitPtr => s_connectionInit.Pointer;
        private static unsafe readonly NativeDelegate<ConnectionFn> s_connectionRollback = new NativeDelegate<ConnectionFn>(RollbackConnection);
        private static IntPtr ConnectionRollbackPtr => s_connectionRollback.Pointer;
        private static unsafe readonly NativeDelegate<ConnectionFn> s_connectionCommit = new NativeDelegate<ConnectionFn>(CommitConnection);
        private static IntPtr ConnectionCommitPtr => s_connectionCommit.Pointer;
        private static unsafe readonly NativeDelegate<ConnectionFn> s_connectionRelease = new NativeDelegate<ConnectionFn>(ReleaseConnection);
        private static IntPtr ConnectionReleasePtr => s_connectionRelease.Pointer;
        internal unsafe delegate AdbcStatusCode ConnectionGetInfo(CAdbcConnection* connection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error);
        private static unsafe readonly NativeDelegate<ConnectionGetInfo> s_connectionGetInfo = new NativeDelegate<ConnectionGetInfo>(GetConnectionInfo);
        private static IntPtr ConnectionGetInfoPtr => s_connectionGetInfo.Pointer;
        private unsafe delegate AdbcStatusCode ConnectionReadPartition(CAdbcConnection* connection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error);
        private static unsafe readonly NativeDelegate<ConnectionReadPartition> s_connectionReadPartition = new NativeDelegate<ConnectionReadPartition>(ReadConnectionPartition);
        private static IntPtr ConnectionReadPartitionPtr => s_connectionReadPartition.Pointer;
        internal unsafe delegate AdbcStatusCode ConnectionSetOption(CAdbcConnection* connection, byte* name, byte* value, CAdbcError* error);
        private static unsafe readonly NativeDelegate<ConnectionSetOption> s_connectionSetOption = new NativeDelegate<ConnectionSetOption>(SetConnectionOption);
        private static IntPtr ConnectionSetOptionPtr => s_connectionSetOption.Pointer;

        private unsafe delegate AdbcStatusCode StatementBind(CAdbcStatement* statement, CArrowArray* array, CArrowSchema* schema, CAdbcError* error);
        private static unsafe readonly NativeDelegate<StatementBind> s_statementBind = new NativeDelegate<StatementBind>(BindStatement);
        private static IntPtr StatementBindPtr => s_statementBind.Pointer;
        internal unsafe delegate AdbcStatusCode StatementExecuteQuery(CAdbcStatement* statement, CArrowArrayStream* stream, long* rows, CAdbcError* error);
        private static unsafe readonly NativeDelegate<StatementExecuteQuery> s_statementExecuteQuery = new NativeDelegate<StatementExecuteQuery>(ExecuteStatementQuery);
        private static IntPtr StatementExecuteQueryPtr = s_statementExecuteQuery.Pointer;
        internal unsafe delegate AdbcStatusCode StatementNew(CAdbcConnection* connection, CAdbcStatement* statement, CAdbcError* error);
        private static unsafe readonly NativeDelegate<StatementNew> s_statementNew = new NativeDelegate<StatementNew>(NewStatement);
        private static IntPtr StatementNewPtr => s_statementNew.Pointer;
        internal unsafe delegate AdbcStatusCode StatementFn(CAdbcStatement* statement, CAdbcError* error);
        private static unsafe readonly NativeDelegate<StatementFn> s_statementRelease = new NativeDelegate<StatementFn>(ReleaseStatement);
        private static IntPtr StatementReleasePtr => s_statementRelease.Pointer;
        private static unsafe readonly NativeDelegate<StatementFn> s_statementPrepare = new NativeDelegate<StatementFn>(PrepareStatement);
        private static IntPtr StatementPreparePtr => s_statementRelease.Pointer;
        internal unsafe delegate AdbcStatusCode StatementSetSqlQuery(CAdbcStatement* statement, byte* text, CAdbcError* error);
        private static unsafe readonly NativeDelegate<StatementSetSqlQuery> s_statementSetSqlQuery = new NativeDelegate<StatementSetSqlQuery>(SetStatementSqlQuery);
        private static IntPtr StatementSetSqlQueryPtr = s_statementSetSqlQuery.Pointer;
#endif

/*
 * Not yet implemented

        unsafe delegate AdbcStatusCode StatementBindStream(CAdbcStatement* statement, CArrowArrayStream* stream, CAdbcError* error);
        unsafe delegate AdbcStatusCode StatementExecutePartitions(CAdbcStatement* statement, CArrowSchema* schema, CAdbcPartitions* partitions, long* rows_affected, CAdbcError* error);
        unsafe delegate AdbcStatusCode StatementGetParameterSchema(CAdbcStatement* statement, CArrowSchema* schema, CAdbcError* error);
        unsafe delegate AdbcStatusCode StatementSetSubstraitPlan(CAdbcStatement statement, byte* plan, int length, CAdbcError error);
*/

        public unsafe static AdbcStatusCode AdbcDriverInit(int version, CAdbcDriver* nativeDriver, CAdbcError* error, AdbcDriver driver)
        {
            DriverStub stub = new DriverStub(driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDriver->private_data = (void*)GCHandle.ToIntPtr(handle);
            nativeDriver->release = ReleaseDriverPtr;

            nativeDriver->DatabaseInit = DatabaseInitPtr;
            nativeDriver->DatabaseNew = stub.NewDatabasePtr;
            nativeDriver->DatabaseSetOption = DatabaseSetOptionPtr;
            nativeDriver->DatabaseRelease = DatabaseReleasePtr;

            // TODO: This should probably only set the pointers for the functionality actually supported by this particular driver
            nativeDriver->ConnectionCommit = ConnectionCommitPtr;
            nativeDriver->ConnectionGetInfo = ConnectionGetInfoPtr;
            nativeDriver->ConnectionGetObjects = ConnectionGetObjectsPtr;
            nativeDriver->ConnectionGetTableSchema = ConnectionGetTableSchemaPtr;
            nativeDriver->ConnectionGetTableTypes = ConnectionGetTableTypesPtr;
            nativeDriver->ConnectionInit = ConnectionInitPtr;
            nativeDriver->ConnectionNew = stub.NewConnectionPtr;
            nativeDriver->ConnectionSetOption = ConnectionSetOptionPtr;
            nativeDriver->ConnectionReadPartition = ConnectionReadPartitionPtr;
            nativeDriver->ConnectionRelease = ConnectionReleasePtr;
            nativeDriver->ConnectionRollback = ConnectionRollbackPtr;

            nativeDriver->StatementBind = StatementBindPtr;
            // nativeDriver->StatementBindStream = StatementBindStreamPtr;
            nativeDriver->StatementExecuteQuery = StatementExecuteQueryPtr;
            // nativeDriver->StatementExecutePartitions = StatementExecutePartitionsPtr;
            // nativeDriver->StatementGetParameterSchema = StatementGetParameterSchemaPtr;
            nativeDriver->StatementNew = StatementNewPtr;
            nativeDriver->StatementPrepare = StatementPreparePtr;
            nativeDriver->StatementRelease = StatementReleasePtr;
            nativeDriver->StatementSetSqlQuery = StatementSetSqlQueryPtr;
            // nativeDriver->StatementSetSubstraitPlan = StatementSetSubstraitPlanPtr;

            return 0;
        }

        private unsafe static void ReleaseError(CAdbcError* error)
        {
            if (error != null && ((IntPtr)error->message) != IntPtr.Zero)
            {
                Marshal.FreeHGlobal((IntPtr)error->message);
            }
        }

        private unsafe static AdbcStatusCode SetError(CAdbcError* error, Exception exception)
        {
            ReleaseError(error);

#if NETSTANDARD
            error->message = (byte*)MarshalExtensions.StringToCoTaskMemUTF8(exception.Message);
#else
            error->message = (byte*)Marshal.StringToCoTaskMemUTF8(exception.Message);
#endif

            error->sqlstate0 = (byte)0;
            error->sqlstate1 = (byte)0;
            error->sqlstate2 = (byte)0;
            error->sqlstate3 = (byte)0;
            error->sqlstate4 = (byte)0;
            error->vendor_code = 0;
            error->release = ReleaseErrorPtr;

            return AdbcStatusCode.UnknownError;
        }

        private sealed class PinnedArray : IDisposable
        {
            IArrowArray _array;
            MemoryHandle[] pinnedHandles;

            public PinnedArray(IArrowArray array)
            {
                _array = array;
                pinnedHandles = new MemoryHandle[GetHandleCount(array.Data)];
                int index = 0;
                PinBuffers(array.Data, pinnedHandles, ref index);
                Debug.Assert(index == pinnedHandles.Length);
            }

            public void Dispose()
            {
                if (_array != null)
                {
                    _array.Dispose();
                    foreach (MemoryHandle handle in pinnedHandles)
                    {
                        handle.Dispose();
                    }
                    _array = null;
                }
            }

            static int GetHandleCount(ArrayData data)
            {
                int handleCount = data.Buffers.Length;
                foreach (ArrayData child in data.Children)
                {
                    handleCount += GetHandleCount(child);
                }
                if (data.Dictionary != null)
                {
                    handleCount += GetHandleCount(data.Dictionary);
                }
                return handleCount;
            }

            static void PinBuffers(ArrayData data, MemoryHandle[] handles, ref int index)
            {
                foreach (ArrowBuffer buffer in data.Buffers)
                {
                    handles[index++] = buffer.Memory.Pin();
                }
                foreach (ArrayData child in data.Children)
                {
                    PinBuffers(child, handles, ref index);
                }
                if (data.Dictionary != null)
                {
                    PinBuffers(data.Dictionary, handles, ref index);
                }
            }
        }

        private static IntPtr FromDisposable(IDisposable d)
        {
            GCHandle gch = GCHandle.Alloc(d);
            return GCHandle.ToIntPtr(gch);
        }

        private static void Dispose(ref IntPtr p)
        {
            GCHandle gch = GCHandle.FromIntPtr(p);
            ((IDisposable)gch.Target).Dispose();
            gch.Free();
            p = IntPtr.Zero;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseDriver(CAdbcDriver* nativeDriver, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDriver->private_data);
            DriverStub stub = (DriverStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDriver->private_data = null;
            return 0;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode InitDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.Init(ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
        {
            if (nativeDatabase->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDatabase->private_data = null;
            return AdbcStatusCode.Success;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetConnectionOption(CAdbcConnection* nativeConnection, byte* name, byte* value, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.SetOption(name, value, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetDatabaseOption(CAdbcDatabase* nativeDatabase, byte* name, byte* value, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;

            return stub.SetOption(name, value, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode InitConnection(CAdbcConnection* nativeConnection, CAdbcDatabase* database, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.InitConnection(ref *database, ref *error);
        }


#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionObjects(CAdbcConnection* nativeConnection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, CAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetObjects(ref *nativeConnection, depth, catalog, db_schema, table_name, table_type, column_name, stream, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionTableTypes(CAdbcConnection* nativeConnection, CArrowArrayStream* stream, CAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetTableTypes(ref *nativeConnection, stream, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionTableSchema(CAdbcConnection* nativeConnection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, CAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetTableSchema(ref *nativeConnection, catalog, db_schema, table_name, schema, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode RollbackConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            stub.Rollback();
            return AdbcStatusCode.Success;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode CommitConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            stub.Commit();
            return AdbcStatusCode.Success;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeConnection->private_data = null;
            return AdbcStatusCode.Success;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReadConnectionPartition(CAdbcConnection* nativeConnection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.ReadPartition(ref *nativeConnection, serialized_partition, serialized_length, stream, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionInfo(CAdbcConnection* nativeConnection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetInfo(ref *nativeConnection, info_codes, info_codes_length, stream, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementSqlQuery(CAdbcStatement* nativeStatement, byte* text, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;

#if NETSTANDARD
            stub.SqlQuery = MarshalExtensions.PtrToStringUTF8((IntPtr)text);
#else
            stub.SqlQuery = Marshal.PtrToStringUTF8((IntPtr)text);
#endif

            return AdbcStatusCode.Success;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode BindStatement(CAdbcStatement* nativeStatement, CArrowArray* array, CArrowSchema* cschema, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;

            Schema schema = CArrowSchemaImporter.ImportSchema(cschema);

            RecordBatch batch = CArrowArrayImporter.ImportRecordBatch(array, schema);

            stub.Bind(batch, schema);

            return 0;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ExecuteStatementQuery(CAdbcStatement* nativeStatement, CArrowArrayStream* stream, long* rows, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;
            var result = stub.ExecuteQuery();
            if (rows != null)
            {
                *rows = result.RowCount;
            }

            GCHandle streamHandle = GCHandle.Alloc(result.Stream);
            stream->private_data = (void*)GCHandle.ToIntPtr(streamHandle);

            return 0;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode NewStatement(CAdbcConnection* nativeConnection, CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.NewStatement(ref *nativeStatement, ref *error);
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseStatement(CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            if (nativeStatement->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeStatement->private_data = null;
            return AdbcStatusCode.Success;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode PrepareStatement(CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            if (nativeStatement->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
            AdbcStatement statement = (AdbcStatement)gch.Target;
            statement.Prepare();
            return AdbcStatusCode.Success;
        }

        private sealed class DriverStub : IDisposable
        {
            private readonly AdbcDriver _driver;
            private unsafe readonly NativeDelegate<DatabaseFn> newDatabase;
            private unsafe readonly NativeDelegate<ConnectionFn> newConnection;

#if NET5_0_OR_GREATER
            internal unsafe delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> NewDatabasePtr =>
                (delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode>)newDatabase.Pointer;
            internal unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> NewConnectionPtr =>
                (delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode>)newConnection.Pointer;
#else
            internal IntPtr NewDatabasePtr => newDatabase.Pointer;
            internal IntPtr NewConnectionPtr => newConnection.Pointer;
#endif

            public DriverStub(AdbcDriver driver)
            {
                _driver = driver;

                unsafe
                {
                    newDatabase = new NativeDelegate<DatabaseFn>(NewDatabase);
                    newConnection = new NativeDelegate<ConnectionFn>(NewConnection);
                }
            }

            private unsafe AdbcStatusCode NewDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
            {
                if (nativeDatabase->private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                DatabaseStub stub = new DatabaseStub(_driver);
                GCHandle handle = GCHandle.Alloc(stub);
                nativeDatabase->private_data = (void*)GCHandle.ToIntPtr(handle);

                return AdbcStatusCode.Success;
            }

            private unsafe AdbcStatusCode NewConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
            {
                if (nativeConnection->private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                ConnectionStub stub = new ConnectionStub(_driver);
                GCHandle handle = GCHandle.Alloc(stub);
                nativeConnection->private_data = (void*)GCHandle.ToIntPtr(handle);

                return AdbcStatusCode.Success;
            }

            public void Dispose()
            {
                _driver.Dispose();
            }
        }

        sealed class DatabaseStub : IDisposable
        {
            readonly AdbcDriver _driver;
            readonly Dictionary<string, string> options;
            AdbcDatabase database;

            public DatabaseStub(AdbcDriver driver)
            {
                _driver = driver;
                options = new Dictionary<string, string>();
            }

            public AdbcStatusCode Init(ref CAdbcError error)
            {
                if (database != null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                database = _driver.Open(options);
                return AdbcStatusCode.Success;
            }

            public unsafe AdbcStatusCode SetOption(byte* name, byte* value, ref CAdbcError error)
            {
                IntPtr namePtr = (IntPtr)name;
                IntPtr valuePtr = (IntPtr)value;

#if NETSTANDARD
                options[MarshalExtensions.PtrToStringUTF8(namePtr)] = MarshalExtensions.PtrToStringUTF8(valuePtr);
#else
                options[Marshal.PtrToStringUTF8(namePtr)] = Marshal.PtrToStringUTF8(valuePtr);
#endif

                return AdbcStatusCode.Success;
            }

            public AdbcStatusCode OpenConnection(IReadOnlyDictionary<string, string> options, ref CAdbcError error, out AdbcConnection connection)
            {
                if (database == null)
                {
                    connection = null;
                    return AdbcStatusCode.UnknownError;
                }

                connection = database.Connect(options);
                return AdbcStatusCode.Success;
            }

            public void Dispose()
            {
                database?.Dispose();
                database = null;
            }
        }

        sealed class ConnectionStub : IDisposable
        {
            readonly AdbcDriver _driver;
            readonly Dictionary<string, string> options;
            AdbcConnection connection;

            public ConnectionStub(AdbcDriver driver)
            {
                _driver = driver;
                options = new Dictionary<string, string>();
            }

            public unsafe AdbcStatusCode SetOption(byte* name, byte* value, ref CAdbcError error)
            {
                IntPtr namePtr = (IntPtr)name;
                IntPtr valuePtr = (IntPtr)value;

#if NETSTANDARD
                options[MarshalExtensions.PtrToStringUTF8(namePtr)] = MarshalExtensions.PtrToStringUTF8(valuePtr);
#else
                options[Marshal.PtrToStringUTF8(namePtr)] = Marshal.PtrToStringUTF8(valuePtr);
#endif

                return AdbcStatusCode.Success;
            }

            public void Rollback() { this.connection.Rollback(); }
            public void Commit() { this.connection.Commit(); }

            public void Dispose()
            {
                connection?.Dispose();
                connection = null;
            }

            public unsafe AdbcStatusCode GetObjects(ref CAdbcConnection nativeConnection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* cstream, ref CAdbcError error)
            {
                if (nativeConnection.private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                string catalogPattern = string.Empty;
                string dbSchemaPattern = string.Empty;
                string tableNamePattern = string.Empty;
                string columnNamePattern = string.Empty;

#if NETSTANDARD
                catalogPattern = MarshalExtensions.PtrToStringUTF8((IntPtr)catalog);
                dbSchemaPattern = MarshalExtensions.PtrToStringUTF8((IntPtr)db_schema);
                tableNamePattern = MarshalExtensions.PtrToStringUTF8((IntPtr)table_name);
                columnNamePattern = MarshalExtensions.PtrToStringUTF8((IntPtr)column_name);
#else
                catalogPattern = Marshal.PtrToStringUTF8((IntPtr)catalog);
                dbSchemaPattern = Marshal.PtrToStringUTF8((IntPtr)db_schema);
                tableNamePattern = Marshal.PtrToStringUTF8((IntPtr)table_name);
                columnNamePattern = Marshal.PtrToStringUTF8((IntPtr)column_name);
#endif

                GCHandle gch = GCHandle.FromIntPtr((IntPtr)table_type);
                List<string> tableTypes = (List<string>)gch.Target;

                AdbcConnection.GetObjectsDepth goDepth = (AdbcConnection.GetObjectsDepth)depth;

                IArrowArrayStream stream = connection.GetObjects(goDepth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern);

                CArrowArrayStreamExporter.ExportArrayStream(stream, cstream);

                return AdbcStatusCode.Success;
            }

            public unsafe AdbcStatusCode GetTableSchema(ref CAdbcConnection nativeConnection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* cschema, ref CAdbcError error)
            {
                if (nativeConnection.private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                string sCatalog = string.Empty;
                string sDbSchema = string.Empty;
                string sTableName = string.Empty;

#if NETSTANDARD
                sCatalog = MarshalExtensions.PtrToStringUTF8((IntPtr)catalog);
                sDbSchema = MarshalExtensions.PtrToStringUTF8((IntPtr)db_schema);
                sTableName = MarshalExtensions.PtrToStringUTF8((IntPtr)table_name);
#else
                sCatalog = Marshal.PtrToStringUTF8((IntPtr)catalog);
                sDbSchema = Marshal.PtrToStringUTF8((IntPtr)db_schema);
                sTableName = Marshal.PtrToStringUTF8((IntPtr)table_name);
#endif

                Schema schema = connection.GetTableSchema(sCatalog, sDbSchema, sTableName);

                CArrowSchemaExporter.ExportSchema(schema, cschema);

                return AdbcStatusCode.Success;
            }

            public unsafe AdbcStatusCode GetTableTypes(ref CAdbcConnection nativeConnection, CArrowArrayStream* cArrayStream, ref CAdbcError error)
            {
                if (nativeConnection.private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                CArrowArrayStreamExporter.ExportArrayStream(connection.GetTableTypes(), cArrayStream);

                return AdbcStatusCode.Success;
            }

            public unsafe AdbcStatusCode ReadPartition(ref CAdbcConnection nativeConnection, byte* serializedPartition, int serialized_length, CArrowArrayStream* stream, ref CAdbcError error)
            {
                if (nativeConnection.private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                GCHandle gch = GCHandle.FromIntPtr((IntPtr)serializedPartition);
                PartitionDescriptor descriptor = (PartitionDescriptor)gch.Target;

                CArrowArrayStreamExporter.ExportArrayStream(connection.ReadPartition(descriptor), stream);

                return AdbcStatusCode.Success;
            }

            public unsafe AdbcStatusCode GetInfo(ref CAdbcConnection nativeConnection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, ref CAdbcError error)
            {
                if (nativeConnection.private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                GCHandle gch = GCHandle.FromIntPtr((IntPtr)info_codes);
                List<int> codes = (List<int>)gch.Target;

                CArrowArrayStreamExporter.ExportArrayStream(connection.GetInfo(codes), stream);

                return AdbcStatusCode.Success;
            }

            public unsafe AdbcStatusCode InitConnection(ref CAdbcDatabase nativeDatabase, ref CAdbcError error)
            {
                if (nativeDatabase.private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase.private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target;
                return stub.OpenConnection(options, ref error, out connection);
            }

            public unsafe AdbcStatusCode NewStatement(ref CAdbcStatement nativeStatement, ref CAdbcError error)
            {
                if (connection == null)
                {
                    return AdbcStatusCode.UnknownError;
                }
                if (nativeStatement.private_data == null)
                {
                    return AdbcStatusCode.UnknownError;
                }

                AdbcStatement statement = connection.CreateStatement();
                GCHandle handle = GCHandle.Alloc(statement);
                nativeStatement.private_data = (void*)GCHandle.ToIntPtr(handle);

                return AdbcStatusCode.Success;
            }
        }
    }
}

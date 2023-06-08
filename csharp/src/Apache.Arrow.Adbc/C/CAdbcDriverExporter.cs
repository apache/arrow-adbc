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
        private unsafe static readonly NativeDelegate<ErrorRelease> releaseError = new NativeDelegate<ErrorRelease>(ReleaseError);
        private unsafe static readonly NativeDelegate<DriverRelease> releaseDriver = new NativeDelegate<DriverRelease>(ReleaseDriver);

        private unsafe static readonly NativeDelegate<DatabaseFn> databaseInit = new NativeDelegate<DatabaseFn>(InitDatabase);
        private unsafe static readonly NativeDelegate<DatabaseFn> databaseRelease = new NativeDelegate<DatabaseFn>(ReleaseDatabase);
        private unsafe static readonly NativeDelegate<DatabaseSetOption> databaseSetOption = new NativeDelegate<DatabaseSetOption>(SetDatabaseOption);

        private unsafe static readonly NativeDelegate<ConnectionGetObjects> connectionGetObjects = new NativeDelegate<ConnectionGetObjects>(GetConnectionObjects);
        private unsafe static readonly NativeDelegate<ConnectionGetTableSchema> connectionGetTableSchema = new NativeDelegate<ConnectionGetTableSchema>(GetConnectionTableSchema);
        private unsafe static readonly NativeDelegate<ConnectionGetTableTypes> connectionGetTableTypes = new NativeDelegate<ConnectionGetTableTypes>(GetConnectionTableTypes);
        private unsafe static readonly NativeDelegate<ConnectionInit> connectionInit = new NativeDelegate<ConnectionInit>(InitConnection);
        private unsafe static readonly NativeDelegate<ConnectionFn> connectionRelease = new NativeDelegate<ConnectionFn>(ReleaseConnection);
        private unsafe static readonly NativeDelegate<ConnectionGetInfo> connectionGetInfo = new NativeDelegate<ConnectionGetInfo>(GetConnectionInfo);
        private unsafe static readonly NativeDelegate<ConnectionReadPartition> connectionReadPartition = new NativeDelegate<ConnectionReadPartition>(ReadConnectionPartition);
        private unsafe static readonly NativeDelegate<ConnectionSetOption> connectionSetOption = new NativeDelegate<ConnectionSetOption>(SetConnectionOption);

        private unsafe static readonly NativeDelegate<StatementBind> statementBind = new NativeDelegate<StatementBind>(BindStatement);
        private unsafe static readonly NativeDelegate<StatementExecuteQuery> statementExecuteQuery = new NativeDelegate<StatementExecuteQuery>(ExecuteStatementQuery);
        private unsafe static readonly NativeDelegate<StatementNew> statementNew = new NativeDelegate<StatementNew>(NewStatement);
        private unsafe static readonly NativeDelegate<StatementFn> statementRelease = new NativeDelegate<StatementFn>(ReleaseStatement);
        private unsafe static readonly NativeDelegate<StatementSetSqlQuery> statementSetSqlQuery = new NativeDelegate<StatementSetSqlQuery>(SetStatementSqlQuery);

        public unsafe static AdbcStatusCode AdbcDriverInit(int version, CAdbcDriver* nativeDriver, CAdbcError* error, AdbcDriver driver)
        {
            DriverStub stub = new DriverStub(driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDriver->private_data = (void*)GCHandle.ToIntPtr(handle);
            nativeDriver->release = (delegate* unmanaged[Stdcall]<CAdbcDriver*, CAdbcError*, AdbcStatusCode>)releaseDriver.Pointer;

            nativeDriver->DatabaseInit = (delegate* unmanaged[Stdcall]<CAdbcDatabase*, CAdbcError*, AdbcStatusCode>)databaseInit.Pointer;
            nativeDriver->DatabaseNew = (delegate* unmanaged[Stdcall]<CAdbcDatabase*, CAdbcError*, AdbcStatusCode>)stub.newDatabase.Pointer;
            nativeDriver->DatabaseSetOption = (delegate* unmanaged[Stdcall]<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode>)databaseSetOption.Pointer;
            nativeDriver->DatabaseRelease = (delegate* unmanaged[Stdcall]<CAdbcDatabase*, CAdbcError*, AdbcStatusCode>)databaseRelease.Pointer;

            nativeDriver->ConnectionCommit = (delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;
            nativeDriver->ConnectionGetInfo = (delegate* unmanaged[Stdcall]<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode>)connectionGetInfo.Pointer;
            nativeDriver->ConnectionGetObjects = (delegate* unmanaged[Stdcall]<CAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode>)connectionGetObjects.Pointer;
            nativeDriver->ConnectionGetTableSchema = (delegate* unmanaged[Stdcall]<CAdbcConnection*, byte*, byte*, byte*, CArrowSchema*, CAdbcError*, AdbcStatusCode>)connectionGetTableSchema.Pointer;
            nativeDriver->ConnectionGetTableTypes = (delegate* unmanaged[Stdcall]<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode>)connectionGetTableTypes.Pointer;
            nativeDriver->ConnectionInit = (delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcDatabase*, CAdbcError*, AdbcStatusCode>)connectionInit.Pointer;
            nativeDriver->ConnectionNew = (delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcError*, AdbcStatusCode>)stub.newConnection.Pointer;
            nativeDriver->ConnectionSetOption = (delegate* unmanaged[Stdcall]<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode>)connectionSetOption.Pointer;
            nativeDriver->ConnectionReadPartition = (delegate* unmanaged[Stdcall]<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode>)connectionReadPartition.Pointer;
            nativeDriver->ConnectionRelease = (delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;
            nativeDriver->ConnectionRollback = (delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;

            nativeDriver->StatementBind = (delegate* unmanaged[Stdcall]<CAdbcStatement*, CArrowArray*, CArrowSchema*, CAdbcError*, AdbcStatusCode>)statementBind.Pointer;
            nativeDriver->StatementNew = (delegate* unmanaged[Stdcall]<CAdbcConnection*, CAdbcStatement*, CAdbcError*, AdbcStatusCode>)statementNew.Pointer;
            nativeDriver->StatementSetSqlQuery = (delegate* unmanaged[Stdcall]<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode>)statementSetSqlQuery.Pointer;
            nativeDriver->StatementExecuteQuery = (delegate* unmanaged[Stdcall]<CAdbcStatement*, CArrowArrayStream*, long*, CAdbcError*, AdbcStatusCode>)statementExecuteQuery.Pointer;
            nativeDriver->StatementPrepare = (delegate* unmanaged[Stdcall]<CAdbcStatement*, CAdbcError*, AdbcStatusCode>)statementRelease.Pointer;
            nativeDriver->StatementRelease = (delegate* unmanaged[Stdcall]<CAdbcStatement*, CAdbcError*, AdbcStatusCode>)statementRelease.Pointer;

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
            error->vendor_code = 0;
            error->release = (delegate* unmanaged[Stdcall]<CAdbcError*, void>)releaseError.Pointer;

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

        private unsafe static AdbcStatusCode ReleaseDriver(CAdbcDriver* nativeDriver, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDriver->private_data);
            DriverStub stub = (DriverStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDriver->private_data = null;
            return 0;
        }

        private unsafe static AdbcStatusCode InitDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.Init(ref *error);
        }

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

        private unsafe static AdbcStatusCode SetConnectionOption(CAdbcConnection* nativeConnection, byte* name, byte* value, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.SetOption(name, value, ref *error);
        }

        private unsafe static AdbcStatusCode SetDatabaseOption(CAdbcDatabase* nativeDatabase, byte* name, byte* value, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;

            return stub.SetOption(name, value, ref *error);
        }

        private unsafe static AdbcStatusCode InitConnection(CAdbcConnection* nativeConnection, CAdbcDatabase* database, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.InitConnection(ref *database, ref *error);
        }


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

        private unsafe static AdbcStatusCode ReadConnectionPartition(CAdbcConnection* nativeConnection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.ReadPartition(ref *nativeConnection, serialized_partition, serialized_length, stream, ref *error);
        }

        private unsafe static AdbcStatusCode GetConnectionInfo(CAdbcConnection* nativeConnection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetInfo(ref *nativeConnection, info_codes, info_codes_length, stream, ref *error);
        }

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

        private unsafe static AdbcStatusCode BindStatement(CAdbcStatement* nativeStatement, CArrowArray* array, CArrowSchema* cschema, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;

            Schema schema = CArrowSchemaImporter.ImportSchema(cschema);

            RecordBatch batch = CArrowArrayImporter.ImportRecordBatch(array, schema);

            stub.Bind(batch, schema);

            return 0;
        }

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

        private unsafe static AdbcStatusCode NewStatement(CAdbcConnection* nativeConnection, CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.NewStatement(ref *nativeStatement, ref *error);
        }

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
    }


    unsafe delegate AdbcStatusCode DriverRelease(CAdbcDriver* driver, CAdbcError* error);

    unsafe delegate AdbcStatusCode DatabaseFn(CAdbcDatabase* database, CAdbcError* error);
    unsafe delegate AdbcStatusCode DatabaseSetOption(CAdbcDatabase* database, byte* name, byte* value, CAdbcError* error);

    unsafe delegate AdbcStatusCode ConnectionFn(CAdbcConnection* connection, CAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetInfo(CAdbcConnection* connection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetObjects(CAdbcConnection* connection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, CAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetTableSchema(CAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, CAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetTableTypes(CAdbcConnection* connection, CArrowArrayStream* stream, CAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionInit(CAdbcConnection* connection, CAdbcDatabase* database, CAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionSetOption(CAdbcConnection* connection, byte* name, byte* value, CAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionReadPartition(CAdbcConnection* connection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error);

    unsafe delegate AdbcStatusCode StatementBind(CAdbcStatement* statement, CArrowArray* array, CArrowSchema* schema, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementBindStream(CAdbcStatement* statement, CArrowArrayStream* stream, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementExecuteQuery(CAdbcStatement* statement, CArrowArrayStream* stream, long* rows, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementExecutePartitions(CAdbcStatement* statement, CArrowSchema* schema, CAdbcPartitions* partitions, long* rows_affected, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementGetParameterSchema(CAdbcStatement* statement, CArrowSchema* schema, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementNew(CAdbcConnection* connection, CAdbcStatement* statement, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementFn(CAdbcStatement* statement, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetOption(CAdbcStatement* statement, byte* name, byte* value, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetSqlQuery(CAdbcStatement* statement, byte* text, CAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetSubstraitPlan(CAdbcStatement statement, byte* plan, int length, CAdbcError error);

    unsafe delegate void ErrorRelease(CAdbcError* error);

    sealed class DriverStub : IDisposable
    {
        readonly AdbcDriver _driver;
        public unsafe readonly NativeDelegate<DatabaseFn> newDatabase;
        public unsafe readonly NativeDelegate<ConnectionFn> newConnection;

        public DriverStub(AdbcDriver driver)
        {
            _driver = driver;

            unsafe
            {
                newDatabase = new NativeDelegate<DatabaseFn>(NewDatabase);
                newConnection = new NativeDelegate<ConnectionFn>(NewConnection);
            }
        }

        public unsafe AdbcStatusCode NewDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
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

        public unsafe AdbcStatusCode NewConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
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

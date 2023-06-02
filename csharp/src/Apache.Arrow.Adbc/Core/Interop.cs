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
using Apache.Arrow.Adbc.Interop;
using Apache.Arrow.C;
using static System.Net.Mime.MediaTypeNames;

#if NETSTANDARD
using Apache.Arrow.Adbc.Extensions;
#endif

namespace Apache.Arrow.Adbc.Core
{
    internal static class AdbcInterop
    {
        private unsafe static readonly NativeDelegate<ErrorRelease> releaseError = new NativeDelegate<ErrorRelease>(ReleaseError);
        private unsafe static readonly NativeDelegate<DriverRelease> releaseDriver = new NativeDelegate<DriverRelease>(ReleaseDriver);

        private unsafe static readonly NativeDelegate<DatabaseFn> databaseInit = new NativeDelegate<DatabaseFn>(InitDatabase);
        private unsafe static readonly NativeDelegate<DatabaseFn> databaseRelease = new NativeDelegate<DatabaseFn>(ReleaseDatabase);
        private unsafe static readonly NativeDelegate<DatabaseSetOption> databaseSetOption = new NativeDelegate<DatabaseSetOption>(SetDatabaseOption);

        private unsafe static readonly NativeDelegate<ConnectionInit> connectionInit = new NativeDelegate<ConnectionInit>(InitConnection);
        private unsafe static readonly NativeDelegate<ConnectionFn> connectionRelease = new NativeDelegate<ConnectionFn>(ReleaseConnection);
        private unsafe static readonly NativeDelegate<ConnectionGetInfo> connectionGetInfo = new NativeDelegate<ConnectionGetInfo>(GetConnectionInfo);
        private unsafe static readonly NativeDelegate<ConnectionSetOption> connectionSetOption = new NativeDelegate<ConnectionSetOption>(SetConnectionOption);
        
        private unsafe static readonly NativeDelegate<StatementExecuteQuery> statementExecuteQuery = new NativeDelegate<StatementExecuteQuery>(ExecuteStatementQuery);
        private unsafe static readonly NativeDelegate<StatementNew> statementNew = new NativeDelegate<StatementNew>(NewStatement);
        private unsafe static readonly NativeDelegate<StatementFn> statementRelease = new NativeDelegate<StatementFn>(ReleaseStatement);
        private unsafe static readonly NativeDelegate<StatementSetSqlQuery> statementSetSqlQuery = new NativeDelegate<StatementSetSqlQuery>(SetStatementSqlQuery);

        public unsafe static AdbcStatusCode AdbcDriverInit(int version, NativeAdbcDriver* nativeDriver, NativeAdbcError* error, AdbcDriver driver)
        {
            DriverStub stub = new DriverStub(driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDriver->private_data = (void*)GCHandle.ToIntPtr(handle);
            nativeDriver->release = (delegate* unmanaged[Stdcall]<NativeAdbcDriver*, NativeAdbcError*, AdbcStatusCode>)releaseDriver.Pointer;

            nativeDriver->DatabaseInit = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)databaseInit.Pointer;
            nativeDriver->DatabaseNew = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)stub.newDatabase.Pointer;
            nativeDriver->DatabaseSetOption = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, byte*, byte*, NativeAdbcError*, AdbcStatusCode>) databaseSetOption.Pointer;
            nativeDriver->DatabaseRelease = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)databaseRelease.Pointer;

            nativeDriver->ConnectionCommit = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;
            //nativeDriver->ConnectionGetInfo = (delegate* unmanaged[Stdcall]<NativeAdbcConnection *, int*, int, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>)connectionGetInfo.Pointer;
            //nativeDriver->ConnectionGetTableSchema = null;
            //nativeDriver->ConnectionGetTableTypes = null;
            nativeDriver->ConnectionInit = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)connectionInit.Pointer;
            nativeDriver->ConnectionNew = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)stub.newConnection.Pointer;
            nativeDriver->ConnectionSetOption = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, byte*, NativeAdbcError*, AdbcStatusCode>)connectionSetOption.Pointer;
            //nativeDriver->ConnectionReadPartition = null;
            nativeDriver->ConnectionRelease = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;
            //nativeDriver->ConnectionRollback = null;

           // nativeDriver->StatementBind = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArray*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode>)
            nativeDriver->StatementNew = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode>)statementNew.Pointer;
            nativeDriver->StatementSetSqlQuery = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, byte*, NativeAdbcError *, AdbcStatusCode >)statementSetSqlQuery.Pointer;
            nativeDriver->StatementExecuteQuery = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, long*, NativeAdbcError*, AdbcStatusCode>)statementExecuteQuery.Pointer;
            nativeDriver->StatementPrepare = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode>)statementRelease.Pointer;
            nativeDriver->StatementRelease = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode>)statementRelease.Pointer;
            
            return 0;
        }

        private unsafe static void ReleaseError(NativeAdbcError* error)
        {
            if (error != null && ((IntPtr)error->message) != IntPtr.Zero)
            {
                Marshal.FreeCoTaskMem((IntPtr)error->message);
            }
        }

        private unsafe static AdbcStatusCode SetError(NativeAdbcError* error, Exception exception)
        {
            ReleaseError(error);

            #if NETSTANDARD
                error->message = (char*)MarshalExtensions.StringToCoTaskMemUTF8(exception.Message);
            #else
                error->message = (char*)Marshal.StringToCoTaskMemUTF8(exception.Message);
            #endif

            error->sqlstate0 = (char)0;
            error->sqlstate1 = (char)0;
            error->sqlstate2 = (char)0;
            error->sqlstate3 = (char)0;
            error->sqlstate4 = (char)0;
            error->vendor_code = 0;
            error->vendor_code = 0;
            error->release = (delegate* unmanaged[Stdcall]<NativeAdbcError*, void>)releaseError.Pointer;
            
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

        private unsafe static AdbcStatusCode ReleaseDriver(NativeAdbcDriver* nativeDriver, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDriver->private_data);
            DriverStub stub = (DriverStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDriver->private_data = null;
            return 0;
        }

        private unsafe static AdbcStatusCode InitDatabase(NativeAdbcDatabase* nativeDatabase, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.Init(ref *error);
        }

        private unsafe static AdbcStatusCode ReleaseDatabase(NativeAdbcDatabase* nativeDatabase, NativeAdbcError* error)
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

        private unsafe static AdbcStatusCode SetConnectionOption(NativeAdbcConnection* nativeConnection, byte* name, byte* value, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.SetOption(name, value, ref *error);
        }

        private unsafe static AdbcStatusCode SetDatabaseOption(NativeAdbcDatabase* nativeDatabase, byte* name, byte* value, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;

            return stub.SetOption(name, value, ref *error);
        }

        private unsafe static AdbcStatusCode InitConnection(NativeAdbcConnection* nativeConnection, NativeAdbcDatabase* database, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.InitConnection(ref *database, ref *error);
        }

        private unsafe static AdbcStatusCode ReleaseConnection(NativeAdbcConnection* nativeConnection, NativeAdbcError* error)
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

        private unsafe static AdbcStatusCode GetConnectionInfo(NativeAdbcConnection* nativeConnection, uint* info_codes, int info_codes_length, CArrowArrayStream* stream, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetConnectionInfo(ref *nativeConnection, *info_codes, info_codes_length, ref *stream, ref *error);
        }

        private unsafe static AdbcStatusCode SetStatementSqlQuery(NativeAdbcStatement* nativeStatement, byte* text, NativeAdbcError* error)
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

        private unsafe static AdbcStatusCode ExecuteStatementQuery(NativeAdbcStatement* nativeStatement, CArrowArrayStream* stream, long* rows, NativeAdbcError* error)
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

        private unsafe static AdbcStatusCode NewStatement(NativeAdbcConnection* nativeConnection, NativeAdbcStatement* nativeStatement, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.NewStatement(ref *nativeStatement, ref *error);
        }

        private unsafe static AdbcStatusCode ReleaseStatement(NativeAdbcStatement* nativeStatement, NativeAdbcError* error)
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

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcDatabase
    {
        public void* private_data;
        public NativeAdbcDriver* private_driver;

        public static NativeAdbcDatabase* Create()
        {
            var ptr = (NativeAdbcDatabase*)Marshal.AllocHGlobal(sizeof(NativeAdbcDatabase));

            ptr->private_data = null;
            ptr->private_driver = null;
            
            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="Create"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void Free(NativeAdbcDatabase* database)
        {
            Marshal.FreeHGlobal((IntPtr)database);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcConnection
    {
        public void* private_data;
        public NativeAdbcDriver* private_driver;
    }

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcStatement
    {
        public void* private_data;
        public NativeAdbcDriver* private_driver;
    }

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcPartitions
    {
        /// <summary>
        /// The number of partitions.
        /// </summary>
        public int num_partitions;

        /// <summary>
        /// The partitions of the result set, where each entry (up to
        /// num_partitions entries) is an opaque identifier that can be
        /// passed to AdbcConnectionReadPartition.
        /// </summary>
        public sbyte** partitions;

        /// <summary>
        /// The length of each corresponding entry in partitions.
        /// </summary>
        public int* partition_lengths;

        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is unintialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// Release the contained partitions.
        ///
        /// Unlike other structures, this is an embedded callback to make it
        /// easier for the driver manager and driver to cooperate.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcPartitions*, void> release; 
    }

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcError
    {
        /// <summary>
        /// The error message.
        /// </summary>
        public char* message;

        /// <summary>
        /// A vendor-specific error code, if applicable.
        /// </summary>
        public int vendor_code;

        /// <summary>
        /// A SQLSTATE error code, if provided, as defined by the
        ///   SQL:2003 standard.  If not set, it should be set to
        ///   "\0\0\0\0\0".
        ///</summary>
        public char sqlstate0;
        public char sqlstate1;
        public char sqlstate2;
        public char sqlstate3;
        public char sqlstate4;

        /// <summary>
        /// Release the contained error.
        ///
        /// Unlike other structures, this is an embedded callback to make it
        /// easier for the driver manager and driver to cooperate.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcError*, void> release; 
    };


    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcDriver
    {
        /// <summary>
        /// Opaque driver-defined state.
        /// This field is NULL if the driver is unintialized/freed (but
        /// it need not have a value even if the driver is initialized).
        /// </summary>
        public void* private_data;

        /// <summary>
        /// Opaque driver manager-defined state.
        /// This field is NULL if the driver is unintialized/freed (but
        /// it need not have a value even if the driver is initialized).
        /// </summary>
        public void* private_manager;

        /// <summary>
        /// Release the driver and perform any cleanup.
        ///
        /// This is an embedded callback to make it easier for the driver
        /// manager and driver to cooperate.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcDriver*, NativeAdbcError*, AdbcStatusCode> release;

        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> DatabaseInit; 
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> DatabaseNew; 
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> DatabaseSetOption;
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> DatabaseRelease; 

        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionCommit; // ConnectionFn
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, int*, int, CArrowArrayStream*,NativeAdbcError*, AdbcStatusCode> ConnectionGetInfo;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode> ConnectionGetObjects;

        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, byte*, byte*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode> ConnectionGetTableSchema;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode> ConnectionGetTableTypes;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> ConnectionInit;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionNew; // ConnectionFn
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> ConnectionSetOption;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, uint*, int, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>  ConnectionReadPartition;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionRelease; // ConnectionFn
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionRollback; // ConnectionFn

        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArray*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode> StatementBind;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode> StatementBindStream;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, long*, NativeAdbcError*, AdbcStatusCode> StatementExecuteQuery;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowSchema*, NativeAdbcPartitions*, long*, NativeAdbcError*, AdbcStatusCode> StatementExecutePartitions;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode> StatementGetParameterSchema;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementNew;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementPrepare; // StatementFn
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementRelease; // StatementFn
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> StatementSetOption;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, byte*, NativeAdbcError*, AdbcStatusCode> StatementSetSqlQuery;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, sbyte*, int, NativeAdbcError*, AdbcStatusCode>  StatementSetSubstraitPlan;
    }


    unsafe delegate AdbcStatusCode DriverRelease(NativeAdbcDriver* driver, NativeAdbcError* error);

    unsafe delegate AdbcStatusCode DatabaseFn(NativeAdbcDatabase* database, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode DatabaseSetOption(NativeAdbcDatabase* database, byte* name, byte* value, NativeAdbcError* error);

    unsafe delegate AdbcStatusCode ConnectionFn(NativeAdbcConnection* connection, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetInfo(NativeAdbcConnection* connection, uint* info_codes, int info_codes_length, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetObjects(NativeAdbcConnection* connection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte column_name, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetTableSchema(NativeAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetTableTypes(NativeAdbcConnection* connection, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionInit(NativeAdbcConnection* connection, NativeAdbcDatabase* database, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionSetOption(NativeAdbcConnection* connection, byte* name, byte* value, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionReadPartition(NativeAdbcConnection* connection, uint* serialized_partition, int serialized_length, CArrowArrayStream* stream, NativeAdbcError* error);

    unsafe delegate AdbcStatusCode StatementBind(NativeAdbcStatement* statement, CArrowArray* array, CArrowSchema* schema, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementBindStream(NativeAdbcStatement* statement, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementExecuteQuery(NativeAdbcStatement* statement, CArrowArrayStream* stream, long* rows, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementExecutePartitions(NativeAdbcStatement* statement, CArrowSchema* schema, NativeAdbcPartitions* partitions, long* rows_affected, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementGetParameterSchema(NativeAdbcStatement* statement, CArrowSchema* schema, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementNew(NativeAdbcConnection* connection, NativeAdbcStatement* statement, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementFn(NativeAdbcStatement* statement, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetOption(NativeAdbcStatement* statement, byte* name, byte* value, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetSqlQuery(NativeAdbcStatement* statement, byte* text, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetSubstraitPlan(NativeAdbcStatement statement, sbyte* plan, int length, NativeAdbcError error);

    unsafe delegate void ErrorRelease(NativeAdbcError* error);

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

        public unsafe AdbcStatusCode NewDatabase(NativeAdbcDatabase* nativeDatabase, NativeAdbcError* error)
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

        public unsafe AdbcStatusCode NewConnection(NativeAdbcConnection* nativeConnection, NativeAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            ConnectionStub stub = new ConnectionStub(_driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeConnection->private_data = (void*) GCHandle.ToIntPtr(handle);

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

        public AdbcStatusCode Init(ref NativeAdbcError error)
        {
            if (database != null)
            {
                return AdbcStatusCode.UnknownError;
            }

            database = _driver.Open(options);
            return AdbcStatusCode.Success;
        }

        public unsafe AdbcStatusCode SetOption(byte* name, byte* value, ref NativeAdbcError error)
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

        public AdbcStatusCode OpenConnection(Dictionary<string, string> options, ref NativeAdbcError error, out AdbcConnection connection)
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

        public unsafe AdbcStatusCode SetOption(byte* name, byte* value, ref NativeAdbcError error)
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

        public unsafe AdbcStatusCode GetConnectionInfo(ref NativeAdbcConnection connection, uint info_codes, int info_codes_length, ref CArrowArrayStream stream, ref NativeAdbcError error)
        {
            if (connection.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            // TODO: logic
            //connection.


            return AdbcStatusCode.Success;
        }

        public unsafe AdbcStatusCode InitConnection(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            if (nativeDatabase.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.OpenConnection(options, ref error, out connection);
        }

        public unsafe AdbcStatusCode NewStatement(ref NativeAdbcStatement nativeStatement, ref NativeAdbcError error)
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

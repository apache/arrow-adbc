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
        private static unsafe readonly NativeDelegate<ErrorRelease> releaseError = new NativeDelegate<ErrorRelease>(ReleaseError);
        private static readonly NativeDelegate<DriverRelease> releaseDriver = new NativeDelegate<DriverRelease>(ReleaseDriver);

        private static readonly NativeDelegate<DatabaseFn> databaseInit = new NativeDelegate<DatabaseFn>(InitDatabase);
        private static readonly NativeDelegate<DatabaseFn> databaseRelease = new NativeDelegate<DatabaseFn>(ReleaseDatabase);
        private static readonly NativeDelegate<DatabaseSetOption> databaseSetOption = new NativeDelegate<DatabaseSetOption>(SetDatabaseOption);

        private static readonly NativeDelegate<ConnectionInit> connectionInit = new NativeDelegate<ConnectionInit>(InitConnection);
        private static readonly NativeDelegate<ConnectionFn> connectionRelease = new NativeDelegate<ConnectionFn>(ReleaseConnection);
        private static readonly NativeDelegate<ConnectionGetInfo> connectionGetInfo = new NativeDelegate<ConnectionGetInfo>(GetConnectionInfo);
        private static readonly NativeDelegate<ConnectionSetOption> connectionSetOption = new NativeDelegate<ConnectionSetOption>(SetConnectionOption);
        
        private static unsafe readonly NativeDelegate<StatementExecuteQuery> statementExecuteQuery = new NativeDelegate<StatementExecuteQuery>(ExecuteStatementQuery);
        private static readonly NativeDelegate<StatementNew> statementNew = new NativeDelegate<StatementNew>(NewStatement);
        private static readonly NativeDelegate<StatementFn> statementRelease = new NativeDelegate<StatementFn>(ReleaseStatement);
        private static readonly NativeDelegate<StatementSetSqlQuery> statementSetSqlQuery = new NativeDelegate<StatementSetSqlQuery>(SetStatementSqlQuery);

        public static unsafe AdbcStatusCode AdbcDriverInit(int version, NativeAdbcDriver* nativeDriver, NativeAdbcError* error, AdbcDriver driver)
        {
            DriverStub stub = new DriverStub(driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDriver->private_data = (void*)GCHandle.ToIntPtr(handle);
            nativeDriver->release = (delegate* unmanaged[Stdcall]<NativeAdbcDriver*, NativeAdbcError*, AdbcStatusCode>)releaseDriver.Pointer;

            nativeDriver->DatabaseInit = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)databaseInit.Pointer;
            nativeDriver->DatabaseNew = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)stub.newDatabase.Pointer;
            nativeDriver->DatabaseSetOption = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, char*, char*, NativeAdbcError*, AdbcStatusCode>) databaseSetOption.Pointer;
            nativeDriver->DatabaseRelease = (delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)databaseRelease.Pointer;

            nativeDriver->ConnectionCommit = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;
            //nativeDriver->ConnectionGetInfo = (delegate* unmanaged[Stdcall]<NativeAdbcConnection *, int*, int, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>)connectionGetInfo.Pointer;
            //nativeDriver->ConnectionGetTableSchema = null;
            //nativeDriver->ConnectionGetTableTypes = null;
            nativeDriver->ConnectionInit = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)connectionInit.Pointer;
            nativeDriver->ConnectionNew = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)stub.newConnection.Pointer;
            nativeDriver->ConnectionSetOption = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, char*, char*, NativeAdbcError*, AdbcStatusCode>)connectionSetOption.Pointer;
            //nativeDriver->ConnectionReadPartition = null;
            nativeDriver->ConnectionRelease = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;
            //nativeDriver->ConnectionRollback = null;

           // nativeDriver->StatementBind = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArray*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode>)
            nativeDriver->StatementNew = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode>)statementNew.Pointer;
            nativeDriver->StatementSetSqlQuery = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, char*, AdbcStatusCode>)statementSetSqlQuery.Pointer;
            nativeDriver->StatementExecuteQuery = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, long*, NativeAdbcError*, AdbcStatusCode>)statementExecuteQuery.Pointer;
            nativeDriver->StatementPrepare = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode>)statementRelease.Pointer;
            nativeDriver->StatementRelease = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode>)statementRelease.Pointer;
            
            return 0;
        }

        private static unsafe void ReleaseError(NativeAdbcError* error)
        {
            if (error != null && ((IntPtr)error->message) != IntPtr.Zero)
            {
                Marshal.FreeCoTaskMem((IntPtr)error->message);
            }
        }

        private static unsafe AdbcStatusCode SetError(NativeAdbcError* error, Exception exception)
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

        private static unsafe AdbcStatusCode ReleaseDriver(ref NativeAdbcDriver nativeDriver, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDriver.private_data);
            DriverStub stub = (DriverStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDriver.private_data = null;
            return 0;
        }

        private static unsafe AdbcStatusCode InitDatabase(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.Init(ref error);
        }

        private static unsafe AdbcStatusCode ReleaseDatabase(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            if (nativeDatabase.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDatabase.private_data = null;
            return AdbcStatusCode.Success;
        }

        private static unsafe AdbcStatusCode SetConnectionOption(ref NativeAdbcConnection nativeConnection, IntPtr name, IntPtr value, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.SetOption(name, value, ref error);
        }

        private static unsafe AdbcStatusCode SetDatabaseOption(ref NativeAdbcDatabase nativeDatabase, IntPtr name, IntPtr value, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.SetOption(name, value, ref error);
        }

        private static unsafe AdbcStatusCode InitConnection(ref NativeAdbcConnection nativeConnection, ref NativeAdbcDatabase database, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.InitConnection(ref database, ref error);
        }

        private static unsafe AdbcStatusCode ReleaseConnection(ref NativeAdbcConnection nativeConnection, ref NativeAdbcError error)
        {
            if (nativeConnection.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeConnection.private_data = null;
            return AdbcStatusCode.Success;
        }

        private static unsafe AdbcStatusCode GetConnectionInfo(ref NativeAdbcConnection nativeConnection, uint x, IntPtr y, ref CArrowArrayStream stream, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetConnectionInfo(ref nativeConnection, x, y, ref stream, ref error);
        }

        private static unsafe AdbcStatusCode SetStatementSqlQuery(ref NativeAdbcStatement nativeStatement, IntPtr text, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement.private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;

#if NETSTANDARD
            stub.SqlQuery = MarshalExtensions.PtrToStringUTF8(text);
#else
            stub.SqlQuery = Marshal.PtrToStringUTF8(text);
#endif

            return AdbcStatusCode.Success;
        }

        private static unsafe AdbcStatusCode ExecuteStatementQuery(ref NativeAdbcStatement nativeStatement, CArrowArrayStream* stream, long* rows, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement.private_data);
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

        private static unsafe AdbcStatusCode NewStatement(ref NativeAdbcConnection nativeConnection, ref NativeAdbcStatement nativeStatement, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.NewStatement(ref nativeStatement, ref error);
        }

        private static unsafe AdbcStatusCode ReleaseStatement(ref NativeAdbcStatement nativeStatement, ref NativeAdbcError error)
        {
            if (nativeStatement.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement.private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeStatement.private_data = null;
            return AdbcStatusCode.Success;
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcDatabase
    {
        public void* private_data;
        public NativeAdbcDriver* private_driver;
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
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, char*, char*, NativeAdbcError*, AdbcStatusCode> DatabaseSetOption;
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> DatabaseRelease; 

        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionCommit; // ConnectionFn
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, int*, int, CArrowArrayStream*,NativeAdbcError*, AdbcStatusCode> ConnectionGetInfo;
        public IntPtr ConnectionGetObjects;
        public IntPtr ConnectionGetTableSchema;
        public IntPtr ConnectionGetTableTypes;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> ConnectionInit;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionNew; // ConnectionFn
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, char*, char*, NativeAdbcError*, AdbcStatusCode> ConnectionSetOption;
        public IntPtr ConnectionReadPartition;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionRelease; // ConnectionFn
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionRollback; // ConnectionFn

        public IntPtr StatementBind;
        public IntPtr StatementBindStream;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, long*, NativeAdbcError*, AdbcStatusCode> StatementExecuteQuery;
        public IntPtr StatementExecutePartitions;
        public IntPtr StatementGetParameterSchema;
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementNew;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementPrepare; // StatementFn
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementRelease; // StatementFn
        public IntPtr StatementSetOption;
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, char*, AdbcStatusCode> StatementSetSqlQuery;
        public IntPtr StatementSetSubstraitPlan;
    }


    delegate AdbcStatusCode DriverRelease(ref NativeAdbcDriver driver, ref NativeAdbcError error);

    delegate AdbcStatusCode DatabaseFn(ref NativeAdbcDatabase database, ref NativeAdbcError error);
    delegate AdbcStatusCode DatabaseSetOption(ref NativeAdbcDatabase database, IntPtr name, IntPtr value, ref NativeAdbcError error);

    delegate AdbcStatusCode ConnectionFn(ref NativeAdbcConnection connection, ref NativeAdbcError error);
    delegate AdbcStatusCode ConnectionGetInfo(ref NativeAdbcConnection connection, uint x, IntPtr y, ref CArrowArrayStream stream, ref NativeAdbcError error);
    delegate AdbcStatusCode ConnectionGetObjects(ref NativeAdbcConnection connection, int x, IntPtr y, IntPtr z, IntPtr a, IntPtr b, IntPtr c, ref CArrowArrayStream stream, ref NativeAdbcError error);
    delegate AdbcStatusCode ConnectionGetTableSchema(ref NativeAdbcConnection connection, IntPtr x, IntPtr y, IntPtr z, ref CArrowArrayStream stream, ref NativeAdbcError error);
    delegate AdbcStatusCode ConnectionGetTableTypes(ref NativeAdbcConnection connection, ref CArrowArrayStream stream, ref NativeAdbcError error);
    delegate AdbcStatusCode ConnectionInit(ref NativeAdbcConnection connection, ref NativeAdbcDatabase database, ref NativeAdbcError error);
    delegate AdbcStatusCode ConnectionSetOption(ref NativeAdbcConnection connection, IntPtr name, IntPtr value, ref NativeAdbcError error);
    delegate AdbcStatusCode ConnectionReadPartition(ref NativeAdbcConnection connection, IntPtr x, IntPtr y, ref CArrowArrayStream stream, ref NativeAdbcError error);

    delegate AdbcStatusCode StatementBind(ref NativeAdbcStatement statement, ref CArrowArray array, ref CArrowSchema schema, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementBindStream(ref NativeAdbcStatement statement, ref CArrowArrayStream stream, ref NativeAdbcError error);
    unsafe delegate AdbcStatusCode StatementExecuteQuery(ref NativeAdbcStatement statement, CArrowArrayStream* stream, long* rows, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementExecutePartitions(ref NativeAdbcStatement statement, ref CArrowSchema schema, ref NativeAdbcPartitions partitions, ref long rows, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementGetParameterSchema(ref NativeAdbcStatement statement, ref CArrowSchema schema, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementNew(ref NativeAdbcConnection connection, ref NativeAdbcStatement statement, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementFn(ref NativeAdbcStatement statement, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementSetOption(ref NativeAdbcStatement statement, IntPtr name, IntPtr value, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementSetSqlQuery(ref NativeAdbcStatement statement, IntPtr text, ref NativeAdbcError error);
    delegate AdbcStatusCode StatementSetSubstraitPlan(ref NativeAdbcStatement statement, IntPtr plan, IntPtr length, ref NativeAdbcError error);

    unsafe delegate void ErrorRelease(NativeAdbcError* error);

    sealed class DriverStub : IDisposable
    {
        readonly AdbcDriver _driver;
        public readonly NativeDelegate<DatabaseFn> newDatabase;
        public readonly NativeDelegate<ConnectionFn> newConnection;

        public DriverStub(AdbcDriver driver)
        {
            _driver = driver;
            newDatabase = new NativeDelegate<DatabaseFn>(NewDatabase);
            newConnection = new NativeDelegate<ConnectionFn>(NewConnection);
        }

        public unsafe AdbcStatusCode NewDatabase(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            if (nativeDatabase.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            DatabaseStub stub = new DatabaseStub(_driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDatabase.private_data = (void*)GCHandle.ToIntPtr(handle);

            return AdbcStatusCode.Success;
        }

        public unsafe AdbcStatusCode NewConnection(ref NativeAdbcConnection nativeConnection, ref NativeAdbcError error)
        {
            if (nativeConnection.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            ConnectionStub stub = new ConnectionStub(_driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeConnection.private_data = (void*) GCHandle.ToIntPtr(handle);

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

        public AdbcStatusCode SetOption(IntPtr name, IntPtr value, ref NativeAdbcError error)
        {
            #if NETSTANDARD
                options[MarshalExtensions.PtrToStringUTF8(name)] = MarshalExtensions.PtrToStringUTF8(value);
            #else
                options[Marshal.PtrToStringUTF8(name)] = Marshal.PtrToStringUTF8(value);
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

        public AdbcStatusCode SetOption(IntPtr name, IntPtr value, ref NativeAdbcError error)
        {
            #if NETSTANDARD
                options[MarshalExtensions.PtrToStringUTF8(name)] = MarshalExtensions.PtrToStringUTF8(value);
            #else
                options[Marshal.PtrToStringUTF8(name)] = Marshal.PtrToStringUTF8(value);
            #endif

            return AdbcStatusCode.Success;
        }

        public void Dispose()
        {
            connection?.Dispose();
            connection = null;
        }

        public unsafe AdbcStatusCode GetConnectionInfo(ref NativeAdbcConnection connection, uint x, IntPtr y, ref CArrowArrayStream stream, ref NativeAdbcError error)
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

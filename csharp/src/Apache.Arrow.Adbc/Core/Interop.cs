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
    public static class AdbcInterop
    {
        static NativeDelegate<DriverRelease> releaseDriver = new NativeDelegate<DriverRelease>(ReleaseDriver);

        static NativeDelegate<DatabaseFn> databaseInit = new NativeDelegate<DatabaseFn>(InitDatabase);
        static NativeDelegate<DatabaseFn> databaseRelease = new NativeDelegate<DatabaseFn>(ReleaseDatabase);
        static NativeDelegate<DatabaseSetOption> databaseSetOption = new NativeDelegate<DatabaseSetOption>(SetDatabaseOption);
        static NativeDelegate<ConnectionInit> connectionInit = new NativeDelegate<ConnectionInit>(InitConnection);
        static NativeDelegate<ConnectionFn> connectionRelease = new NativeDelegate<ConnectionFn>(ReleaseConnection);
        static NativeDelegate<ConnectionSetOption> connectionSetOption = new NativeDelegate<ConnectionSetOption>(SetConnectionOption);
        static unsafe NativeDelegate<StatementExecuteQuery> statementExecuteQuery = new NativeDelegate<StatementExecuteQuery>(ExecuteStatementQuery);
        static NativeDelegate<StatementNew> statementNew = new NativeDelegate<StatementNew>(NewStatement);
        static NativeDelegate<StatementFn> statementRelease = new NativeDelegate<StatementFn>(ReleaseStatement);
        static NativeDelegate<StatementSetSqlQuery> statementSetSqlQuery = new NativeDelegate<StatementSetSqlQuery>(SetStatementSqlQuery);

        unsafe static IntPtr errorRelease = new NativeDelegate<ErrorRelease>(ReleaseError);

        public unsafe static AdbcStatusCode AdbcDriverInit(int version, NativeAdbcDriver* nativeDriver, NativeAdbcError* error, AdbcDriver driver)
        {
            DriverStub stub = new DriverStub(driver);
            GCHandle handle = GCHandle.Alloc(stub);
            (*nativeDriver).private_data = GCHandle.ToIntPtr(handle);
            (*nativeDriver).release = releaseDriver;
            (*nativeDriver).DatabaseNew = stub.newDatabase;
            (*nativeDriver).DatabaseInit = databaseInit;
            (*nativeDriver).DatabaseRelease = databaseRelease;
            (*nativeDriver).DatabaseSetOption = databaseSetOption;
            (*nativeDriver).ConnectionNew = stub.newConnection;
            (*nativeDriver).ConnectionInit = connectionInit;
            (*nativeDriver).ConnectionRelease = connectionRelease;
            (*nativeDriver).ConnectionSetOption = connectionSetOption;
            (*nativeDriver).StatementNew = statementNew;
            (*nativeDriver).StatementSetSqlQuery = statementSetSqlQuery;
            (*nativeDriver).StatementExecuteQuery = statementExecuteQuery;
            (*nativeDriver).StatementRelease = statementRelease;
            return 0;
        }

        unsafe static void ReleaseError(NativeAdbcError* error)
        {
            if (error != null && (*error).message != IntPtr.Zero)
            {
                Marshal.FreeCoTaskMem((*error).message);
            }
        }

        unsafe static AdbcStatusCode SetError(NativeAdbcError* error, Exception exception)
        {
            ReleaseError(error);

#if NETSTANDARD
            (*error).message = MarshalExtensions.StringToCoTaskMemUTF8(exception.Message);
#else
            (*error).message = Marshal.StringToCoTaskMemUTF8(exception.Message);
#endif

            (*error).sqlstate0 = (char)0;
            (*error).sqlstate1 = (char)0;
            (*error).sqlstate2 = (char)0;
            (*error).sqlstate3 = (char)0;
            (*error).sqlstate4 = (char)0;
            (*error).vendor_code = 0;
            (*error).vendor_code = 0;
            (*error).release = errorRelease;

            return AdbcStatusCode.UnknownError;
        }

        sealed class PinnedArray : IDisposable
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

        static IntPtr FromDisposable(IDisposable d)
        {
            GCHandle gch = GCHandle.Alloc(d);
            return GCHandle.ToIntPtr(gch);
        }

        static void Dispose(ref IntPtr p)
        {
            GCHandle gch = GCHandle.FromIntPtr(p);
            ((IDisposable)gch.Target).Dispose();
            gch.Free();
            p = IntPtr.Zero;
        }

        static AdbcStatusCode ReleaseDriver(ref NativeAdbcDriver nativeDriver, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeDriver.private_data);
            DriverStub stub = (DriverStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDriver.private_data = IntPtr.Zero;
            return 0;
        }

        static AdbcStatusCode InitDatabase(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.Init(ref error);
        }

        static AdbcStatusCode ReleaseDatabase(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            if (nativeDatabase.private_data == IntPtr.Zero)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr(nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeDatabase.private_data = IntPtr.Zero;
            return AdbcStatusCode.Success;
        }

        static AdbcStatusCode SetDatabaseOption(ref NativeAdbcDatabase nativeDatabase, IntPtr name, IntPtr value, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.SetOption(name, value, ref error);
        }

        static AdbcStatusCode InitConnection(ref NativeAdbcConnection nativeConnection, ref NativeAdbcDatabase database, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.InitConnection(ref database, ref error);
        }

        static AdbcStatusCode ReleaseConnection(ref NativeAdbcConnection nativeConnection, ref NativeAdbcError error)
        {
            if (nativeConnection.private_data == IntPtr.Zero)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr(nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeConnection.private_data = IntPtr.Zero;
            return AdbcStatusCode.Success;
        }

        static AdbcStatusCode SetConnectionOption(ref NativeAdbcConnection nativeConnection, IntPtr name, IntPtr value, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.SetOption(name, value, ref error);
        }

        static AdbcStatusCode SetStatementSqlQuery(ref NativeAdbcStatement nativeStatement, IntPtr text, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeStatement.private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;

#if NETSTANDARD
            stub.SqlQuery = MarshalExtensions.PtrToStringUTF8(text);
#else
            stub.SqlQuery = Marshal.PtrToStringUTF8(text);
#endif

            return AdbcStatusCode.Success;
        }

        static unsafe AdbcStatusCode ExecuteStatementQuery(ref NativeAdbcStatement nativeStatement, CArrowArrayStream* stream, long* rows, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeStatement.private_data);
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

        static AdbcStatusCode NewStatement(ref NativeAdbcConnection nativeConnection, ref NativeAdbcStatement nativeStatement, ref NativeAdbcError error)
        {
            GCHandle gch = GCHandle.FromIntPtr(nativeConnection.private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.NewStatement(ref nativeStatement, ref error);
        }

        static AdbcStatusCode ReleaseStatement(ref NativeAdbcStatement nativeStatement, ref NativeAdbcError error)
        {
            if (nativeStatement.private_data == IntPtr.Zero)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr(nativeStatement.private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;
            stub.Dispose();
            gch.Free();
            nativeStatement.private_data = IntPtr.Zero;
            return AdbcStatusCode.Success;
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct NativeAdbcDatabase
    {
        public IntPtr private_data;
        public IntPtr private_driver;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct NativeAdbcConnection
    {
        public IntPtr private_data;
        public IntPtr private_driver;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct NativeAdbcStatement
    {
        public IntPtr private_data;
        public IntPtr private_driver;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct NativeAdbcPartitions
    {
        /// \brief The number of partitions.
        public IntPtr num_partitions;

        /// \brief The partitions of the result set, where each entry (up to
        ///   num_partitions entries) is an opaque identifier that can be
        ///   passed to AdbcConnectionReadPartition.
        public IntPtr partitions;

        /// \brief The length of each corresponding entry in partitions.
        public IntPtr partition_lengths;

        /// \brief Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is unintialized/freed.
        public IntPtr private_data;

        /// \brief Release the contained partitions.
        ///
        /// Unlike other structures, this is an embedded callback to make it
        /// easier for the driver manager and driver to cooperate.
        public IntPtr release; // void (*release)(struct AdbcPartitions* partitions);
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct NativeAdbcError
    {
        /// \brief The error message.
        public IntPtr message;

        /// \brief A vendor-specific error code, if applicable.
        public int vendor_code;

        /// \brief A SQLSTATE error code, if provided, as defined by the
        ///   SQL:2003 standard.  If not set, it should be set to
        ///   "\0\0\0\0\0".
        public char sqlstate0;
        public char sqlstate1;
        public char sqlstate2;
        public char sqlstate3;
        public char sqlstate4;

        /// \brief Release the contained error.
        ///
        /// Unlike other structures, this is an embedded callback to make it
        /// easier for the driver manager and driver to cooperate.
        public IntPtr release; // void (*release)(struct AdbcError* error);
    };


    [StructLayout(LayoutKind.Sequential)]
    public struct NativeAdbcDriver
    {
        public IntPtr private_data;
        public IntPtr private_manager;

        public IntPtr release; // DriverRelease

        public IntPtr DatabaseInit; // DatabaseFn
        public IntPtr DatabaseNew; // DatabaseFn
        public IntPtr DatabaseSetOption;
        public IntPtr DatabaseRelease; // DatabaseFn

        public IntPtr ConnectionCommit; // ConnectionFn
        public IntPtr ConnectionGetInfo;
        public IntPtr ConnectionGetObjects;
        public IntPtr ConnectionGetTableSchema;
        public IntPtr ConnectionGetTableTypes;
        public IntPtr ConnectionInit;
        public IntPtr ConnectionNew; // ConnectionFn
        public IntPtr ConnectionSetOption;
        public IntPtr ConnectionReadPartition;
        public IntPtr ConnectionRelease; // ConnectionFn
        public IntPtr ConnectionRollback; // ConnectionFn

        public IntPtr StatementBind;
        public IntPtr StatementBindStream;
        public IntPtr StatementExecuteQuery;
        public IntPtr StatementExecutePartitions;
        public IntPtr StatementGetParameterSchema;
        public IntPtr StatementNew;
        public IntPtr StatementPrepare; // StatementFn
        public IntPtr StatementRelease; // StatementFn
        public IntPtr StatementSetOption;
        public IntPtr StatementSetSqlQuery;
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

        public AdbcStatusCode NewDatabase(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            if (nativeDatabase.private_data != IntPtr.Zero)
            {
                return AdbcStatusCode.UnknownError;
            }

            DatabaseStub stub = new DatabaseStub(_driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDatabase.private_data = GCHandle.ToIntPtr(handle);

            return AdbcStatusCode.Success;
        }

        public AdbcStatusCode NewConnection(ref NativeAdbcConnection nativeConnection, ref NativeAdbcError error)
        {
            if (nativeConnection.private_data != IntPtr.Zero)
            {
                return AdbcStatusCode.UnknownError;
            }

            ConnectionStub stub = new ConnectionStub(_driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeConnection.private_data = GCHandle.ToIntPtr(handle);

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

        public AdbcStatusCode InitConnection(ref NativeAdbcDatabase nativeDatabase, ref NativeAdbcError error)
        {
            if (nativeDatabase.private_data == IntPtr.Zero)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr(nativeDatabase.private_data);
            DatabaseStub stub = (DatabaseStub)gch.Target;
            return stub.OpenConnection(options, ref error, out connection);
        }

        public AdbcStatusCode NewStatement(ref NativeAdbcStatement nativeStatement, ref NativeAdbcError error)
        {
            if (connection == null)
            {
                return AdbcStatusCode.UnknownError;
            }
            if (nativeStatement.private_data != IntPtr.Zero)
            {
                return AdbcStatusCode.UnknownError;
            }

            AdbcStatement statement = connection.CreateStatement();
            GCHandle handle = GCHandle.Alloc(statement);
            nativeStatement.private_data = GCHandle.ToIntPtr(handle);

            return AdbcStatusCode.Success;
        }
    }
}

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
using Apache.Arrow.Ipc;

#if NETSTANDARD
using Apache.Arrow.Adbc.Extensions;
#endif

namespace Apache.Arrow.Adbc
{
    internal static class AdbcInterop
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
            nativeDriver->ConnectionGetInfo = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, int, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>)connectionGetInfo.Pointer;
            nativeDriver->ConnectionGetObjects = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>)connectionGetObjects.Pointer;
            nativeDriver->ConnectionGetTableSchema = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*,  byte*, byte*, byte*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode>)connectionGetTableSchema.Pointer;
            nativeDriver->ConnectionGetTableTypes = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>) connectionGetTableTypes.Pointer;
            nativeDriver->ConnectionInit = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode>)connectionInit.Pointer;
            nativeDriver->ConnectionNew = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)stub.newConnection.Pointer;
            nativeDriver->ConnectionSetOption = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, byte*, NativeAdbcError*, AdbcStatusCode>)connectionSetOption.Pointer;
            nativeDriver->ConnectionReadPartition = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, int, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>) connectionReadPartition.Pointer;
            nativeDriver->ConnectionRelease = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;
            nativeDriver->ConnectionRollback = (delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode>)connectionRelease.Pointer;

            nativeDriver->StatementBind = (delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArray*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode>)statementBind.Pointer;
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
                Marshal.FreeHGlobal((IntPtr)error->message);
            }
        }

        private unsafe static AdbcStatusCode SetError(NativeAdbcError* error, Exception exception)
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


        private unsafe static AdbcStatusCode GetConnectionObjects(NativeAdbcConnection* nativeConnection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, NativeAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetObjects(ref *nativeConnection, depth, catalog, db_schema, table_name, table_type, column_name, stream, ref *error);
        }

        private unsafe static AdbcStatusCode GetConnectionTableTypes(NativeAdbcConnection* nativeConnection, CArrowArrayStream* stream, NativeAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetTableTypes(ref *nativeConnection, stream, ref *error);
        }

        private unsafe static AdbcStatusCode GetConnectionTableSchema(NativeAdbcConnection* nativeConnection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, NativeAdbcError* error)
        {
            if (nativeConnection->private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetTableSchema(ref *nativeConnection, catalog, db_schema, table_name, schema, ref *error);
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

        private unsafe static AdbcStatusCode ReadConnectionPartition(NativeAdbcConnection* nativeConnection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.ReadPartition(ref *nativeConnection, serialized_partition, serialized_length, stream, ref *error);
        }

        private unsafe static AdbcStatusCode GetConnectionInfo(NativeAdbcConnection* nativeConnection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
            ConnectionStub stub = (ConnectionStub)gch.Target;
            return stub.GetInfo(ref *nativeConnection, info_codes, info_codes_length, stream, ref *error);
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

        private unsafe static AdbcStatusCode BindStatement(NativeAdbcStatement* nativeStatement, CArrowArray* array, CArrowSchema* cschema, NativeAdbcError* error)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
            AdbcStatement stub = (AdbcStatement)gch.Target;

            Schema schema = CArrowSchemaImporter.ImportSchema(cschema);

            RecordBatch batch = CArrowArrayImporter.ImportRecordBatch(array, schema);

            stub.Bind(batch, schema);

            return 0;
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

    /// <summary>
    /// Clients first initialize a database, then create a connection.  
    /// This gives the implementation a place to initialize and
    /// own any common connection state.  For example, in-memory databases
    /// can place ownership of the actual database in this object.
    /// </summary>
    /// <remarks>
    /// An instance of a database.
    ///
    /// Must be kept alive as long as any connections exist.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcDatabase
    {
        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is unintialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// The associated driver (used by the driver manager to help track state).
        /// </summary>
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

    /// <summary>
    /// An active database connection.
    ///
    /// Provides methods for query execution, managing prepared
    /// statements, using transactions, and so on.
    ///
    /// Connections are not required to be thread-safe, but they can be
    /// used from multiple threads so long as clients take care to
    /// serialize accesses to a connection.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcConnection
    {
        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is unintialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// The associated driver (used by the driver manager to help
        ///   track state).
        /// </summary>
        public NativeAdbcDriver* private_driver;
    }

    /// <summary>
    /// A container for all state needed to execute a database
    /// query, such as the query itself, parameters for prepared
    /// statements, driver parameters, etc.
    ///
    /// Statements may represent queries or prepared statements.
    ///
    /// Statements may be used multiple times and can be reconfigured
    /// (e.g. they can be reused to execute multiple different queries).
    /// However, executing a statement (and changing certain other state)
    /// will invalidate result sets obtained prior to that execution.
    ///
    /// Multiple statements may be created from a single connection.
    /// However, the driver may block or error if they are used
    /// concurrently (whether from a single thread or multiple threads).
    ///
    /// Statements are not required to be thread-safe, but they can be
    /// used from multiple threads so long as clients take care to
    /// serialize accesses to a statement.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcStatement
    {
        /// <summary>
        /// Opaque implementation-defined state.
        /// This field is NULLPTR iff the connection is unintialized/freed.
        /// </summary>
        public void* private_data;

        /// <summary>
        /// The associated driver (used by the driver manager to help track state).
        /// </summary>
        public NativeAdbcDriver* private_driver;
    }

    /// <summary>
    /// The partitions of a distributed/partitioned result set.
    /// </summary>
    /// <remarks>
    /// Some backends may internally partition the results. These
    /// partitions are exposed to clients who may wish to integrate them
    /// with a threaded or distributed execution model, where partitions
    /// can be divided among threads or machines and fetched in parallel.
    ///
    /// To use partitioning, execute the statement with
    /// AdbcStatementExecutePartitions to get the partition descriptors.
    /// Call AdbcConnectionReadPartition to turn the individual
    /// descriptors into ArrowArrayStream instances.  This may be done on
    /// a different connection than the one the partition was created
    /// with, or even in a different process on another machine.
    ///
    /// Drivers are not required to support partitioning.
    /// </remarks>
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
        public byte** partitions;

        /// <summary>
        /// The length of each corresponding entry in partitions.
        /// </summary>
        public nuint* partition_lengths;

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

    /// <summary>
    /// A detailed error message for an operation.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct NativeAdbcError
    {
        /// <summary>
        /// The error message.
        /// </summary>
        public byte* message;

        /// <summary>
        /// A vendor-specific error code, if applicable.
        /// </summary>
        public int vendor_code;

        /// <summary>
        /// A SQLSTATE error code, if provided, as defined by the
        ///   SQL:2003 standard.  If not set, it should be set to
        ///   "\0\0\0\0\0".
        ///   
        /// This is the first value.
        ///</summary>
        public byte sqlstate0;

        /// <summary>
        /// A SQLSTATE error code, if provided, as defined by the
        ///   SQL:2003 standard.  If not set, it should be set to
        ///   "\0\0\0\0\0".
        ///   
        /// This is the second value.
        ///</summary>
        public byte sqlstate1;

        /// <summary>
        /// A SQLSTATE error code, if provided, as defined by the
        ///   SQL:2003 standard.  If not set, it should be set to
        ///   "\0\0\0\0\0".
        ///   
        /// This is the third value.
        ///</summary>
        public byte sqlstate2;

        /// <summary>
        /// A SQLSTATE error code, if provided, as defined by the
        ///   SQL:2003 standard.  If not set, it should be set to
        ///   "\0\0\0\0\0".
        ///   
        /// This is the fourth value.
        ///</summary>
        public byte sqlstate3;

        /// <summary>
        /// A SQLSTATE error code, if provided, as defined by the
        ///   SQL:2003 standard.  If not set, it should be set to
        ///   "\0\0\0\0\0".
        ///   
        /// This is the last value.
        ///</summary>
        public byte sqlstate4;

        /// <summary>
        /// Release the contained error.
        ///
        /// Unlike other structures, this is an embedded callback to make it
        /// easier for the driver manager and driver to cooperate.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcError*, void> release; 
    };

    /// <summary>
    /// An instance of an initialized database driver.
    /// </summary>
    /// <remarks>
    /// This provides a common interface for vendor-specific driver
    /// initialization routines. Drivers should populate this struct, and
    /// applications can call ADBC functions through this struct, without
    /// worrying about multiple definitions of the same symbol.
    /// </remarks>
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

        /// <summary>
        /// Finish setting options and initialize the database.
        ///
        /// Some drivers may support setting options after initialization
        /// as well.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> DatabaseInit;

        /// <summary>
        /// Allocate a new (but uninitialized) database.
        ///
        /// Callers pass in a zero-initialized AdbcDatabase.
        ///
        /// Drivers should allocate their internal data structure and set the private_data
        /// field to point to the newly allocated struct. This struct should be released
        /// when AdbcDatabaseRelease is called.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> DatabaseNew;

        /// <summary>
        /// Set a byte* option.
        ///
        /// Options may be set before AdbcDatabaseInit.  Some drivers may
        /// support setting options after initialization as well.
        ///
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> DatabaseSetOption;

        /// <summary>
        /// Destroy this database. No connections may exist.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> DatabaseRelease;

        /// <summary>
        /// Commit any pending transactions. Only used if autocommit is disabled.
        ///
        /// Behavior is undefined if this is mixed with SQL transaction statements.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionCommit; // ConnectionFn

        /// <summary>
        /// Get metadata about the database/driver.
        ///
        /// The result is an Arrow dataset with the following schema:
        ///
        /// Field Name                  | Field Type
        /// ----------------------------|------------------------
        /// info_name                   | uint32 not null
        /// info_value                  | INFO_SCHEMA
        ///
        /// INFO_SCHEMA is a dense union with members:
        ///
        /// Field Name (Type Code)      | Field Type
        /// ----------------------------|------------------------
        /// string_value (0)            | utf8
        /// bool_value (1)              | bool
        /// int64_value (2)             | int64
        /// int32_bitmask (3)           | int32
        /// string_list (4)             | list<utf8>
        /// int32_to_int32_list_map (5) | map<int32, list<int32>>
        ///
        /// Each metadatum is identified by an integer code.  The recognized
        /// codes are defined as constants.  Codes [0, 10_000) are reserved
        /// for ADBC usage.  Drivers/vendors will ignore requests for
        /// unrecognized codes (the row will be omitted from the result).
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, int, CArrowArrayStream*,NativeAdbcError*, AdbcStatusCode> ConnectionGetInfo;

        /// <summary>
        ///  Get a hierarchical view of all catalogs, database schemas,
        ///   tables, and columns.
        ///
        /// The result is an Arrow dataset with the following schema:
        ///
        /// | Field Name               | Field Type              |
        /// |--------------------------|-------------------------|
        /// | catalog_name             | utf8                    |
        /// | catalog_db_schemas       | list<DB_SCHEMA_SCHEMA>  |
        ///
        /// DB_SCHEMA_SCHEMA is a Struct with fields:
        ///
        /// | Field Name               | Field Type              |
        /// |--------------------------|-------------------------|
        /// | db_schema_name           | utf8                    |
        /// | db_schema_tables         | list<TABLE_SCHEMA>      |
        ///
        /// TABLE_SCHEMA is a Struct with fields:
        ///
        /// | Field Name               | Field Type              |
        /// |--------------------------|-------------------------|
        /// | table_name               | utf8 not null           |
        /// | table_type               | utf8 not null           |
        /// | table_columns            | list<COLUMN_SCHEMA>     |
        /// | table_constraints        | list<CONSTRAINT_SCHEMA> |
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode> ConnectionGetObjects;

        /// <summary>
        /// Get the Arrow schema of a table.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*,  byte*, byte*, byte*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode> ConnectionGetTableSchema;

        /// <summary>
        /// Get a list of table types in the database.
        ///
        /// The result is an Arrow dataset with the following schema:
        ///
        /// Field Name     | Field Type
        /// ---------------|--------------
        /// table_type     | utf8 not null
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode> ConnectionGetTableTypes;

        /// <summary>
        /// Finish setting options and initialize the connection.
        ///
        /// Some drivers may support setting options after initialization
        /// as well.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcDatabase*, NativeAdbcError*, AdbcStatusCode> ConnectionInit;

        /// <summary>
        /// Allocate a new (but uninitialized) connection.
        ///
        /// Callers pass in a zero-initialized AdbcConnection.
        ///
        /// Drivers should allocate their internal data structure and set the private_data
        /// field to point to the newly allocated struct. This struct should be released
        /// when AdbcConnectionRelease is called.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionNew; // ConnectionFn

        /// <summary>
        /// Set a byte* option.
        ///
        /// Options may be set before AdbcConnectionInit.  Some  drivers may
        /// support setting options after initialization as well.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> ConnectionSetOption;

        /// <summary>
        /// Construct a statement for a partition of a query. The
        ///   results can then be read independently.
        ///
        /// A partition can be retrieved from AdbcPartitions.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, byte*, int, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode>  ConnectionReadPartition;

        /// <summary>
        /// Destroy this connection.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionRelease; // ConnectionFn

        /// <summary>
        /// Roll back any pending transactions. Only used if autocommit is disabled.
        ///
        /// Behavior is undefined if this is mixed with SQL transaction
        /// statements.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcError*, AdbcStatusCode> ConnectionRollback; // ConnectionFn

        /// <summary>
        /// Bind Arrow data. This can be used for bulk inserts or prepared statements.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArray*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode> StatementBind;

        /// <summary>
        /// Bind Arrow data. This can be used for bulk inserts or prepared statements.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, NativeAdbcError*, AdbcStatusCode> StatementBindStream;

        /// <summary>
        /// Execute a statement and get the results.
        ///
        /// This invalidates any prior result sets.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowArrayStream*, long*, NativeAdbcError*, AdbcStatusCode> StatementExecuteQuery;

        /// <summary>
        /// Execute a statement and get the results as a partitioned result set.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowSchema*, NativeAdbcPartitions*, long*, NativeAdbcError*, AdbcStatusCode> StatementExecutePartitions;

        /// <summary>
        /// Get the schema for bound parameters.
        ///
        /// This retrieves an Arrow schema describing the number, names, and
        /// types of the parameters in a parameterized statement.  The fields
        /// of the schema should be in order of the ordinal position of the
        /// parameters; named parameters should appear only once.
        ///
        /// If the parameter does not have a name, or the name cannot be
        /// determined, the name of the corresponding field in the schema will
        /// be an empty string.  If the type cannot be determined, the type of
        /// the corresponding field will be NA (NullType).
        ///
        /// This should be called after AdbcStatementPrepare.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, CArrowSchema*, NativeAdbcError*, AdbcStatusCode> StatementGetParameterSchema;

        /// <summary>
        /// Create a new statement for a given connection.
        ///
        /// Callers pass in a zero-initialized AdbcStatement.
        ///
        /// Drivers should allocate their internal data structure and set the private_data
        /// field to point to the newly allocated struct. This struct should be released
        /// when AdbcStatementRelease is called.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcConnection*, NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementNew;

        /// <summary>
        /// Turn this statement into a prepared statement to be
        ///   executed multiple times.
        ///
        /// This invalidates any prior result sets.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementPrepare; // StatementFn

        /// <summary>
        /// Destroy a statement.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, NativeAdbcError*, AdbcStatusCode> StatementRelease; // StatementFn

        /// <summary>
        /// Set a string option on a statement.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, byte*, byte*, NativeAdbcError*, AdbcStatusCode> StatementSetOption;

        /// <summary>
        /// Set the SQL query to execute.
        ///
        /// The query can then be executed with StatementExecute.  For
        /// queries expected to be executed repeatedly, StatementPrepare
        /// the statement first.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, byte*, NativeAdbcError*, AdbcStatusCode> StatementSetSqlQuery;

        /// <summary>
        /// Set the Substrait plan to execute.
        ///
        /// The query can then be executed with AdbcStatementExecute.  For
        /// queries expected to be executed repeatedly, AdbcStatementPrepare
        /// the statement first.
        /// </summary>
        public delegate* unmanaged[Stdcall]<NativeAdbcStatement*, byte*, int, NativeAdbcError*, AdbcStatusCode>  StatementSetSubstraitPlan;
    }

    unsafe delegate AdbcStatusCode DriverRelease(NativeAdbcDriver* driver, NativeAdbcError* error);

    unsafe delegate AdbcStatusCode DatabaseFn(NativeAdbcDatabase* database, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode DatabaseSetOption(NativeAdbcDatabase* database, byte* name, byte* value, NativeAdbcError* error);

    unsafe delegate AdbcStatusCode ConnectionFn(NativeAdbcConnection* connection, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetInfo(NativeAdbcConnection* connection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetObjects(NativeAdbcConnection* connection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetTableSchema(NativeAdbcConnection* connection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionGetTableTypes(NativeAdbcConnection* connection, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionInit(NativeAdbcConnection* connection, NativeAdbcDatabase* database, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionSetOption(NativeAdbcConnection* connection, byte* name, byte* value, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode ConnectionReadPartition(NativeAdbcConnection* connection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, NativeAdbcError* error);

    unsafe delegate AdbcStatusCode StatementBind(NativeAdbcStatement* statement, CArrowArray* array, CArrowSchema* schema, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementBindStream(NativeAdbcStatement* statement, CArrowArrayStream* stream, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementExecuteQuery(NativeAdbcStatement* statement, CArrowArrayStream* stream, long* rows, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementExecutePartitions(NativeAdbcStatement* statement, CArrowSchema* schema, NativeAdbcPartitions* partitions, long* rows_affected, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementGetParameterSchema(NativeAdbcStatement* statement, CArrowSchema* schema, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementNew(NativeAdbcConnection* connection, NativeAdbcStatement* statement, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementFn(NativeAdbcStatement* statement, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetOption(NativeAdbcStatement* statement, byte* name, byte* value, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetSqlQuery(NativeAdbcStatement* statement, byte* text, NativeAdbcError* error);
    unsafe delegate AdbcStatusCode StatementSetSubstraitPlan(NativeAdbcStatement statement, byte* plan, int length, NativeAdbcError error);

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

        public unsafe AdbcStatusCode GetObjects(ref NativeAdbcConnection nativeConnection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* cstream, ref NativeAdbcError error)
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

        public unsafe AdbcStatusCode GetTableSchema(ref NativeAdbcConnection nativeConnection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* cschema, ref NativeAdbcError error)
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

        public unsafe AdbcStatusCode GetTableTypes(ref NativeAdbcConnection nativeConnection, CArrowArrayStream* cArrayStream, ref NativeAdbcError error)
        {
            if (nativeConnection.private_data == null)
            {
                return AdbcStatusCode.UnknownError;
            }

            CArrowArrayStreamExporter.ExportArrayStream(connection.GetTableTypes(), cArrayStream);

            return AdbcStatusCode.Success;
        }

        public unsafe AdbcStatusCode ReadPartition(ref NativeAdbcConnection nativeConnection, byte* serializedPartition, int serialized_length, CArrowArrayStream* stream, ref NativeAdbcError error)
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

        public unsafe AdbcStatusCode GetInfo(ref NativeAdbcConnection nativeConnection, byte* info_codes, int info_codes_length, CArrowArrayStream* stream, ref NativeAdbcError error)
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

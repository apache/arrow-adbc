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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.C
{
    internal delegate AdbcStatusCode AdbcDriverInit(int version, ref CAdbcDriver driver, ref CAdbcError error);

    /// <summary>
    /// Class for working with imported drivers from files
    /// </summary>
    public static class CAdbcDriverImporter
    {
        private const string driverInit = "AdbcDriverInit";
        private const int ADBC_VERSION_1_0_0 = 1000000;

        /// <summary>
        /// Loads an <see cref="AdbcDriver"/> from the file system.
        /// </summary>
        /// <param name="file">The path to the driver to load</param>
        /// <param name="entryPoint">The name of the entry point. If not provided, the name AdbcDriverInit will be used.</param>
        public static AdbcDriver Load(string file, string? entryPoint = null)
        {
            if (file == null)
            {
                throw new ArgumentNullException(nameof(file));
            }

            if (!File.Exists(file))
            {
                throw new ArgumentException("file does not exist", nameof(file));
            }

            IntPtr library = NativeLibrary.Load(file);
            if (library == IntPtr.Zero)
            {
                throw new ArgumentException("unable to load library", nameof(file));
            }

            try
            {
                entryPoint = entryPoint ?? driverInit;
                IntPtr export = NativeLibrary.GetExport(library, entryPoint);
                if (export == IntPtr.Zero)
                {
                    NativeLibrary.Free(library);
                    throw new ArgumentException($"Unable to find {entryPoint} export", nameof(file));
                }

                AdbcDriverInit init = Marshal.GetDelegateForFunctionPointer<AdbcDriverInit>(export);
                CAdbcDriver driver = new CAdbcDriver();
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(init, ADBC_VERSION_1_0_0, ref driver);

                    ValidateDriver(ref driver, ADBC_VERSION_1_0_0);

                    ImportedAdbcDriver result = new ImportedAdbcDriver(library, driver);
                    library = IntPtr.Zero;
                    return result;
                }
            }
            finally
            {
                if (library != IntPtr.Zero) { NativeLibrary.Free(library); }
            }
        }

        private static unsafe void ValidateDriver(ref CAdbcDriver driver, int version)
        {
#if NET5_0_OR_GREATER
            void* empty = null;
#else
            IntPtr empty = IntPtr.Zero;
#endif
            if (driver.DatabaseNew == empty) { throw AdbcException.Missing(nameof(driver.DatabaseNew)); }
            if (driver.DatabaseInit == empty) { throw AdbcException.Missing(nameof(driver.DatabaseInit)); }
            if (driver.DatabaseRelease == empty) { throw AdbcException.Missing(nameof(driver.DatabaseRelease)); }
            if (driver.DatabaseSetOption == empty) { driver.DatabaseSetOption = DatabaseSetOptionDefault; }

            if (driver.ConnectionNew == empty) { throw AdbcException.Missing(nameof(driver.ConnectionNew)); }
            if (driver.ConnectionInit == empty) { throw AdbcException.Missing(nameof(driver.ConnectionInit)); }
            if (driver.ConnectionRelease == empty) { throw AdbcException.Missing(nameof(driver.ConnectionRelease)); }
            if (driver.ConnectionCommit == empty) { driver.ConnectionCommit = ConnectionCommitDefault; }
            if (driver.ConnectionGetInfo == empty) { driver.ConnectionGetInfo = ConnectionGetInfoDefault; }
            if (driver.ConnectionGetObjects == empty) { driver.ConnectionGetObjects = ConnectionGetObjectsDefault; }
            if (driver.ConnectionGetTableSchema == empty) { driver.ConnectionGetTableSchema = ConnectionGetTableSchemaDefault; }
            if (driver.ConnectionGetTableTypes == empty) { driver.ConnectionGetTableTypes = ConnectionGetTableTypesDefault; }
            if (driver.ConnectionReadPartition == empty) { driver.ConnectionReadPartition = ConnectionReadPartitionDefault; }
            if (driver.ConnectionRollback == empty) { driver.ConnectionRollback = ConnectionRollbackDefault; }
            if (driver.ConnectionSetOption == empty) { driver.ConnectionSetOption = ConnectionSetOptionDefault; }

            if (driver.StatementExecutePartitions == empty) { driver.StatementExecutePartitions = StatementExecutePartitionsDefault; }
            if (driver.StatementExecuteQuery == empty) { throw AdbcException.Missing(nameof(driver.StatementExecuteQuery)); }
            if (driver.StatementNew == empty) { throw AdbcException.Missing(nameof(driver.StatementNew)); }
            if (driver.StatementRelease == empty) { throw AdbcException.Missing(nameof(driver.StatementRelease)); }
            if (driver.StatementBind == empty) { driver.StatementBind = StatementBindDefault; }
            if (driver.StatementBindStream == empty) { driver.StatementBindStream = StatementBindStreamDefault; }
            if (driver.StatementGetParameterSchema == empty) { driver.StatementGetParameterSchema = StatementGetParameterSchemaDefault; }
            if (driver.StatementPrepare == empty) { driver.StatementPrepare = StatementPrepareDefault; }
            if (driver.StatementSetOption == empty) { driver.StatementSetOption = StatementSetOptionDefault; }
            if (driver.StatementSetSqlQuery == empty) { driver.StatementSetSqlQuery = StatementSetSqlQueryDefault; }
            if (driver.StatementSetSubstraitPlan == empty) { driver.StatementSetSubstraitPlan = StatementSetSubstraitPlanDefault; }
        }

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

        private static unsafe AdbcStatusCode NotImplemented(CAdbcError* error, string name)
        {
            if (error != null)
            {
                CAdbcDriverExporter.ReleaseError(error);

                error->message = (byte*)MarshalExtensions.StringToCoTaskMemUTF8($"Adbc{name} not implemented");
                error->sqlstate0 = (byte)0;
                error->sqlstate1 = (byte)0;
                error->sqlstate2 = (byte)0;
                error->sqlstate3 = (byte)0;
                error->sqlstate4 = (byte)0;
                error->vendor_code = 0;
                error->release = CAdbcDriverExporter.ReleaseErrorPtr;
            }

            return AdbcStatusCode.NotImplemented;
        }

        /// <summary>
        /// Native implementation of <see cref="AdbcDriver"/>
        /// </summary>
        internal sealed class ImportedAdbcDriver : AdbcDriver
        {
            private IntPtr _library;
            private CAdbcDriver _nativeDriver;
            private int _references;
            private bool _disposed;

            public ImportedAdbcDriver(IntPtr library, CAdbcDriver nativeDriver)
            {
                _library = library;
                _nativeDriver = nativeDriver;
                _references = 1;
            }

            ~ImportedAdbcDriver()
            {
                Dispose(false);
            }

            internal unsafe ref CAdbcDriver Driver
            {
                get
                {
                    if (_disposed) { throw new ObjectDisposedException(nameof(ImportedAdbcDriver)); }
                    return ref _nativeDriver;
                }
            }

            internal ImportedAdbcDriver AddReference()
            {
                Interlocked.Increment(ref _references);
                return this;
            }

            internal unsafe void RemoveReference()
            {
                if (Interlocked.Decrement(ref _references) == 0)
                {
                    if (_nativeDriver.release != default)
                    {
                        using (CallHelper caller = new CallHelper())
                        {
                            caller.Call(_nativeDriver.release, ref _nativeDriver);
                        }

                        NativeLibrary.Free(_library);
                        _library = IntPtr.Zero;
                    }
                }
            }

            /// <summary>
            /// Opens a database
            /// </summary>
            /// <param name="parameters">
            /// Parameters to use when calling DatabaseNew.
            /// </param>
            public unsafe override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
            {
                if (parameters == null) throw new ArgumentNullException(nameof(parameters));

                CAdbcDatabase nativeDatabase = new CAdbcDatabase();
                ImportedAdbcDatabase? result = null;
                try
                {
                    using (CallHelper caller = new CallHelper())
                    {
                        caller.Call(Driver.DatabaseNew, ref nativeDatabase);

                        foreach (KeyValuePair<string, string> pair in parameters)
                        {
                            caller.Call(Driver.DatabaseSetOption, ref nativeDatabase, pair.Key, pair.Value);
                        }

                        caller.Call(Driver.DatabaseInit, ref nativeDatabase);
                    }

                    result = new ImportedAdbcDatabase(this, nativeDatabase);
                }
                finally
                {
                    if (result == null && nativeDatabase.private_data != null)
                    {
                        using (CallHelper caller = new CallHelper())
                        {
                            caller.Call(Driver.DatabaseRelease, ref nativeDatabase);
                        }
                    }
                }

                return result;
            }

            public unsafe override void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            private void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    _disposed = true;
                    RemoveReference();
                    if (disposing)
                    {
                        base.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// A native implementation of <see cref="AdbcDatabase"/>
        /// </summary>
        internal sealed class ImportedAdbcDatabase : AdbcDatabase
        {
            private readonly ImportedAdbcDriver _driver;
            private CAdbcDatabase _nativeDatabase;
            private bool _disposed;

            internal ImportedAdbcDatabase(ImportedAdbcDriver driver, CAdbcDatabase nativeDatabase)
            {
                _driver = driver.AddReference();
                _nativeDatabase = nativeDatabase;
            }

            ~ImportedAdbcDatabase()
            {
                Dispose(false);
            }

            private unsafe ref CAdbcDriver Driver
            {
                get
                {
                    if (_disposed) { throw new ObjectDisposedException(nameof(ImportedAdbcDatabase)); }
                    return ref _driver.Driver;
                }
            }

            public unsafe override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
            {
                CAdbcConnection nativeConnection = new CAdbcConnection();
                ImportedAdbcConnection? result = null;
                try
                {
                    using (CallHelper caller = new CallHelper())
                    {
                        caller.Call(Driver.ConnectionNew, ref nativeConnection);

                        if (options != null)
                        {
                            foreach (KeyValuePair<string, string> pair in options)
                            {
                                caller.Call(Driver.ConnectionSetOption, ref nativeConnection, pair.Key, pair.Value);
                            }
                        }

                        caller.Call(Driver.ConnectionInit, ref nativeConnection, ref _nativeDatabase);

                        result = new ImportedAdbcConnection(_driver, nativeConnection);
                    }
                }
                finally
                {
                    if (result == null && nativeConnection.private_data != null)
                    {
                        using (CallHelper caller = new CallHelper())
                        {
                            caller.Call(Driver.ConnectionRelease, ref nativeConnection);
                        }
                    }
                }

                return result;
            }

            public unsafe override void SetOption(string key, string value)
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(Driver.DatabaseSetOption, ref _nativeDatabase, key, value);
                }
            }

            public override void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            private unsafe void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    _disposed = true;

                    try
                    {
                        if (_nativeDatabase.private_data != default)
                        {
                            using (CallHelper caller = new CallHelper())
                            {
                                caller.Call(_driver.Driver.DatabaseRelease, ref _nativeDatabase);
                            }
                        }
                    }
                    finally
                    {
                        _nativeDatabase.private_data = default;
                        _driver.RemoveReference();
                    }
                    if (disposing)
                    {
                        base.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// A native implementation of <see cref="AdbcConnection"/>
        /// </summary>
        internal sealed class ImportedAdbcConnection : AdbcConnection
        {
            private readonly ImportedAdbcDriver _driver;
            private CAdbcConnection _nativeConnection;
            private bool _disposed;
            private bool? _autoCommit;
            private IsolationLevel? _isolationLevel;
            private bool? _readOnly;

            internal ImportedAdbcConnection(ImportedAdbcDriver driver, CAdbcConnection nativeConnection)
            {
                _driver = driver.AddReference();
                _nativeConnection = nativeConnection;
            }

            ~ImportedAdbcConnection()
            {
                Dispose(false);
            }

            private unsafe ref CAdbcDriver Driver
            {
                get
                {
                    if (_disposed) { throw new ObjectDisposedException(nameof(ImportedAdbcConnection)); }
                    return ref _driver.Driver;
                }
            }

            public override bool AutoCommit
            {
                get => _autoCommit ?? throw AdbcException.NotImplemented("no value has been set for AutoCommit");
                set
                {
                    SetOption(AdbcOptions.Autocommit, AdbcOptions.GetEnabled(value));
                    _autoCommit = value;
                }
            }

            public override IsolationLevel IsolationLevel
            {
                get => _isolationLevel ?? IsolationLevel.Default;
                set
                {
                    SetOption(AdbcOptions.IsolationLevel, AdbcOptions.GetIsolationLevel(value));
                    _isolationLevel = value;
                }
            }

            public override bool ReadOnly
            {
                get => _readOnly ?? throw AdbcException.NotImplemented("no value has been set for ReadOnly");
                set
                {
                    SetOption(AdbcOptions.ReadOnly, AdbcOptions.GetEnabled(value));
                    _readOnly = value;
                }
            }

            public unsafe override AdbcStatement CreateStatement()
            {
                CAdbcStatement nativeStatement = new CAdbcStatement();
                ImportedAdbcStatement? result = null;
                try
                {
                    using (CallHelper caller = new CallHelper())
                    {
                        fixed (CAdbcConnection* connection = &_nativeConnection)
                        {
                            caller.TranslateCode(
#if NET5_0_OR_GREATER
                                Driver.StatementNew
#else
                                Marshal.GetDelegateForFunctionPointer<StatementNew>(Driver.StatementNew)
#endif
                                (connection, &nativeStatement, &caller._error));
                        }

                        result = new ImportedAdbcStatement(_driver, nativeStatement);
                    }
                }
                finally
                {
                    if (result == null && nativeStatement.private_data != null)
                    {
                        using (CallHelper caller = new CallHelper())
                        {
                            caller.Call(Driver.StatementRelease, ref nativeStatement);
                        }
                    }
                }

                return result;
            }

            public unsafe override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
            {
                using (CallHelper caller = new CallHelper())
                {
                    Span<AdbcInfoCode> span = codes.AsSpan();
                    fixed (CAdbcConnection* connection = &_nativeConnection)
                    fixed (AdbcInfoCode* spanPtr = span)
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            Driver.ConnectionGetInfo
#else
                            Marshal.GetDelegateForFunctionPointer<ConnectionGetInfo>(Driver.ConnectionGetInfo)
#endif
                            (connection, (int*)spanPtr, codes.Count, caller.CreateStream(), &caller._error));
                        return caller.ImportStream();
                    }
                }
            }

            public unsafe override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
            {
                tableTypes = tableTypes ?? [];
                byte** utf8TableTypes = null;
                try
                {
                    // need to terminate with a null entry per https://github.com/apache/arrow-adbc/blob/b97e22c4d6524b60bf261e1970155500645be510/adbc.h#L909-L911
                    utf8TableTypes = (byte**)Marshal.AllocHGlobal(IntPtr.Size * (tableTypes.Count + 1));
                    utf8TableTypes[tableTypes.Count] = null;

                    for (int i = 0; i < tableTypes.Count; i++)
                    {
                        string tableType = tableTypes[i];
                        utf8TableTypes[i] = (byte*)MarshalExtensions.StringToCoTaskMemUTF8(tableType);
                    }

                    using (Utf8Helper utf8Catalog = new Utf8Helper(catalogPattern))
                    using (Utf8Helper utf8Schema = new Utf8Helper(dbSchemaPattern))
                    using (Utf8Helper utf8Table = new Utf8Helper(tableNamePattern))
                    using (Utf8Helper utf8Column = new Utf8Helper(columnNamePattern))
                    using (CallHelper caller = new CallHelper())
                    {
                        fixed (CAdbcConnection* connection = &_nativeConnection)
                        {
                            caller.TranslateCode(
#if NET5_0_OR_GREATER
                                Driver.ConnectionGetObjects
#else
                                Marshal.GetDelegateForFunctionPointer<ConnectionGetObjects>(Driver.ConnectionGetObjects)
#endif
                                (connection, (int)depth, utf8Catalog, utf8Schema, utf8Table, utf8TableTypes, utf8Column, caller.CreateStream(), &caller._error));
                            return caller.ImportStream();
                        }
                    }
                }
                finally
                {
                    for (int i = 0; i < tableTypes.Count; i++)
                    {
                        Marshal.FreeCoTaskMem((IntPtr)utf8TableTypes[i]);
                    }
                    Marshal.FreeHGlobal((IntPtr)utf8TableTypes);
                }
            }

            public unsafe override IArrowArrayStream GetTableTypes()
            {
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcConnection* connection = &_nativeConnection)
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            Driver.ConnectionGetTableTypes
#else
                            Marshal.GetDelegateForFunctionPointer<ConnectionGetTableTypes>(Driver.ConnectionGetTableTypes)
#endif
                            (connection, caller.CreateStream(), &caller._error));
                        return caller.ImportStream();
                    }
                }
            }

            public unsafe override Schema GetTableSchema(string? catalog, string? db_schema, string table_name)
            {
                using (Utf8Helper utf8Catalog = new Utf8Helper(catalog))
                using (Utf8Helper utf8Schema = new Utf8Helper(db_schema))
                using (Utf8Helper utf8Table = new Utf8Helper(table_name))
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcConnection* connection = &_nativeConnection)
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            Driver.ConnectionGetTableSchema
#else
                            Marshal.GetDelegateForFunctionPointer<ConnectionGetTableSchema>(Driver.ConnectionGetTableSchema)
#endif
                            (connection, utf8Catalog, utf8Schema, utf8Table, caller.CreateSchema(), &caller._error));
                        return caller.ImportSchema();
                    }
                }
            }

            public override AdbcStatement BulkIngest(string targetTableName, BulkIngestMode mode)
            {
                AdbcStatement statement = CreateStatement();
                bool succeeded = false;
                try
                {
                    statement.SetOption(AdbcOptions.Ingest.TargetTable, targetTableName);
                    statement.SetOption(AdbcOptions.Ingest.Mode, AdbcOptions.GetIngestMode(mode));
                    succeeded = true;
                    return statement;
                }
                finally
                {
                    if (!succeeded) { statement.Dispose(); }
                }
            }

            public unsafe override void Commit()
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(Driver.ConnectionCommit, ref _nativeConnection);
                }
            }

            public unsafe override void Rollback()
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(Driver.ConnectionRollback, ref _nativeConnection);
                }
            }

            public unsafe override void SetOption(string key, string value)
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(Driver.ConnectionSetOption, ref _nativeConnection, key, value);
                }
            }

            public override void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            private unsafe void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    _disposed = true;

                    try
                    {
                        if (_nativeConnection.private_data != default)
                        {
                            using (CallHelper caller = new CallHelper())
                            {
                                caller.Call(_driver.Driver.ConnectionRelease, ref _nativeConnection);
                            }
                        }
                    }
                    finally
                    {
                        _nativeConnection.private_data = default;
                        _driver.RemoveReference();
                    }
                    if (disposing)
                    {
                        base.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// A native implementation of <see cref="AdbcStatement"/>
        /// </summary>
        internal sealed class ImportedAdbcStatement : AdbcStatement
        {
            private ImportedAdbcDriver _driver;
            private CAdbcStatement _nativeStatement;
            private byte[]? _substraitPlan;
            private bool _disposed;

            internal ImportedAdbcStatement(ImportedAdbcDriver driver, CAdbcStatement nativeStatement)
            {
                _driver = driver.AddReference();
                _nativeStatement = nativeStatement;
            }

            ~ImportedAdbcStatement()
            {
                Dispose(false);
            }

            private unsafe ref CAdbcDriver Driver
            {
                get
                {
                    if (_disposed) { throw new ObjectDisposedException(nameof(ImportedAdbcStatement)); }
                    return ref _driver.Driver;
                }
            }

            public unsafe override byte[]? SubstraitPlan
            {
                get => _substraitPlan;
                set
                {
                    using (CallHelper caller = new CallHelper())
                    {
                        fixed (CAdbcStatement* statement = &_nativeStatement)
                        fixed (byte* substraitPlan = value)
                        {
                            caller.TranslateCode(
#if NET5_0_OR_GREATER
                                Driver.StatementSetSubstraitPlan
#else
                                Marshal.GetDelegateForFunctionPointer<StatementSetSubstraitPlan>(Driver.StatementSetSubstraitPlan)
#endif
                                (statement, substraitPlan, value?.Length ?? 0, &caller._error));
                        }
                        _substraitPlan = value;
                    }
                }
            }

            public unsafe override void Bind(RecordBatch batch, Schema schema)
            {
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        CArrowArrayExporter.ExportRecordBatch(batch, caller.CreateArray());
                        CArrowSchemaExporter.ExportSchema(schema, caller.CreateSchema());

                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            Driver.StatementBind
#else
                            Marshal.GetDelegateForFunctionPointer<StatementBind>(Driver.StatementBind)
#endif
                            (statement, caller.Array, caller.Schema, &caller._error));

                        // On success, ownership passes to the driver
                        caller.ForgetArray();
                        caller.ForgetSchema();
                    }
                }
            }

            public unsafe override void BindStream(IArrowArrayStream stream)
            {
                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        CArrowArrayStreamExporter.ExportArrayStream(stream, caller.CreateStream());
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            Driver.StatementBindStream
#else
                            Marshal.GetDelegateForFunctionPointer<StatementBindStream>(Driver.StatementBindStream)
#endif
                            (statement, caller.ArrayStream, &caller._error));

                        // On success, ownership passes to the driver
                        caller.ForgetStream();
                    }
                }
            }

            public unsafe override QueryResult ExecuteQuery()
            {
                if (SqlQuery != null)
                {
                    // TODO: Consider moving this to the setter
                    SetSqlQuery(SqlQuery);
                }

                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        long rows = 0;
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            Driver.StatementExecuteQuery
#else
                            Marshal.GetDelegateForFunctionPointer<StatementExecuteQuery>(Driver.StatementExecuteQuery)
#endif
                            (statement, caller.CreateStream(), &rows, &caller._error));

                        return new QueryResult(rows, caller.ImportStream());
                    }
                }
            }

            public unsafe override UpdateResult ExecuteUpdate()
            {
                if (SqlQuery != null)
                {
                    // TODO: Consider moving this to the setter
                    SetSqlQuery(SqlQuery);
                }

                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        long rows = 0;
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            Driver.StatementExecuteQuery
#else
                            Marshal.GetDelegateForFunctionPointer<StatementExecuteQuery>(Driver.StatementExecuteQuery)
#endif
                            (statement, null, &rows, &caller._error));

                        return new UpdateResult(rows);
                    }
                }
            }

            public unsafe override PartitionedResult ExecutePartitioned()
            {
                if (SqlQuery != null)
                {
                    // TODO: Consider moving this to the setter
                    SetSqlQuery(SqlQuery);
                }

                using (CallHelper caller = new CallHelper())
                {
                    fixed (CAdbcStatement* statement = &_nativeStatement)
                    {
                        CAdbcPartitions* nativePartitions = null;
                        long rowsAffected = 0;
                        try
                        {
                            nativePartitions = (CAdbcPartitions*)Marshal.AllocHGlobal(sizeof(CAdbcPartitions));
                            caller.TranslateCode(
#if NET5_0_OR_GREATER
                                Driver.StatementExecutePartitions
#else
                                Marshal.GetDelegateForFunctionPointer<StatementExecutePartitions>(Driver.StatementExecutePartitions)
#endif
                                (statement, caller.CreateSchema(), nativePartitions, &rowsAffected, &caller._error));

                            PartitionDescriptor[] partitions = new PartitionDescriptor[nativePartitions->num_partitions];
                            for (int i = 0; i < partitions.Length; i++)
                            {
                                partitions[i] = new PartitionDescriptor(MarshalExtensions.MarshalBuffer(nativePartitions->partitions[i], checked((int)nativePartitions->partition_lengths[i]))!);
                            }

                            return new PartitionedResult(caller.ImportSchema(), rowsAffected, partitions);
                        }
                        finally
                        {
                            if (nativePartitions->release != null)
                            {
#if NET5_0_OR_GREATER
                                nativePartitions->release(nativePartitions);
#else
                                Marshal.GetDelegateForFunctionPointer<PartitionsRelease>(nativePartitions->release)(nativePartitions);
#endif
                            }

                            if (nativePartitions != null)
                            {
                                Marshal.FreeHGlobal((IntPtr)nativePartitions);
                            }
                        }
                    }
                }
            }

            public unsafe override void SetOption(string key, string value)
            {
                using (CallHelper caller = new CallHelper())
                {
                    caller.Call(Driver.StatementSetOption, ref _nativeStatement, key, value);
                }
            }

            public override void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            private unsafe void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    _disposed = true;

                    try
                    {
                        if (_nativeStatement.private_data != default)
                        {
                            using (CallHelper caller = new CallHelper())
                            {
                                caller.Call(_driver.Driver.StatementRelease, ref _nativeStatement);
                            }
                        }
                    }
                    finally
                    {
                        _nativeStatement.private_data = default;
                        _driver.RemoveReference();
                    }
                    if (disposing)
                    {
                        base.Dispose();
                    }
                }
            }

            private unsafe void SetSqlQuery(string sqlQuery)
            {
                fixed (CAdbcStatement* statement = &_nativeStatement)
                {
                    using (Utf8Helper query = new Utf8Helper(sqlQuery))
                    using (CallHelper caller = new CallHelper())
                    {
                        caller.TranslateCode(
#if NET5_0_OR_GREATER
                            _driver.Driver.StatementSetSqlQuery
#else
                            Marshal.GetDelegateForFunctionPointer<StatementSetSqlQuery>(_driver.Driver.StatementSetSqlQuery)
#endif
                            (statement, query, &caller._error));
                    }
                }
            }
        }

        /// <summary>
        /// Assists with UTF8/string marshalling
        /// </summary>
        private struct Utf8Helper : IDisposable
        {
            private IntPtr _s;

            public Utf8Helper(string? s)
            {
                if (s != null) _s = MarshalExtensions.StringToCoTaskMemUTF8(s);
            }

            public static implicit operator IntPtr(Utf8Helper s) { return s._s; }
            public static unsafe implicit operator byte*(Utf8Helper s) { return (byte*)s._s; }

            public void Dispose()
            {
                if (_s != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(_s);
                    _s = IntPtr.Zero;
                }
            }
        }

        /// <summary>
        /// Assists with delegate calls and handling error codes
        /// </summary>
        private unsafe struct CallHelper : IDisposable
        {
            public CAdbcError _error;
            private CArrowSchema* _schema;
            private CArrowArray* _array;
            private CArrowArrayStream* _arrayStream;

            public CArrowSchema* Schema => _schema;
            public CArrowArray* Array => _array;
            public CArrowArrayStream* ArrayStream => _arrayStream;

            public CArrowSchema* CreateSchema()
            {
                Debug.Assert(_schema == null);
                _schema = CArrowSchema.Create();
                return _schema;
            }

            public void ForgetSchema()
            {
                Debug.Assert(_schema != null);
                _schema = null;
            }

            public Schema ImportSchema()
            {
                Debug.Assert(_schema != null);
                Schema schema = CArrowSchemaImporter.ImportSchema(_schema);

                // ImportSchema makes a copy so we need to free the original
                CArrowSchema.Free(_schema);
                _schema = null;

                return schema;
            }

            public CArrowArray* CreateArray()
            {
                Debug.Assert(_array == null);
                _array = CArrowArray.Create();
                return _array;
            }

            public void ForgetArray()
            {
                Debug.Assert(_array != null);
                _array = null;
            }

            public CArrowArrayStream* CreateStream()
            {
                Debug.Assert(_arrayStream == null);
                _arrayStream = CArrowArrayStream.Create();
                return _arrayStream;
            }

            public void ForgetStream()
            {
                Debug.Assert(_arrayStream != null);
                _arrayStream = null;
            }

            public IArrowArrayStream ImportStream()
            {
                Debug.Assert(_arrayStream != null);
                IArrowArrayStream arrayStream = CArrowArrayStreamImporter.ImportArrayStream(_arrayStream);
                _arrayStream = null;
                return arrayStream;
            }

            public unsafe void Call(AdbcDriverInit init, int version, ref CAdbcDriver driver)
            {
                TranslateCode(init(version, ref driver, ref this._error));
            }

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcDriver*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDriver nativeDriver)
            {
                fixed (CAdbcDriver* driver = &nativeDriver)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(driver, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcDriver nativeDriver)
            {
                fixed (CAdbcDriver* driver = &nativeDriver)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<DriverRelease>(fn)(driver, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDatabase nativeDatabase)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(db, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcDatabase nativeDatabase)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<DatabaseFn>(fn)(db, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcDatabase nativeDatabase, string key, string? value)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(fn(db, utf8Key, utf8Value, e));
                    }
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcDatabase nativeDatabase, string key, string? value)
            {
                fixed (CAdbcDatabase* db = &nativeDatabase)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(Marshal.GetDelegateForFunctionPointer<DatabaseSetOption>(fn)(db, utf8Key, utf8Value, e));
                    }
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<ConnectionFn>(fn)(cn, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(stmt, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcStatement nativeStatement)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<StatementPrepare>(fn)(stmt, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, string key, string? value)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(fn(cn, utf8Key, utf8Value, e));
                    }
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection, string key, string? value)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(Marshal.GetDelegateForFunctionPointer<ConnectionSetOption>(fn)(cn, utf8Key, utf8Value, e));
                    }
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcConnection*, CAdbcDatabase*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcConnection nativeConnection, ref CAdbcDatabase database)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcDatabase* db = &database)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(fn(cn, db, e));
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcConnection nativeConnection, ref CAdbcDatabase database)
            {
                fixed (CAdbcConnection* cn = &nativeConnection)
                fixed (CAdbcDatabase* db = &database)
                fixed (CAdbcError* e = &_error)
                {
                    TranslateCode(Marshal.GetDelegateForFunctionPointer<ConnectionInit>(fn)(cn, db, e));
                }
            }
#endif

#if NET5_0_OR_GREATER
            public unsafe void Call(delegate* unmanaged<CAdbcStatement*, byte*, byte*, CAdbcError*, AdbcStatusCode> fn, ref CAdbcStatement nativeStatement, string key, string? value)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(fn(stmt, utf8Key, utf8Value, e));
                    }
                }
            }
#else
            public unsafe void Call(IntPtr fn, ref CAdbcStatement nativeStatement, string key, string? value)
            {
                fixed (CAdbcStatement* stmt = &nativeStatement)
                fixed (CAdbcError* e = &_error)
                {
                    using (Utf8Helper utf8Key = new Utf8Helper(key))
                    using (Utf8Helper utf8Value = new Utf8Helper(value))
                    {
                        TranslateCode(Marshal.GetDelegateForFunctionPointer<StatementSetOption>(fn)(stmt, utf8Key, utf8Value, e));
                    }
                }
            }
#endif

            public unsafe void Dispose()
            {
                if (_error.release != default)
                {
                    fixed (CAdbcError* err = &_error)
                    {
#if NET5_0_OR_GREATER
                        _error.release(err);
#else
                        Marshal.GetDelegateForFunctionPointer<ErrorRelease>(err->release)(err);
#endif
                        _error.release = default;
                    }
                }

                if (_array != null)
                {
                    CArrowArray.Free(_array);
                    _array = null;
                }
                if (_schema != null)
                {
                    CArrowSchema.Free(_schema);
                    _schema = null;
                }
                if (_arrayStream != null)
                {
                    CArrowArrayStream.Free(_arrayStream);
                    _arrayStream = null;
                }
            }

            public unsafe void TranslateCode(AdbcStatusCode statusCode)
            {
                if (statusCode != AdbcStatusCode.Success)
                {
                    string message = "Undefined error";
                    if (_error.message != null)
                    {
                        message = MarshalExtensions.PtrToStringUTF8(_error.message)!;
                    }

                    Dispose();

                    throw new AdbcException(message);
                }
            }
        }
    }
}

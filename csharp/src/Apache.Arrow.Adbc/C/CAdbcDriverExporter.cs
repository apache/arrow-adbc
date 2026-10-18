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
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// Exposes a managed <see cref="AdbcDriver"/> through the ADBC C API.
    /// </summary>
    /// <remarks>
    /// When a caller requests ADBC 1.1.0, every 1.1.0 entry point is populated
    /// except <c>ErrorFromArrayStream</c>; exported streams report errors through
    /// <c>get_last_error</c> instead. Functionality that the managed driver doesn't
    /// override surfaces as <see cref="AdbcStatusCode.NotImplemented"/>, which is
    /// the same status a driver manager reports for a missing entry point.
    /// </remarks>
    public class CAdbcDriverExporter
    {
        /// <summary>
        /// The value of <c>ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA</c>. A caller sets an error's
        /// vendor code to this value to indicate that it supports the ADBC 1.1.0 error layout.
        /// </summary>
        internal const int ErrorVendorCodePrivateData = int.MinValue;

        private static readonly IdleCollector s_idleCollector = new IdleCollector(CollectGarbage, DefaultIdleCollectionDelay());

        /// <summary>
        /// Gets or sets how long the exporter waits, after the last database has been released,
        /// before it forces a full, compacting garbage collection; or <c>null</c> to never force one.
        /// </summary>
        /// <remarks>
        /// <para>A driver published with NativeAOT has a garbage-collected heap of its own, and
        /// the runtime only collects it in response to allocations. Once a caller has released
        /// all of its databases, the driver stops allocating and the memory it last used stays
        /// committed for the life of the process. Forcing a collection once the driver is idle
        /// returns that memory to the operating system. The collection runs on a thread-pool
        /// thread, and only if no database has been opened in the meantime.</para>
        /// <para>The default is one second when the driver is compiled with NativeAOT, and
        /// <c>null</c> otherwise, because a JIT-compiled driver shares its heap with the host.</para>
        /// <para>A collection that is still pending when the driver is released runs after
        /// the release returns. A driver that expects its host to unload it after releasing it
        /// should set this to <c>null</c>, although unloading a NativeAOT library isn't
        /// supported in any case.</para>
        /// </remarks>
        /// <exception cref="ArgumentOutOfRangeException">The value is negative or longer than
        /// <see cref="int.MaxValue"/> milliseconds.</exception>
        public static TimeSpan? IdleCollectionDelay
        {
            get => s_idleCollector.Delay;
            set => s_idleCollector.Delay = value;
        }

        private static TimeSpan? DefaultIdleCollectionDelay()
        {
#if NET7_0_OR_GREATER
            return System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeSupported ? null : TimeSpan.FromSeconds(1);
#else
            return null;
#endif
        }

        private static void CollectGarbage()
        {
            // Exported Arrow buffers are currently freed by their finalizers rather than when the
            // consumer releases them (apache/arrow-dotnet#464), so run those before compacting.
            GC.Collect();
            GC.WaitForPendingFinalizers();
#if NET7_0_OR_GREATER
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
#else
            System.Runtime.GCSettings.LargeObjectHeapCompactionMode = System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
#endif
        }

        private static unsafe readonly NativeDelegate<ErrorRelease> s_releaseError = new NativeDelegate<ErrorRelease>(ReleaseError);

#if NET5_0_OR_GREATER
        internal static unsafe delegate* unmanaged<CAdbcError*, void> ReleaseErrorPtr => (delegate* unmanaged<CAdbcError*, void>)s_releaseError.Pointer;
        private static unsafe delegate* unmanaged<CAdbcDriver*, CAdbcError*, AdbcStatusCode> ReleaseDriverPtr => &ReleaseDriver;
        private static unsafe delegate* unmanaged<CAdbcPartitions*, void> ReleasePartitionsPtr => &ReleasePartitions;

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
        private static unsafe delegate* unmanaged<CAdbcConnection*, int*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetInfoPtr => &GetConnectionInfo;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionReadPartitionPtr => &ReadConnectionPartition;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> ConnectionSetOptionPtr => &SetConnectionOption;

        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArray*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementBindPtr => &BindStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> StatementBindStreamPtr => &BindStreamStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, long*, CAdbcError*, AdbcStatusCode> StatementExecuteQueryPtr => &ExecuteStatementQuery;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcPartitions*, long*, CAdbcError*, AdbcStatusCode> StatementExecutePartitionsPtr => &ExecuteStatementPartitions;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementExecuteSchemaPtr => &ExecuteStatementSchema;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementNewPtr => &NewStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementReleasePtr => &ReleaseStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementPreparePtr => &PrepareStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, CAdbcError*, AdbcStatusCode> StatementSetOptionPtr => &SetStatementOption;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode> StatementSetSqlQueryPtr => &SetStatementSqlQuery;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, int, CAdbcError*, AdbcStatusCode> StatementSetSubstraitPlanPtr => &SetStatementSubstraitPlan;
        private static unsafe delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementGetParameterSchemaPtr => &GetStatementParameterSchema;

        // ADBC 1.1.0
        private static unsafe delegate* unmanaged<CAdbcError*, int> ErrorGetDetailCountPtr => &GetErrorDetailCount;
        private static unsafe delegate* unmanaged<CAdbcError*, int, CAdbcErrorDetail> ErrorGetDetailPtr => &GetErrorDetail;

        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionPtr => &GetDatabaseOption;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionBytesPtr => &GetDatabaseOptionBytes;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, double*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionDoublePtr => &GetDatabaseOptionDouble;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, long*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionIntPtr => &GetDatabaseOptionInt;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> DatabaseSetOptionBytesPtr => &SetDatabaseOptionBytes;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, double, CAdbcError*, AdbcStatusCode> DatabaseSetOptionDoublePtr => &SetDatabaseOptionDouble;
        private static unsafe delegate* unmanaged<CAdbcDatabase*, byte*, long, CAdbcError*, AdbcStatusCode> DatabaseSetOptionIntPtr => &SetDatabaseOptionInt;

        private static unsafe delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionCancelPtr => &CancelConnection;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionPtr => &GetConnectionOption;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionBytesPtr => &GetConnectionOptionBytes;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, double*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionDoublePtr => &GetConnectionOptionDouble;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, long*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionIntPtr => &GetConnectionOptionInt;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, byte, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetStatisticsPtr => &GetConnectionStatistics;
        private static unsafe delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetStatisticNamesPtr => &GetConnectionStatisticNames;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> ConnectionSetOptionBytesPtr => &SetConnectionOptionBytes;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, double, CAdbcError*, AdbcStatusCode> ConnectionSetOptionDoublePtr => &SetConnectionOptionDouble;
        private static unsafe delegate* unmanaged<CAdbcConnection*, byte*, long, CAdbcError*, AdbcStatusCode> ConnectionSetOptionIntPtr => &SetConnectionOptionInt;

        private static unsafe delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementCancelPtr => &CancelStatement;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> StatementGetOptionPtr => &GetStatementOption;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> StatementGetOptionBytesPtr => &GetStatementOptionBytes;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, double*, CAdbcError*, AdbcStatusCode> StatementGetOptionDoublePtr => &GetStatementOptionDouble;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, long*, CAdbcError*, AdbcStatusCode> StatementGetOptionIntPtr => &GetStatementOptionInt;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> StatementSetOptionBytesPtr => &SetStatementOptionBytes;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, double, CAdbcError*, AdbcStatusCode> StatementSetOptionDoublePtr => &SetStatementOptionDouble;
        private static unsafe delegate* unmanaged<CAdbcStatement*, byte*, long, CAdbcError*, AdbcStatusCode> StatementSetOptionIntPtr => &SetStatementOptionInt;
#else
        internal static unsafe IntPtr ReleaseErrorPtr => s_releaseError.Pointer;
        private static unsafe IntPtr ReleaseDriverPtr = NativeDelegate<DriverRelease>.AsNativePointer(ReleaseDriver);
        private static unsafe IntPtr ReleasePartitionsPtr = NativeDelegate<PartitionsRelease>.AsNativePointer(ReleasePartitions);

        private static unsafe IntPtr DatabaseInitPtr = NativeDelegate<DatabaseFn>.AsNativePointer(InitDatabase);
        private static unsafe IntPtr DatabaseReleasePtr = NativeDelegate<DatabaseFn>.AsNativePointer(ReleaseDatabase);
        private static unsafe IntPtr DatabaseSetOptionPtr = NativeDelegate<DatabaseSetOption>.AsNativePointer(SetDatabaseOption);

        private static unsafe IntPtr ConnectionGetObjectsPtr = NativeDelegate<ConnectionGetObjects>.AsNativePointer(GetConnectionObjects);
        private static unsafe IntPtr ConnectionGetTableSchemaPtr = NativeDelegate<ConnectionGetTableSchema>.AsNativePointer(GetConnectionTableSchema);
        private static unsafe IntPtr ConnectionGetTableTypesPtr = NativeDelegate<ConnectionGetTableTypes>.AsNativePointer(GetConnectionTableTypes);
        private static unsafe IntPtr ConnectionInitPtr = NativeDelegate<ConnectionInit>.AsNativePointer(InitConnection);
        private static unsafe IntPtr ConnectionRollbackPtr = NativeDelegate<ConnectionFn>.AsNativePointer(RollbackConnection);
        private static unsafe IntPtr ConnectionCommitPtr = NativeDelegate<ConnectionFn>.AsNativePointer(CommitConnection);
        private static unsafe IntPtr ConnectionReleasePtr = NativeDelegate<ConnectionFn>.AsNativePointer(ReleaseConnection);
        private static unsafe IntPtr ConnectionGetInfoPtr = NativeDelegate<ConnectionGetInfo>.AsNativePointer(GetConnectionInfo);
        private static unsafe IntPtr ConnectionReadPartitionPtr = NativeDelegate<ConnectionReadPartition>.AsNativePointer(ReadConnectionPartition);
        private static unsafe IntPtr ConnectionSetOptionPtr = NativeDelegate<ConnectionSetOption>.AsNativePointer(SetConnectionOption);

        private static unsafe IntPtr StatementBindPtr = NativeDelegate<StatementBind>.AsNativePointer(BindStatement);
        private static unsafe IntPtr StatementBindStreamPtr = NativeDelegate<StatementBindStream>.AsNativePointer(BindStreamStatement);
        private static unsafe IntPtr StatementExecuteQueryPtr = NativeDelegate<StatementExecuteQuery>.AsNativePointer(ExecuteStatementQuery);
        private static unsafe IntPtr StatementExecutePartitionsPtr = NativeDelegate<StatementExecutePartitions>.AsNativePointer(ExecuteStatementPartitions);
        private static unsafe IntPtr StatementExecuteSchemaPtr = NativeDelegate<StatementExecuteSchema>.AsNativePointer(ExecuteStatementSchema);
        private static unsafe IntPtr StatementNewPtr = NativeDelegate<StatementNew>.AsNativePointer(NewStatement);
        private static unsafe IntPtr StatementReleasePtr = NativeDelegate<StatementFn>.AsNativePointer(ReleaseStatement);
        private static unsafe IntPtr StatementPreparePtr = NativeDelegate<StatementFn>.AsNativePointer(PrepareStatement);
        private static unsafe IntPtr StatementSetOptionPtr = NativeDelegate<StatementSetOption>.AsNativePointer(SetStatementOption);
        private static unsafe IntPtr StatementSetSqlQueryPtr = NativeDelegate<StatementSetSqlQuery>.AsNativePointer(SetStatementSqlQuery);
        private static unsafe IntPtr StatementSetSubstraitPlanPtr = NativeDelegate<StatementSetSubstraitPlan>.AsNativePointer(SetStatementSubstraitPlan);
        private static unsafe IntPtr StatementGetParameterSchemaPtr = NativeDelegate<StatementGetParameterSchema>.AsNativePointer(GetStatementParameterSchema);

        // ADBC 1.1.0
        private static unsafe IntPtr ErrorGetDetailCountPtr = NativeDelegate<ErrorGetDetailCount>.AsNativePointer(GetErrorDetailCount);
        private static unsafe IntPtr ErrorGetDetailPtr = NativeDelegate<ErrorGetDetail>.AsNativePointer(GetErrorDetail);

        private static unsafe IntPtr DatabaseGetOptionPtr = NativeDelegate<DatabaseGetOption>.AsNativePointer(GetDatabaseOption);
        private static unsafe IntPtr DatabaseGetOptionBytesPtr = NativeDelegate<DatabaseGetOptionBytes>.AsNativePointer(GetDatabaseOptionBytes);
        private static unsafe IntPtr DatabaseGetOptionDoublePtr = NativeDelegate<DatabaseGetOptionDouble>.AsNativePointer(GetDatabaseOptionDouble);
        private static unsafe IntPtr DatabaseGetOptionIntPtr = NativeDelegate<DatabaseGetOptionInt>.AsNativePointer(GetDatabaseOptionInt);
        private static unsafe IntPtr DatabaseSetOptionBytesPtr = NativeDelegate<DatabaseSetOptionBytes>.AsNativePointer(SetDatabaseOptionBytes);
        private static unsafe IntPtr DatabaseSetOptionDoublePtr = NativeDelegate<DatabaseSetOptionDouble>.AsNativePointer(SetDatabaseOptionDouble);
        private static unsafe IntPtr DatabaseSetOptionIntPtr = NativeDelegate<DatabaseSetOptionInt>.AsNativePointer(SetDatabaseOptionInt);

        private static unsafe IntPtr ConnectionCancelPtr = NativeDelegate<ConnectionFn>.AsNativePointer(CancelConnection);
        private static unsafe IntPtr ConnectionGetOptionPtr = NativeDelegate<ConnectionGetOption>.AsNativePointer(GetConnectionOption);
        private static unsafe IntPtr ConnectionGetOptionBytesPtr = NativeDelegate<ConnectionGetOptionBytes>.AsNativePointer(GetConnectionOptionBytes);
        private static unsafe IntPtr ConnectionGetOptionDoublePtr = NativeDelegate<ConnectionGetOptionDouble>.AsNativePointer(GetConnectionOptionDouble);
        private static unsafe IntPtr ConnectionGetOptionIntPtr = NativeDelegate<ConnectionGetOptionInt>.AsNativePointer(GetConnectionOptionInt);
        private static unsafe IntPtr ConnectionGetStatisticsPtr = NativeDelegate<ConnectionGetStatistics>.AsNativePointer(GetConnectionStatistics);
        private static unsafe IntPtr ConnectionGetStatisticNamesPtr = NativeDelegate<ConnectionGetStatisticNames>.AsNativePointer(GetConnectionStatisticNames);
        private static unsafe IntPtr ConnectionSetOptionBytesPtr = NativeDelegate<ConnectionSetOptionBytes>.AsNativePointer(SetConnectionOptionBytes);
        private static unsafe IntPtr ConnectionSetOptionDoublePtr = NativeDelegate<ConnectionSetOptionDouble>.AsNativePointer(SetConnectionOptionDouble);
        private static unsafe IntPtr ConnectionSetOptionIntPtr = NativeDelegate<ConnectionSetOptionInt>.AsNativePointer(SetConnectionOptionInt);

        private static unsafe IntPtr StatementCancelPtr = NativeDelegate<StatementFn>.AsNativePointer(CancelStatement);
        private static unsafe IntPtr StatementGetOptionPtr = NativeDelegate<StatementGetOption>.AsNativePointer(GetStatementOption);
        private static unsafe IntPtr StatementGetOptionBytesPtr = NativeDelegate<StatementGetOptionBytes>.AsNativePointer(GetStatementOptionBytes);
        private static unsafe IntPtr StatementGetOptionDoublePtr = NativeDelegate<StatementGetOptionDouble>.AsNativePointer(GetStatementOptionDouble);
        private static unsafe IntPtr StatementGetOptionIntPtr = NativeDelegate<StatementGetOptionInt>.AsNativePointer(GetStatementOptionInt);
        private static unsafe IntPtr StatementSetOptionBytesPtr = NativeDelegate<StatementSetOptionBytes>.AsNativePointer(SetStatementOptionBytes);
        private static unsafe IntPtr StatementSetOptionDoublePtr = NativeDelegate<StatementSetOptionDouble>.AsNativePointer(SetStatementOptionDouble);
        private static unsafe IntPtr StatementSetOptionIntPtr = NativeDelegate<StatementSetOptionInt>.AsNativePointer(SetStatementOptionInt);
#endif

        public unsafe static AdbcStatusCode AdbcDriverInit(int version, CAdbcDriver* nativeDriver, CAdbcError* error, AdbcDriver driver)
        {
            if (version != AdbcVersion.Version_1_0_0 && version != AdbcVersion.Version_1_1_0)
            {
                return AdbcStatusCode.NotImplemented;
            }

            // Under NativeAOT on Linux, a thread that runs UTF8Encoding's static constructor and
            // contends on another type's constructor lock re-enters it while initializing Lock,
            // finds the encoding still null, and fails fast. A newly started thread does this
            // when it sets its name, so run it here, before the exporter starts any threads.
            // See https://github.com/dotnet/runtime/issues/136031.
            _ = Encoding.UTF8;

            DriverStub stub = new DriverStub(driver);
            GCHandle handle = GCHandle.Alloc(stub);
            nativeDriver->private_data = (void*)GCHandle.ToIntPtr(handle);
            nativeDriver->release = ReleaseDriverPtr;

            nativeDriver->DatabaseInit = DatabaseInitPtr;
            nativeDriver->DatabaseNew = stub.NewDatabasePtr;
            nativeDriver->DatabaseSetOption = DatabaseSetOptionPtr;
            nativeDriver->DatabaseRelease = DatabaseReleasePtr;

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
            nativeDriver->StatementBindStream = StatementBindStreamPtr;
            nativeDriver->StatementExecuteQuery = StatementExecuteQueryPtr;
            nativeDriver->StatementExecutePartitions = StatementExecutePartitionsPtr;
            nativeDriver->StatementGetParameterSchema = StatementGetParameterSchemaPtr;
            nativeDriver->StatementNew = StatementNewPtr;
            nativeDriver->StatementPrepare = StatementPreparePtr;
            nativeDriver->StatementRelease = StatementReleasePtr;
            nativeDriver->StatementSetOption = StatementSetOptionPtr;
            nativeDriver->StatementSetSqlQuery = StatementSetSqlQueryPtr;
            nativeDriver->StatementSetSubstraitPlan = StatementSetSubstraitPlanPtr;

            if (version >= AdbcVersion.Version_1_1_0)
            {
                nativeDriver->ErrorGetDetailCount = ErrorGetDetailCountPtr;
                nativeDriver->ErrorGetDetail = ErrorGetDetailPtr;
                // ErrorFromArrayStream is left null: exported streams report errors
                // through ArrowArrayStream.get_last_error only.

                nativeDriver->DatabaseGetOption = DatabaseGetOptionPtr;
                nativeDriver->DatabaseGetOptionBytes = DatabaseGetOptionBytesPtr;
                nativeDriver->DatabaseGetOptionDouble = DatabaseGetOptionDoublePtr;
                nativeDriver->DatabaseGetOptionInt = DatabaseGetOptionIntPtr;
                nativeDriver->DatabaseSetOptionBytes = DatabaseSetOptionBytesPtr;
                nativeDriver->DatabaseSetOptionDouble = DatabaseSetOptionDoublePtr;
                nativeDriver->DatabaseSetOptionInt = DatabaseSetOptionIntPtr;

                nativeDriver->ConnectionCancel = ConnectionCancelPtr;
                nativeDriver->ConnectionGetOption = ConnectionGetOptionPtr;
                nativeDriver->ConnectionGetOptionBytes = ConnectionGetOptionBytesPtr;
                nativeDriver->ConnectionGetOptionDouble = ConnectionGetOptionDoublePtr;
                nativeDriver->ConnectionGetOptionInt = ConnectionGetOptionIntPtr;
                nativeDriver->ConnectionGetStatistics = ConnectionGetStatisticsPtr;
                nativeDriver->ConnectionGetStatisticNames = ConnectionGetStatisticNamesPtr;
                nativeDriver->ConnectionSetOptionBytes = ConnectionSetOptionBytesPtr;
                nativeDriver->ConnectionSetOptionDouble = ConnectionSetOptionDoublePtr;
                nativeDriver->ConnectionSetOptionInt = ConnectionSetOptionIntPtr;

                nativeDriver->StatementCancel = StatementCancelPtr;
                nativeDriver->StatementExecuteSchema = StatementExecuteSchemaPtr;
                nativeDriver->StatementGetOption = StatementGetOptionPtr;
                nativeDriver->StatementGetOptionBytes = StatementGetOptionBytesPtr;
                nativeDriver->StatementGetOptionDouble = StatementGetOptionDoublePtr;
                nativeDriver->StatementGetOptionInt = StatementGetOptionIntPtr;
                nativeDriver->StatementSetOptionBytes = StatementSetOptionBytesPtr;
                nativeDriver->StatementSetOptionDouble = StatementSetOptionDoublePtr;
                nativeDriver->StatementSetOptionInt = StatementSetOptionIntPtr;
            }

            return 0;
        }

        internal unsafe static void ReleaseError(CAdbcError* error)
        {
            if (error == null)
            {
                return;
            }

            if (error->message != null)
            {
                Marshal.FreeCoTaskMem((IntPtr)error->message);
                error->message = null;
            }

            // private_data only exists when the caller allocated an ADBC 1.1.0 error
            if (error->vendor_code == ErrorVendorCodePrivateData && error->private_data != null)
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)error->private_data);
                (gch.Target as ErrorDetails)?.Dispose();
                gch.Free();
                error->private_data = null;
            }

            error->release = default;
        }

        private unsafe static AdbcStatusCode SetError(CAdbcError* error, Exception exception)
        {
            AdbcStatusCode status = exception switch
            {
                AdbcException adbcException => adbcException.Status,
                OperationCanceledException => AdbcStatusCode.Cancelled,
                ArgumentException => AdbcStatusCode.InvalidArgument,
                _ => AdbcStatusCode.UnknownError,
            };

            // The error is optional
            if (error == null)
            {
                return status;
            }

            try
            {
                FillError(error, exception);
            }
            catch (Exception)
            {
                // SetError runs in the catch block of an unmanaged entry point, where an
                // exception would end the process. Report the status without a message.
            }

            return status;
        }

        private unsafe static void FillError(CAdbcError* error, Exception exception)
        {
            bool supportsDetails = error->vendor_code == ErrorVendorCodePrivateData;

            // The caller may pass an error that still holds a value, possibly from another
            // driver, so release it through its own callback.
            if (error->release != default)
            {
#if NET5_0_OR_GREATER
                error->release(error);
#else
                Marshal.GetDelegateForFunctionPointer<ErrorRelease>(error->release)(error);
#endif
            }
            error->message = null;
            error->release = default;
            if (supportsDetails)
            {
                error->private_data = null;
            }

            error->message = (byte*)MarshalExtensions.StringToCoTaskMemUTF8(exception.Message);
            error->sqlstate0 = (byte)0;
            error->sqlstate1 = (byte)0;
            error->sqlstate2 = (byte)0;
            error->sqlstate3 = (byte)0;
            error->sqlstate4 = (byte)0;
            error->vendor_code = 0;
            error->release = ReleaseErrorPtr;

            if (exception is AdbcException adbc)
            {
                if (adbc.SqlState != null)
                {
                    byte* dest = &error->sqlstate0;
                    fixed (char* sqlState = adbc.SqlState)
                    {
                        int len = Math.Min(5, adbc.SqlState.Length);
                        for (int i = 0; i < len; i++)
                        {
                            dest[i] = unchecked((byte)sqlState[i]);
                        }
                    }
                }

                IReadOnlyList<KeyValuePair<string, byte[]>> details = adbc.Details;
                if (supportsDetails && details.Count > 0)
                {
                    // The caller's error struct has the 1.1.0 layout, so private_data is present.
                    error->vendor_code = ErrorVendorCodePrivateData;
                    error->private_data = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(new ErrorDetails(details)));
                }
                else
                {
                    // Never report the marker value without the private_data that goes with it;
                    // the struct may not even have room for private_data.
                    error->vendor_code = adbc.NativeError == ErrorVendorCodePrivateData ? 0 : adbc.NativeError;
                }
            }
        }

        /// <summary>
        /// Exports a stream produced by the managed driver. The exported stream takes
        /// ownership on success; if exporting fails, nobody else will dispose it.
        /// </summary>
        private unsafe static void ExportStream(IArrowArrayStream? stream, CArrowArrayStream* cStream)
        {
            try
            {
                CArrowArrayStreamExporter.ExportArrayStream(stream!, cStream);
            }
            catch
            {
                stream?.Dispose();
                throw;
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
            ((IDisposable)gch.Target!).Dispose();
            gch.Free();
            p = IntPtr.Zero;
        }

        private static unsafe DatabaseStub GetStub(CAdbcDatabase* nativeDatabase) =>
            (DatabaseStub)GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data).Target!;

        private static unsafe ConnectionStub GetStub(CAdbcConnection* nativeConnection) =>
            (ConnectionStub)GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data).Target!;

        private static unsafe IOptionHolder GetOptions(CAdbcStatement* nativeStatement) =>
            new StatementOptions((AdbcStatement)GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data).Target!);

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseDriver(CAdbcDriver* nativeDriver, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDriver->private_data);
                DriverStub stub = (DriverStub)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeDriver->private_data = null;
                s_idleCollector.DriverReleased();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void ReleasePartitions(CAdbcPartitions* partitions)
        {
            FreePartitions(partitions);
        }

        private unsafe static void FreePartitions(CAdbcPartitions* partitions)
        {
            if (partitions != null)
            {
                if (partitions->partitions != null)
                {
                    for (int i = 0; i < partitions->num_partitions; i++)
                    {
                        byte* partition = partitions->partitions[i];
                        if (partition != null)
                        {
                            Marshal.FreeHGlobal((IntPtr)partition);
                            partitions->partitions[i] = null;
                        }
                    }
                    Marshal.FreeHGlobal((IntPtr)partitions->partitions);
                    partitions->partitions = null;
                }
                if (partitions->partition_lengths != null)
                {
                    Marshal.FreeHGlobal((IntPtr)partitions->partition_lengths);
                    partitions->partition_lengths = null;
                }

                partitions->release = default;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int GetErrorDetailCount(CAdbcError* error)
        {
            try
            {
                return GetErrorDetails(error)?.Count ?? 0;
            }
            catch (Exception)
            {
                return 0;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static CAdbcErrorDetail GetErrorDetail(CAdbcError* error, int index)
        {
            try
            {
                return GetErrorDetails(error)?.GetDetail(index) ?? default;
            }
            catch (Exception)
            {
                return default;
            }
        }

        private unsafe static ErrorDetails? GetErrorDetails(CAdbcError* error)
        {
            if (error == null || error->vendor_code != ErrorVendorCodePrivateData || error->private_data == null)
            {
                return null;
            }
            return GCHandle.FromIntPtr((IntPtr)error->private_data).Target as ErrorDetails;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode InitDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target!;
                stub.Init();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseDatabase(CAdbcDatabase* nativeDatabase, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase->private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeDatabase->private_data = null;
                s_idleCollector.DatabaseReleased();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetConnectionOption(CAdbcConnection* nativeConnection, byte* name, byte* value, CAdbcError* error)
        {
            try
            {
                SetStringOption(GetStub(nativeConnection), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetDatabaseOption(CAdbcDatabase* nativeDatabase, byte* name, byte* value, CAdbcError* error)
        {
            try
            {
                SetStringOption(GetStub(nativeDatabase), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetDatabaseOption(CAdbcDatabase* nativeDatabase, byte* name, byte* value, nint* length, CAdbcError* error)
        {
            try
            {
                GetStringOption(GetStub(nativeDatabase), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetDatabaseOptionBytes(CAdbcDatabase* nativeDatabase, byte* name, byte* value, nint* length, CAdbcError* error)
        {
            try
            {
                GetBytesOption(GetStub(nativeDatabase), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetDatabaseOptionDouble(CAdbcDatabase* nativeDatabase, byte* name, double* value, CAdbcError* error)
        {
            try
            {
                GetDoubleOption(GetStub(nativeDatabase), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetDatabaseOptionInt(CAdbcDatabase* nativeDatabase, byte* name, long* value, CAdbcError* error)
        {
            try
            {
                GetInt64Option(GetStub(nativeDatabase), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetDatabaseOptionBytes(CAdbcDatabase* nativeDatabase, byte* name, byte* value, nint length, CAdbcError* error)
        {
            try
            {
                SetBytesOption(GetStub(nativeDatabase), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetDatabaseOptionDouble(CAdbcDatabase* nativeDatabase, byte* name, double value, CAdbcError* error)
        {
            try
            {
                GetStub(nativeDatabase).SetOptionDouble(GetKey(name), value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetDatabaseOptionInt(CAdbcDatabase* nativeDatabase, byte* name, long value, CAdbcError* error)
        {
            try
            {
                GetStub(nativeDatabase).SetOptionInt64(GetKey(name), value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode InitConnection(CAdbcConnection* nativeConnection, CAdbcDatabase* database, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.InitConnection(ref *database);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }


#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionObjects(CAdbcConnection* nativeConnection, int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetObjects(depth, catalog, db_schema, table_name, table_type, column_name, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionTableTypes(CAdbcConnection* nativeConnection, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetTableTypes(stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionTableSchema(CAdbcConnection* nativeConnection, byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* schema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetTableSchema(catalog, db_schema, table_name, schema);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode RollbackConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.Rollback();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode CommitConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.Commit();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeConnection->private_data = null;
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReadConnectionPartition(CAdbcConnection* nativeConnection, byte* serialized_partition, int serialized_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.ReadPartition(serialized_partition, serialized_length, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionInfo(CAdbcConnection* nativeConnection, int* info_codes, int info_codes_length, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.GetInfo(info_codes, info_codes_length, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionStatistics(CAdbcConnection* nativeConnection, byte* catalog, byte* db_schema, byte* table_name, byte approximate, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GetStub(nativeConnection).GetStatistics(catalog, db_schema, table_name, approximate != 0, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionStatisticNames(CAdbcConnection* nativeConnection, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GetStub(nativeConnection).GetStatisticNames(stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionOption(CAdbcConnection* nativeConnection, byte* name, byte* value, nint* length, CAdbcError* error)
        {
            try
            {
                GetStringOption(GetStub(nativeConnection), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionOptionBytes(CAdbcConnection* nativeConnection, byte* name, byte* value, nint* length, CAdbcError* error)
        {
            try
            {
                GetBytesOption(GetStub(nativeConnection), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionOptionDouble(CAdbcConnection* nativeConnection, byte* name, double* value, CAdbcError* error)
        {
            try
            {
                GetDoubleOption(GetStub(nativeConnection), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetConnectionOptionInt(CAdbcConnection* nativeConnection, byte* name, long* value, CAdbcError* error)
        {
            try
            {
                GetInt64Option(GetStub(nativeConnection), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetConnectionOptionBytes(CAdbcConnection* nativeConnection, byte* name, byte* value, nint length, CAdbcError* error)
        {
            try
            {
                SetBytesOption(GetStub(nativeConnection), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetConnectionOptionDouble(CAdbcConnection* nativeConnection, byte* name, double value, CAdbcError* error)
        {
            try
            {
                GetStub(nativeConnection).SetOptionDouble(GetKey(name), value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetConnectionOptionInt(CAdbcConnection* nativeConnection, byte* name, long value, CAdbcError* error)
        {
            try
            {
                GetStub(nativeConnection).SetOptionInt64(GetKey(name), value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementSqlQuery(CAdbcStatement* nativeStatement, byte* text, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                stub.SqlQuery = MarshalExtensions.PtrToStringUTF8(text);

                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementOption(CAdbcStatement* nativeStatement, byte* name, byte* value, CAdbcError* error)
        {
            try
            {
                SetStringOption(GetOptions(nativeStatement), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetStatementOption(CAdbcStatement* nativeStatement, byte* name, byte* value, nint* length, CAdbcError* error)
        {
            try
            {
                GetStringOption(GetOptions(nativeStatement), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetStatementOptionBytes(CAdbcStatement* nativeStatement, byte* name, byte* value, nint* length, CAdbcError* error)
        {
            try
            {
                GetBytesOption(GetOptions(nativeStatement), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetStatementOptionDouble(CAdbcStatement* nativeStatement, byte* name, double* value, CAdbcError* error)
        {
            try
            {
                GetDoubleOption(GetOptions(nativeStatement), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetStatementOptionInt(CAdbcStatement* nativeStatement, byte* name, long* value, CAdbcError* error)
        {
            try
            {
                GetInt64Option(GetOptions(nativeStatement), name, value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementOptionBytes(CAdbcStatement* nativeStatement, byte* name, byte* value, nint length, CAdbcError* error)
        {
            try
            {
                SetBytesOption(GetOptions(nativeStatement), name, value, length);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementOptionDouble(CAdbcStatement* nativeStatement, byte* name, double value, CAdbcError* error)
        {
            try
            {
                GetOptions(nativeStatement).SetOptionDouble(GetKey(name), value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementOptionInt(CAdbcStatement* nativeStatement, byte* name, long value, CAdbcError* error)
        {
            try
            {
                GetOptions(nativeStatement).SetOptionInt64(GetKey(name), value);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode CancelStatement(CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                stub.Cancel();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode CancelConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.Cancel();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode SetStatementSubstraitPlan(CAdbcStatement* nativeStatement, byte* plan, int length, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                stub.SubstraitPlan = MarshalExtensions.MarshalBuffer(plan, length);

                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode GetStatementParameterSchema(CAdbcStatement* nativeStatement, CArrowSchema* schema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                CArrowSchemaExporter.ExportSchema(stub.GetParameterSchema(), schema);

                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode BindStatement(CAdbcStatement* nativeStatement, CArrowArray* array, CArrowSchema* cschema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                Schema schema = CArrowSchemaImporter.ImportSchema(cschema);
                RecordBatch batch = CArrowArrayImporter.ImportRecordBatch(array, schema);
                stub.Bind(batch, schema);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode BindStreamStatement(CAdbcStatement* nativeStatement, CArrowArrayStream* stream, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                IArrowArrayStream arrayStream = CArrowArrayStreamImporter.ImportArrayStream(stream);
                stub.BindStream(arrayStream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ExecuteStatementQuery(CAdbcStatement* nativeStatement, CArrowArrayStream* stream, long* rows, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;

                // A null stream means the caller doesn't expect a result set
                if (stream == null)
                {
                    UpdateResult update = stub.ExecuteUpdate();
                    if (rows != null)
                    {
                        *rows = update.AffectedRows;
                    }
                    return AdbcStatusCode.Success;
                }

                var result = stub.ExecuteQuery();
                if (rows != null)
                {
                    *rows = result.RowCount;
                }

                ExportStream(result.Stream, stream);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ExecuteStatementPartitions(CAdbcStatement* nativeStatement, CArrowSchema* schema, CAdbcPartitions* partitions, long* rows, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                var result = stub.ExecutePartitioned();
                if (rows != null)
                {
                    *rows = result.AffectedRows;
                }

                // Build the result locally and publish it only once it's complete, so that a
                // failure partway through never hands the caller a partially filled struct.
                int count = result.PartitionDescriptors.Count;
                CAdbcPartitions built = new CAdbcPartitions { num_partitions = count };
                try
                {
                    built.partitions = (byte**)Marshal.AllocHGlobal(IntPtr.Size * Math.Max(1, count));
                    for (int i = 0; i < count; i++)
                    {
                        built.partitions[i] = null;
                    }
                    built.partition_lengths = (nuint*)Marshal.AllocHGlobal(IntPtr.Size * Math.Max(1, count));
                    for (int i = 0; i < count; i++)
                    {
                        ReadOnlySpan<byte> partition = result.PartitionDescriptors[i].Descriptor;
                        built.partition_lengths[i] = (nuint)partition.Length;
                        built.partitions[i] = (byte*)Marshal.AllocHGlobal(Math.Max(1, partition.Length));
                        fixed (void* descriptor = partition)
                        {
                            Buffer.MemoryCopy(descriptor, built.partitions[i], partition.Length, partition.Length);
                        }
                    }

                    if (schema != null)
                    {
                        CArrowSchemaExporter.ExportSchema(result.Schema, schema);
                    }
                }
                catch
                {
                    FreePartitions(&built);
                    throw;
                }

                built.release = ReleasePartitionsPtr;
                *partitions = built;
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ExecuteStatementSchema(CAdbcStatement* nativeStatement, CArrowSchema* schema, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                var result = stub.ExecuteSchema();

                CArrowSchemaExporter.ExportSchema(result, schema);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode NewStatement(CAdbcConnection* nativeConnection, CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeConnection->private_data);
                ConnectionStub stub = (ConnectionStub)gch.Target!;
                stub.NewStatement(ref *nativeStatement);
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode ReleaseStatement(CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement stub = (AdbcStatement)gch.Target!;
                stub.Dispose();
                gch.Free();
                nativeStatement->private_data = null;
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static AdbcStatusCode PrepareStatement(CAdbcStatement* nativeStatement, CAdbcError* error)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStatement->private_data);
                AdbcStatement statement = (AdbcStatement)gch.Target!;
                statement.Prepare();
                return AdbcStatusCode.Success;
            }
            catch (Exception e)
            {
                return SetError(error, e);
            }
        }

        private static unsafe string GetKey(byte* key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            return MarshalExtensions.PtrToStringUTF8(key)!;
        }

        private static unsafe void SetStringOption(IOptionHolder holder, byte* key, byte* value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            holder.SetOption(GetKey(key), MarshalExtensions.PtrToStringUTF8(value)!);
        }

        private static unsafe void SetBytesOption(IOptionHolder holder, byte* key, byte* value, nint length)
        {
            if (value == null && length != 0) throw new ArgumentNullException(nameof(value));
            holder.SetOptionBytes(GetKey(key), new ReadOnlySpan<byte>(value, checked((int)length)));
        }

        private static unsafe void GetStringOption(IOptionHolder holder, byte* key, byte* value, nint* length)
        {
            string result = holder.GetOption(GetKey(key));
            CopyOptionValue(Encoding.UTF8.GetBytes(result), nullTerminate: true, value, length);
        }

        private static unsafe void GetBytesOption(IOptionHolder holder, byte* key, byte* value, nint* length)
        {
            CopyOptionValue(holder.GetOptionBytes(GetKey(key)), nullTerminate: false, value, length);
        }

        private static unsafe void GetInt64Option(IOptionHolder holder, byte* key, long* value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            *value = holder.GetOptionInt64(GetKey(key));
        }

        private static unsafe void GetDoubleOption(IOptionHolder holder, byte* key, double* value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            *value = holder.GetOptionDouble(GetKey(key));
        }

        /// <summary>
        /// Implements the buffer protocol of the AdbcXxxGetOption and AdbcXxxGetOptionBytes
        /// functions: on input <paramref name="length"/> is the size of the buffer; on output it is
        /// the size of the value. The value is only written if it fits.
        /// </summary>
        private static unsafe void CopyOptionValue(byte[] data, bool nullTerminate, byte* value, nint* length)
        {
            if (length == null) throw new ArgumentNullException(nameof(length));

            int required = data.Length + (nullTerminate ? 1 : 0);
            if (value != null && *length >= required)
            {
                data.AsSpan().CopyTo(new Span<byte>(value, data.Length));
                if (nullTerminate)
                {
                    value[data.Length] = 0;
                }
            }
            *length = required;
        }

        /// <summary>
        /// The operations shared by databases, connections and statements for
        /// getting and setting options.
        /// </summary>
        private interface IOptionHolder
        {
            void SetOption(string key, string value);
            void SetOptionInt64(string key, long value);
            void SetOptionDouble(string key, double value);
            void SetOptionBytes(string key, ReadOnlySpan<byte> value);
            string GetOption(string key);
            long GetOptionInt64(string key);
            double GetOptionDouble(string key);
            byte[] GetOptionBytes(string key);
        }

        /// <summary>
        /// Options set on a database or connection before it is initialized. Values
        /// are strings, longs, doubles or byte arrays.
        /// </summary>
        private sealed class PendingOptions
        {
            // GetOption must be thread-safe per the ADBC specification, so access to
            // the recorded values is synchronized.
            private readonly Dictionary<string, object> _options = new Dictionary<string, object>();

            public void Set(string key, object value)
            {
                lock (_options)
                {
                    _options[key] = value;
                }
            }

            public KeyValuePair<string, object>[] Snapshot()
            {
                lock (_options)
                {
                    return _options.ToArray();
                }
            }

            public bool TryGet(string key, out object? value)
            {
                lock (_options)
                {
                    return _options.TryGetValue(key, out value);
                }
            }

            public static string AsString(string key, object value) =>
                value is byte[]? throw new AdbcException($"Option '{key}' has a binary value", AdbcStatusCode.InvalidArgument) : AdbcOptionValue.ToOptionString(key, value);

            public static long AsInt64(string key, object value) =>
                value is long l ? l : AdbcOptionValue.ParseInt64(key, AsString(key, value));

            public static double AsDouble(string key, object value) =>
                value switch
                {
                    double d => d,
                    long l => l,
                    _ => AdbcOptionValue.ParseDouble(key, AsString(key, value)),
                };

            public static byte[] AsBytes(string key, object value) =>
                value as byte[] ?? throw new AdbcException($"Option '{key}' does not have a binary value", AdbcStatusCode.InvalidArgument);

            /// <summary>
            /// Gets an option value from <paramref name="getter"/>, falling back to the value
            /// recorded here if the managed implementation doesn't know about the option.
            /// </summary>
            public T Get<T>(string key, Func<string, T>? getter, Func<string, object, T> convert)
            {
                if (getter != null)
                {
                    try
                    {
                        return getter(key);
                    }
                    catch (AdbcException e) when (
                        (e.Status == AdbcStatusCode.NotFound || e.Status == AdbcStatusCode.NotImplemented) &&
                        TryGet(key, out _))
                    {
                    }
                }

                return TryGet(key, out object? value) ? convert(key, value!) : throw AdbcOptionValue.NotFound(key);
            }
        }

        /// <summary>
        /// Unmanaged copies of <see cref="AdbcException.Details"/>, owned by a <see cref="CAdbcError"/>.
        /// </summary>
        private sealed class ErrorDetails : IDisposable
        {
            private readonly IntPtr[] _keys;
            private readonly IntPtr[] _values;
            private readonly int[] _lengths;

            public ErrorDetails(IReadOnlyList<KeyValuePair<string, byte[]>> details)
            {
                _keys = new IntPtr[details.Count];
                _values = new IntPtr[details.Count];
                _lengths = new int[details.Count];
                for (int i = 0; i < details.Count; i++)
                {
                    byte[] value = details[i].Value ?? System.Array.Empty<byte>();
                    _keys[i] = MarshalExtensions.StringToCoTaskMemUTF8(details[i].Key ?? string.Empty);
                    _values[i] = Marshal.AllocCoTaskMem(Math.Max(1, value.Length));
                    _lengths[i] = value.Length;
                    Marshal.Copy(value, 0, _values[i], value.Length);
                }
            }

            public int Count => _keys.Length;

            public unsafe CAdbcErrorDetail GetDetail(int index)
            {
                if (index < 0 || index >= _keys.Length)
                {
                    return default;
                }

                return new CAdbcErrorDetail
                {
                    key = (byte*)_keys[index],
                    value = (byte*)_values[index],
                    length = _lengths[index],
                };
            }

            public void Dispose()
            {
                for (int i = 0; i < _keys.Length; i++)
                {
                    Marshal.FreeCoTaskMem(_keys[i]);
                    Marshal.FreeCoTaskMem(_values[i]);
                    _keys[i] = IntPtr.Zero;
                    _values[i] = IntPtr.Zero;
                }
            }
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
                try
                {
                    DatabaseStub stub = new DatabaseStub(_driver);
                    GCHandle handle = GCHandle.Alloc(stub);
                    nativeDatabase->private_data = (void*)GCHandle.ToIntPtr(handle);
                    s_idleCollector.DatabaseOpened();
                    return AdbcStatusCode.Success;
                }
                catch (Exception e)
                {
                    return SetError(error, e);
                }
            }

            private unsafe AdbcStatusCode NewConnection(CAdbcConnection* nativeConnection, CAdbcError* error)
            {
                try
                {
                    ConnectionStub stub = new ConnectionStub(_driver);
                    GCHandle handle = GCHandle.Alloc(stub);
                    nativeConnection->private_data = (void*)GCHandle.ToIntPtr(handle);
                    return AdbcStatusCode.Success;
                }
                catch (Exception e)
                {
                    return SetError(error, e);
                }
            }

            public void Dispose()
            {
                _driver.Dispose();
            }
        }

        sealed class DatabaseStub : IDisposable, IOptionHolder
        {
            readonly AdbcDriver _driver;
            readonly PendingOptions options;
            AdbcDatabase? database;

            public DatabaseStub(AdbcDriver driver)
            {
                _driver = driver;
                options = new PendingOptions();
            }

            public void Init()
            {
                if (database != null) throw new InvalidOperationException("database already initialized");

                KeyValuePair<string, object>[] values = options.Snapshot();
                if (values.All(pair => pair.Value is string))
                {
                    Dictionary<string, string> stringOptions = new Dictionary<string, string>();
                    foreach (KeyValuePair<string, object> pair in values)
                    {
                        stringOptions[pair.Key] = (string)pair.Value;
                    }
                    database = _driver.Open(stringOptions);
                }
                else
                {
                    database = _driver.Open(values.ToDictionary(pair => pair.Key, pair => pair.Value));
                }
            }

            // Options are recorded even after initialization so that DatabaseGetOption can
            // report them when the managed database doesn't implement GetOption itself.

            public void SetOption(string key, string value)
            {
                database?.SetOption(key, value);
                options.Set(key, value);
            }

            public void SetOptionInt64(string key, long value)
            {
                database?.SetOptionInt64(key, value);
                options.Set(key, value);
            }

            public void SetOptionDouble(string key, double value)
            {
                database?.SetOptionDouble(key, value);
                options.Set(key, value);
            }

            public void SetOptionBytes(string key, ReadOnlySpan<byte> value)
            {
                database?.SetOptionBytes(key, value);
                options.Set(key, value.ToArray());
            }

            public string GetOption(string key) => options.Get(key, database == null ? null : database.GetOption, PendingOptions.AsString);
            public long GetOptionInt64(string key) => options.Get(key, database == null ? null : database.GetOptionInt64, PendingOptions.AsInt64);
            public double GetOptionDouble(string key) => options.Get(key, database == null ? null : database.GetOptionDouble, PendingOptions.AsDouble);
            public byte[] GetOptionBytes(string key) => options.Get(key, database == null ? null : database.GetOptionBytes, PendingOptions.AsBytes);

            public AdbcDatabase Database => database ?? throw new InvalidOperationException("database not initialized");

            public void Dispose()
            {
                database?.Dispose();
                database = null;
            }
        }

        sealed class ConnectionStub : IDisposable, IOptionHolder
        {
            readonly AdbcDriver _driver;
            readonly PendingOptions options;
            AdbcConnection? connection;

            // Values of the transaction options that were passed to Connect. The base
            // AdbcConnection reports defaults for these rather than failing, so a driver that
            // honors them in Connect but doesn't override the property getters would otherwise
            // report the wrong value. A key is removed once it's set through the property,
            // because a driver with a working setter is expected to have a working getter.
            // Replaced rather than mutated so that GetOption can read it without locking.
            volatile Dictionary<string, string> consumedAtConnect = new Dictionary<string, string>();

            public ConnectionStub(AdbcDriver driver)
            {
                _driver = driver;
                options = new PendingOptions();
            }

            internal AdbcConnection Connection => connection == null ? throw new InvalidOperationException("connection is not open") : connection;

            public void SetOption(string key, string value)
            {
                if (connection != null)
                {
                    switch (key)
                    {
                        case AdbcOptions.Connection.Autocommit:
                            connection.AutoCommit = ParseOptionValue(key, value, AdbcOptions.GetEnabled);
                            ForgetConsumedAtConnect(key);
                            break;
                        case AdbcOptions.Connection.IsolationLevel:
                            connection.IsolationLevel = ParseOptionValue(key, value, AdbcOptions.GetIsolationLevel);
                            ForgetConsumedAtConnect(key);
                            break;
                        case AdbcOptions.Connection.ReadOnly:
                            connection.ReadOnly = ParseOptionValue(key, value, AdbcOptions.GetEnabled);
                            ForgetConsumedAtConnect(key);
                            break;
                        case AdbcOptions.Connection.CurrentCatalog:
                            connection.CurrentCatalog = value;
                            break;
                        case AdbcOptions.Connection.CurrentDbSchema:
                            connection.CurrentDbSchema = value;
                            break;
                        default:
                            connection.SetOption(key, value);
                            break;
                    }
                }
                options.Set(key, value);
            }

            public void SetOptionInt64(string key, long value)
            {
                connection?.SetOptionInt64(key, value);
                options.Set(key, value);
            }

            public void SetOptionDouble(string key, double value)
            {
                connection?.SetOptionDouble(key, value);
                options.Set(key, value);
            }

            public void SetOptionBytes(string key, ReadOnlySpan<byte> value)
            {
                connection?.SetOptionBytes(key, value);
                options.Set(key, value.ToArray());
            }

            public string GetOption(string key)
            {
                if (consumedAtConnect.TryGetValue(key, out string? value))
                {
                    return value;
                }

                AdbcConnection? current = connection;
                return options.Get(key, current == null ? null : current.GetOption, PendingOptions.AsString);
            }

            private static T ParseOptionValue<T>(string key, string value, Func<string, T> parse)
            {
                try
                {
                    return parse(value);
                }
                catch (NotSupportedException)
                {
                    throw new AdbcException($"Invalid value '{value}' for option '{key}'", AdbcStatusCode.InvalidArgument);
                }
            }

            private void ForgetConsumedAtConnect(string key)
            {
                Dictionary<string, string> current = consumedAtConnect;
                if (current.ContainsKey(key))
                {
                    Dictionary<string, string> updated = new Dictionary<string, string>(current);
                    updated.Remove(key);
                    consumedAtConnect = updated;
                }
            }
            public long GetOptionInt64(string key) => options.Get(key, connection == null ? null : connection.GetOptionInt64, PendingOptions.AsInt64);
            public double GetOptionDouble(string key) => options.Get(key, connection == null ? null : connection.GetOptionDouble, PendingOptions.AsDouble);
            public byte[] GetOptionBytes(string key) => options.Get(key, connection == null ? null : connection.GetOptionBytes, PendingOptions.AsBytes);

            public void Rollback() { Connection.Rollback(); }
            public void Commit() { Connection.Commit(); }
            public void Cancel() { Connection.Cancel(); }

            public void Dispose()
            {
                connection?.Dispose();
                connection = null;
            }

            public unsafe void GetObjects(int depth, byte* catalog, byte* db_schema, byte* table_name, byte** table_type, byte* column_name, CArrowArrayStream* cstream)
            {
                string? catalogPattern = MarshalExtensions.PtrToStringUTF8(catalog);
                string? dbSchemaPattern = MarshalExtensions.PtrToStringUTF8(db_schema);
                string? tableNamePattern = MarshalExtensions.PtrToStringUTF8(table_name);
                string? columnNamePattern = MarshalExtensions.PtrToStringUTF8(column_name);

                string[]? tableTypes = null;
                const int maxTableTypeCount = 100;
                if (table_type != null)
                {
                    int count = 0;
                    while (table_type[count] != null && count <= maxTableTypeCount)
                    {
                        count++;
                    }

                    if (count > maxTableTypeCount)
                    {
                        throw new InvalidOperationException($"We do not expect to get more than {maxTableTypeCount} table types");
                    }

                    tableTypes = new string[count];
                    for (int i = 0; i < count; i++)
                    {
                        tableTypes[i] = MarshalExtensions.PtrToStringUTF8((IntPtr)table_type[i])!;
                    }
                }

                AdbcConnection.GetObjectsDepth goDepth = (AdbcConnection.GetObjectsDepth)depth;

                IArrowArrayStream stream = Connection.GetObjects(goDepth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern);

                ExportStream(stream, cstream);
            }

            public unsafe void GetTableSchema(byte* catalog, byte* db_schema, byte* table_name, CArrowSchema* cschema)
            {
                string? sCatalog = MarshalExtensions.PtrToStringUTF8(catalog);
                string? sDbSchema = MarshalExtensions.PtrToStringUTF8(db_schema);
                string sTableName = MarshalExtensions.PtrToStringUTF8(table_name)!;

                Schema schema = Connection.GetTableSchema(sCatalog, sDbSchema, sTableName);

                CArrowSchemaExporter.ExportSchema(schema, cschema);
            }

            public unsafe void GetTableTypes(CArrowArrayStream* cArrayStream)
            {
                ExportStream(Connection.GetTableTypes(), cArrayStream);
            }

            public unsafe void GetStatistics(byte* catalog, byte* db_schema, byte* table_name, bool approximate, CArrowArrayStream* cArrayStream)
            {
                string? catalogPattern = MarshalExtensions.PtrToStringUTF8(catalog);
                string? dbSchemaPattern = MarshalExtensions.PtrToStringUTF8(db_schema);
                // The C API allows a null table name, meaning "all tables".
                string tableNamePattern = MarshalExtensions.PtrToStringUTF8(table_name)!;

                ExportStream(
                    Connection.GetStatistics(catalogPattern, dbSchemaPattern, tableNamePattern, approximate),
                    cArrayStream);
            }

            public unsafe void GetStatisticNames(CArrowArrayStream* cArrayStream)
            {
                ExportStream(Connection.GetStatisticsNames(), cArrayStream);
            }

            public unsafe void ReadPartition(byte* serializedPartition, int serialized_length, CArrowArrayStream* stream)
            {
                byte[] partition = new byte[serialized_length];
                fixed (byte* partitionPtr = partition)
                {
                    Buffer.MemoryCopy(serializedPartition, partitionPtr, serialized_length, serialized_length);
                }

                ExportStream(Connection.ReadPartition(new PartitionDescriptor(partition)), stream);
            }

            public unsafe void GetInfo(int* info_codes, int info_codes_length, CArrowArrayStream* stream)
            {
                AdbcInfoCode[] infoCodes = new AdbcInfoCode[info_codes_length];
                fixed (AdbcInfoCode* infoCodesPtr = infoCodes)
                {
                    long length = (long)info_codes_length * sizeof(int);
                    Buffer.MemoryCopy(info_codes, infoCodesPtr, length, length);
                }

                ExportStream(Connection.GetInfo(infoCodes.ToList()), stream);
            }

            public unsafe void InitConnection(ref CAdbcDatabase nativeDatabase)
            {
                if (connection != null) throw new InvalidOperationException("connection already initialized");

                GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeDatabase.private_data);
                DatabaseStub stub = (DatabaseStub)gch.Target!;

                // Use the string overload when possible so that existing drivers keep going
                // through the method they override.
                KeyValuePair<string, object>[] values = options.Snapshot();
                AdbcConnection opened;
                if (values.All(pair => pair.Value is string))
                {
                    Dictionary<string, string> stringOptions = new Dictionary<string, string>(values.Length);
                    foreach (KeyValuePair<string, object> pair in values)
                    {
                        stringOptions[pair.Key] = (string)pair.Value;
                    }
                    opened = stub.Database.Connect(stringOptions);
                }
                else
                {
                    opened = stub.Database.ConnectWithTypedOptions(values.ToDictionary(pair => pair.Key, pair => pair.Value));
                }

                Dictionary<string, string> consumed = new Dictionary<string, string>();
                foreach (KeyValuePair<string, object> pair in values)
                {
                    switch (pair.Key)
                    {
                        case AdbcOptions.Connection.Autocommit:
                        case AdbcOptions.Connection.ReadOnly:
                        case AdbcOptions.Connection.IsolationLevel:
                            consumed[pair.Key] = PendingOptions.AsString(pair.Key, pair.Value);
                            break;
                    }
                }

                consumedAtConnect = consumed;
                connection = opened;
            }

            public unsafe void NewStatement(ref CAdbcStatement nativeStatement)
            {
                AdbcStatement statement = Connection.CreateStatement();
                GCHandle handle = GCHandle.Alloc(statement);
                nativeStatement.private_data = (void*)GCHandle.ToIntPtr(handle);
            }
        }

        sealed class StatementOptions : IOptionHolder
        {
            readonly AdbcStatement _statement;

            public StatementOptions(AdbcStatement statement) { _statement = statement; }

            public void SetOption(string key, string value) => _statement.SetOption(key, value);
            public void SetOptionInt64(string key, long value) => _statement.SetOptionInt64(key, value);
            public void SetOptionDouble(string key, double value) => _statement.SetOptionDouble(key, value);
            public void SetOptionBytes(string key, ReadOnlySpan<byte> value) => _statement.SetOptionBytes(key, value);
            public string GetOption(string key) => _statement.GetOption(key);
            public long GetOptionInt64(string key) => _statement.GetOptionInt64(key);
            public double GetOptionDouble(string key) => _statement.GetOptionDouble(key);
            public byte[] GetOptionBytes(string key) => _statement.GetOptionBytes(key);
        }
    }
}

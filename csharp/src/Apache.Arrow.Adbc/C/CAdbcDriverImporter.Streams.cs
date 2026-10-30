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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.C
{
    public static partial class CAdbcDriverImporter
    {
        /// <summary>
        /// A result stream imported from a driver. When the stream fails, it asks the driver
        /// for the ADBC error behind the failure (see <c>AdbcErrorFromArrayStream</c>) and
        /// throws it as an <see cref="AdbcException"/>.
        /// </summary>
        private sealed unsafe class ImportedArrayStream : IArrowArrayStream
        {
            private readonly ImportedAdbcDriver? _driver;
            private readonly Schema _schema;
            private CArrowArrayStream* _cStream;

            /// <summary>
            /// Imports <paramref name="cStream"/>, which must have been allocated with
            /// <see cref="CArrowArrayStream.Create"/>. On success, the imported stream owns
            /// it and holds a reference to <paramref name="driver"/>, whose callbacks it calls
            /// until it's disposed; on failure, the caller still owns the stream.
            /// </summary>
            public ImportedArrayStream(ImportedAdbcDriver? driver, CArrowArrayStream* cStream)
            {
                if (NativeArrayStream.From(cStream)->release == default)
                {
                    throw new ArgumentException("Tried to import an array stream that has already been released.", nameof(cStream));
                }

                _driver = driver;

                CArrowSchema* cSchema = CArrowSchema.Create();
                try
                {
                    int errno = GetSchema(cStream, cSchema);
                    if (errno != 0)
                    {
                        throw StreamError(cStream, errno);
                    }
                    _schema = CArrowSchemaImporter.ImportSchema(cSchema);
                }
                finally
                {
                    CArrowSchema.Free(cSchema);
                }

                _driver?.AddReference();
                _cStream = cStream;
            }

            ~ImportedArrayStream()
            {
                Dispose();
            }

            public Schema Schema => _schema;

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_cStream == null)
                {
                    throw new ObjectDisposedException(nameof(ImportedArrayStream));
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return new ValueTask<RecordBatch?>(Task.FromCanceled<RecordBatch?>(cancellationToken));
                }

                try
                {
                    return new ValueTask<RecordBatch?>(ReadNext());
                }
                catch (Exception e)
                {
                    return new ValueTask<RecordBatch?>(Task.FromException<RecordBatch?>(e));
                }
            }

            private RecordBatch? ReadNext()
            {
                CArrowArray* cArray = CArrowArray.Create();
                try
                {
                    int errno = GetNext(_cStream, cArray);
                    if (errno != 0)
                    {
                        throw StreamError(_cStream, errno);
                    }

                    // A released array marks the end of the stream
                    if (NativeArray.From(cArray)->release == IntPtr.Zero)
                    {
                        return null;
                    }

                    return CArrowArrayImporter.ImportRecordBatch(cArray, _schema);
                }
                finally
                {
                    CArrowArray.Free(cArray);
                }
            }

            public void Dispose()
            {
                CArrowArrayStream* cStream = _cStream;
                if (cStream != null)
                {
                    _cStream = null;
                    try
                    {
                        FreeStream(cStream);
                    }
                    finally
                    {
                        _driver?.RemoveReference();
                    }
                }
                GC.SuppressFinalize(this);
            }

            /// <summary>
            /// Releases and frees a stream allocated with <see cref="CArrowArrayStream.Create"/>.
            /// </summary>
            /// <remarks>
            /// On .NET Framework, this releases the stream through <c>ArrayStreamCallbacks</c>, so
            /// it can release a stream exported in the same process with any delegate type.
            /// </remarks>
            public static void FreeStream(CArrowArrayStream* cStream)
            {
                NativeArrayStream* nativeStream = NativeArrayStream.From(cStream);
                if (nativeStream->release != default)
                {
#if NET5_0_OR_GREATER
                    nativeStream->release(cStream);
#else
                    ArrayStreamCallbacks.Import<ArrayStreamRelease>(nativeStream->release)(cStream);
#endif
                    nativeStream->release = default;
                }
                CArrowArrayStream.Free(cStream);
            }

            /// <summary>
            /// Builds the exception for a stream call that returned <paramref name="errno"/>:
            /// the driver's ADBC error if it reports one, and otherwise the stream's last error.
            /// </summary>
            private Exception StreamError(CArrowArrayStream* cStream, int errno)
            {
                if (_driver != null && _driver.DriverVersion >= AdbcVersion.Version_1_1_0)
                {
                    ref CAdbcDriver nativeDriver = ref _driver.DriverUnsafe;
                    if (nativeDriver.ErrorFromArrayStream != default)
                    {
                        AdbcStatusCode status = AdbcStatusCode.UnknownError;
#if NET5_0_OR_GREATER
                        CAdbcError* error = nativeDriver.ErrorFromArrayStream(cStream, &status);
#else
                        CAdbcError* error = Marshal.GetDelegateForFunctionPointer<ErrorFromArrayStream>(nativeDriver.ErrorFromArrayStream)(cStream, &status);
#endif
                        // The stream owns the error, so it must not be released here
                        if (error != null)
                        {
                            IReadOnlyList<KeyValuePair<string, byte[]>>? details = ReadErrorDetails(_driver, error);
                            return new ImportedAdbcException(
                                ref *error,
                                status == AdbcStatusCode.Success ? AdbcStatusCode.UnknownError : status,
                                details);
                        }
                    }
                }

                NativeArrayStream* nativeStream = NativeArrayStream.From(cStream);
#if NET5_0_OR_GREATER
                byte* message = nativeStream->get_last_error(cStream);
#else
                byte* message = ArrayStreamCallbacks.Import<ArrayStreamGetLastError>(nativeStream->get_last_error)(cStream);
#endif
                return new Exception(message == null
                    ? $"Array stream operation failed with no message. Error code: {errno}"
                    : MarshalExtensions.PtrToStringUTF8(message));
            }

            private static int GetSchema(CArrowArrayStream* cStream, CArrowSchema* cSchema)
            {
                NativeArrayStream* nativeStream = NativeArrayStream.From(cStream);
#if NET5_0_OR_GREATER
                return nativeStream->get_schema(cStream, cSchema);
#else
                return ArrayStreamCallbacks.Import<ArrayStreamGetSchema>(nativeStream->get_schema)(cStream, cSchema);
#endif
            }

            private static int GetNext(CArrowArrayStream* cStream, CArrowArray* cArray)
            {
                NativeArrayStream* nativeStream = NativeArrayStream.From(cStream);
#if NET5_0_OR_GREATER
                return nativeStream->get_next(cStream, cArray);
#else
                return ArrayStreamCallbacks.Import<ArrayStreamGetNext>(nativeStream->get_next)(cStream, cArray);
#endif
            }
        }
    }
}

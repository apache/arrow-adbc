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
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.C
{
    public partial class CAdbcDriverExporter
    {
#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged<CArrowArrayStream*, CArrowSchema*, int> StreamGetSchemaPtr => &GetStreamSchema;
        private static unsafe delegate* unmanaged<CArrowArrayStream*, CArrowArray*, int> StreamGetNextPtr => &GetStreamNext;
        private static unsafe delegate* unmanaged<CArrowArrayStream*, byte*> StreamGetLastErrorPtr => &GetStreamLastError;
        private static unsafe delegate* unmanaged<CArrowArrayStream*, void> StreamReleasePtr => &ReleaseStream;
        private static unsafe delegate* unmanaged<CArrowArrayStream*, AdbcStatusCode*, CAdbcError*> ErrorFromArrayStreamPtr => &GetErrorFromArrayStream;
#else
        private static unsafe IntPtr StreamGetSchemaPtr = NativeDelegate<ArrayStreamGetSchema>.AsNativePointer(GetStreamSchema);
        private static unsafe IntPtr StreamGetNextPtr = NativeDelegate<ArrayStreamGetNext>.AsNativePointer(GetStreamNext);
        private static unsafe IntPtr StreamGetLastErrorPtr = NativeDelegate<ArrayStreamGetLastError>.AsNativePointer(GetStreamLastError);
        private static unsafe IntPtr StreamReleasePtr = NativeDelegate<ArrayStreamRelease>.AsNativePointer(ReleaseStream);
        private static unsafe IntPtr ErrorFromArrayStreamPtr = NativeDelegate<ErrorFromArrayStream>.AsNativePointer(GetErrorFromArrayStream);
#endif

        /// <summary>
        /// Exports a stream produced by the managed driver. The exported stream takes
        /// ownership on success; if exporting fails, nobody else will dispose it.
        /// </summary>
        /// <remarks>
        /// When reading the stream fails, the exported stream keeps the exception so that
        /// <c>AdbcErrorFromArrayStream</c> can report its status, SQLSTATE, vendor code and
        /// details, and returns an errno that corresponds to the status.
        /// </remarks>
        private unsafe static void ExportStream(IArrowArrayStream? stream, CArrowArrayStream* cStream)
        {
            try
            {
                if (stream == null)
                {
                    throw new ArgumentNullException(nameof(stream));
                }
                if (cStream == null)
                {
                    throw new ArgumentNullException(nameof(cStream));
                }

                NativeArrayStream* nativeStream = NativeArrayStream.From(cStream);
                nativeStream->private_data = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(new ExportedStream(stream)));
                nativeStream->get_schema = StreamGetSchemaPtr;
                nativeStream->get_next = StreamGetNextPtr;
                nativeStream->get_last_error = StreamGetLastErrorPtr;
                nativeStream->release = StreamReleasePtr;
            }
            catch
            {
                stream?.Dispose();
                throw;
            }
        }

        private unsafe static ExportedStream? GetExportedStream(CArrowArrayStream* cStream)
        {
            NativeArrayStream* nativeStream = NativeArrayStream.From(cStream);
            if (cStream == null ||
                (IntPtr)nativeStream->release != (IntPtr)StreamReleasePtr ||
                nativeStream->private_data == null)
            {
                return null;
            }
            return GCHandle.FromIntPtr((IntPtr)nativeStream->private_data).Target as ExportedStream;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int GetStreamSchema(CArrowArrayStream* cStream, CArrowSchema* cSchema)
        {
            ExportedStream? stream = null;
            try
            {
                stream = GetExportedStream(cStream) ?? throw new ArgumentException("not a stream exported by this driver", nameof(cStream));
                stream.ClearError();
                CArrowSchemaExporter.ExportSchema(stream.Stream.Schema, cSchema);
                return 0;
            }
            catch (Exception e)
            {
                return stream?.SetError(e) ?? Errno.EINVAL;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int GetStreamNext(CArrowArrayStream* cStream, CArrowArray* cArray)
        {
            ExportedStream? stream = null;
            try
            {
                NativeArray.From(cArray)->release = default;
                stream = GetExportedStream(cStream) ?? throw new ArgumentException("not a stream exported by this driver", nameof(cStream));
                stream.ClearError();

                ValueTask<RecordBatch?> pending = stream.Stream.ReadNextRecordBatchAsync();
                RecordBatch? batch = pending.IsCompletedSuccessfully ? pending.Result : pending.AsTask().GetAwaiter().GetResult();
                if (batch != null)
                {
                    CArrowArrayExporter.ExportRecordBatch(batch, cArray);
                }
                return 0;
            }
            catch (Exception e)
            {
                return stream?.SetError(e) ?? Errno.EINVAL;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static byte* GetStreamLastError(CArrowArrayStream* cStream)
        {
            try
            {
                return GetExportedStream(cStream)?.LastErrorMessage;
            }
            catch (Exception)
            {
                return null;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void ReleaseStream(CArrowArrayStream* cStream)
        {
            NativeArrayStream* nativeStream = NativeArrayStream.From(cStream);
            try
            {
                if (nativeStream->private_data != null)
                {
                    GCHandle gch = GCHandle.FromIntPtr((IntPtr)nativeStream->private_data);
                    try
                    {
                        ((ExportedStream)gch.Target!).Dispose();
                    }
                    finally
                    {
                        gch.Free();
                        nativeStream->private_data = null;
                    }
                }
            }
            catch (Exception)
            {
                // An exception would end the process, and the consumer has no way to learn of it.
            }
            nativeStream->release = default;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static CAdbcError* GetErrorFromArrayStream(CArrowArrayStream* cStream, AdbcStatusCode* status)
        {
            try
            {
                return GetExportedStream(cStream)?.GetError(status);
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// The state behind an exported <see cref="CArrowArrayStream"/>: the managed stream
        /// and the error from the last call that failed, if any.
        /// </summary>
        private sealed unsafe class ExportedStream : IDisposable
        {
            private IArrowArrayStream? _stream;
            private CAdbcError* _error;
            private AdbcStatusCode _status;

            public ExportedStream(IArrowArrayStream stream)
            {
                _stream = stream;
            }

            public IArrowArrayStream Stream => _stream ?? throw new ObjectDisposedException(nameof(ExportedStream));

            public byte* LastErrorMessage => _error != null && _error->release != default ? _error->message : null;

            public CAdbcError* GetError(AdbcStatusCode* status)
            {
                if (_error == null || _error->release == default)
                {
                    return null;
                }
                if (status != null)
                {
                    *status = _status;
                }
                return _error;
            }

            /// <summary>
            /// Records <paramref name="exception"/> as the stream's error and returns the
            /// errno that corresponds to its status.
            /// </summary>
            public int SetError(Exception exception)
            {
                ClearError();
                _status = CAdbcDriverExporter.SetError(null, exception);
                if (_status == AdbcStatusCode.Success)
                {
                    // An errno of 0 would read as the end of the stream
                    _status = AdbcStatusCode.UnknownError;
                }

                try
                {
                    if (_error == null)
                    {
                        _error = (CAdbcError*)Marshal.AllocHGlobal(sizeof(CAdbcError));
                        *_error = default;
                    }

                    // AdbcErrorFromArrayStream is part of ADBC 1.1.0, so the consumer
                    // understands the 1.1.0 error layout.
                    _error->vendor_code = ErrorVendorCodePrivateData;
                    CAdbcDriverExporter.SetError(_error, exception);
                }
                catch (Exception)
                {
                    // Report the errno without a message.
                }
                return Errno.FromStatus(_status);
            }

            public void ClearError()
            {
                if (_error != null)
                {
                    if (_error->release != default)
                    {
                        ReleaseError(_error);
                    }
                    *_error = default;
                }
            }

            public void Dispose()
            {
                try
                {
                    _stream?.Dispose();
                }
                finally
                {
                    _stream = null;
                    if (_error != null)
                    {
                        ClearError();
                        Marshal.FreeHGlobal((IntPtr)_error);
                        _error = null;
                    }
                }
            }
        }

        /// <summary>
        /// The errno values that an <see cref="CArrowArrayStream"/> reports, which depend on the
        /// C runtime of the platform.
        /// </summary>
        internal static class Errno
        {
            public const int ENOENT = 2;
            public const int EIO = 5;
            public const int EACCES = 13;
            public const int EEXIST = 17;
            public const int EINVAL = 22;

            public static readonly int ENOTSUP;
            public static readonly int ECANCELED;
            public static readonly int ETIMEDOUT;

            static Errno()
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    ENOTSUP = 129;
                    ECANCELED = 105;
                    ETIMEDOUT = 138;
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                {
                    ENOTSUP = 45;
                    ECANCELED = 89;
                    ETIMEDOUT = 60;
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Create("FREEBSD")))
                {
                    ENOTSUP = 45;
                    ECANCELED = 85;
                    ETIMEDOUT = 60;
                }
                else
                {
                    // Linux
                    ENOTSUP = 95;
                    ECANCELED = 125;
                    ETIMEDOUT = 110;
                }
            }

            /// <summary>
            /// Maps a status to an errno the same way the C and Go drivers do.
            /// </summary>
            public static int FromStatus(AdbcStatusCode status) => status switch
            {
                AdbcStatusCode.Success => 0,
                AdbcStatusCode.NotImplemented => ENOTSUP,
                AdbcStatusCode.NotFound => ENOENT,
                AdbcStatusCode.AlreadyExists => EEXIST,
                AdbcStatusCode.InvalidArgument or AdbcStatusCode.InvalidState => EINVAL,
                AdbcStatusCode.Cancelled => ECANCELED,
                AdbcStatusCode.Timeout => ETIMEDOUT,
                AdbcStatusCode.Unauthenticated or AdbcStatusCode.Unauthorized => EACCES,
                _ => EIO,
            };
        }
    }
}

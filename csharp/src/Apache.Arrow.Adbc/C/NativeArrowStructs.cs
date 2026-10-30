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
using Apache.Arrow.C;

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// The layout of <see cref="CArrowArrayStream"/>, whose callbacks Apache.Arrow doesn't
    /// expose. ADBC exports and imports result streams itself so that it can attach an
    /// ADBC error to a stream that fails (see <c>AdbcErrorFromArrayStream</c>).
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct NativeArrayStream
    {
#if NET5_0_OR_GREATER
        public delegate* unmanaged<CArrowArrayStream*, CArrowSchema*, int> get_schema;
        public delegate* unmanaged<CArrowArrayStream*, CArrowArray*, int> get_next;
        public delegate* unmanaged<CArrowArrayStream*, byte*> get_last_error;
        public delegate* unmanaged<CArrowArrayStream*, void> release;
#else
        public IntPtr get_schema;
        public IntPtr get_next;
        public IntPtr get_last_error;
        public IntPtr release;
#endif
        public void* private_data;

        public static NativeArrayStream* From(CArrowArrayStream* stream) => (NativeArrayStream*)stream;
    }

    /// <summary>
    /// The layout of <see cref="CArrowArray"/>, whose release callback Apache.Arrow doesn't expose.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct NativeArray
    {
        public long length;
        public long null_count;
        public long offset;
        public long n_buffers;
        public long n_children;
        public byte** buffers;
        public CArrowArray** children;
        public CArrowArray* dictionary;
        public IntPtr release;
        public void* private_data;

        public static NativeArray* From(CArrowArray* array) => (NativeArray*)array;
    }
}

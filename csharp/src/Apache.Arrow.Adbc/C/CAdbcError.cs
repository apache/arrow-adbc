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

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// A detailed error message for an operation.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CAdbcError
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
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcError*, void> release;
#else
        internal IntPtr release;
#endif

        /// <summary>
        /// Opaque implementation-defined state.
        /// </summary>
        /// <remarks>
        /// This field may not be used unless vendor_code is ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA.
        /// When it is, this field is null iff the error is uninitialized/freed
        ///
        /// Added in ADBC 1.1.0.
        /// </remarks>
        public void* private_data;

        /// <summary>
        /// The associated driver, used by the driver manager to help track state.
        /// </summary>
        /// <remarks>
        /// This field may not be used unless vendor_code is ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA.
        ///
        /// Added in ADBC 1.1.0.
        /// </remarks>
        public CAdbcDriver* private_driver;
    };
}

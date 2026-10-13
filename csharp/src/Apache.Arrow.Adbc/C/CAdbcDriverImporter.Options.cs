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
using System.Text;

#if NET5_0_OR_GREATER
using unsafe GetOptionFn = delegate* unmanaged<void*, byte*, byte*, nint*, Apache.Arrow.Adbc.C.CAdbcError*, Apache.Arrow.Adbc.AdbcStatusCode>;
using unsafe GetOptionDoubleFn = delegate* unmanaged<void*, byte*, double*, Apache.Arrow.Adbc.C.CAdbcError*, Apache.Arrow.Adbc.AdbcStatusCode>;
using unsafe GetOptionIntFn = delegate* unmanaged<void*, byte*, long*, Apache.Arrow.Adbc.C.CAdbcError*, Apache.Arrow.Adbc.AdbcStatusCode>;
using unsafe SetOptionBytesFn = delegate* unmanaged<void*, byte*, byte*, nint, Apache.Arrow.Adbc.C.CAdbcError*, Apache.Arrow.Adbc.AdbcStatusCode>;
using unsafe SetOptionDoubleFn = delegate* unmanaged<void*, byte*, double, Apache.Arrow.Adbc.C.CAdbcError*, Apache.Arrow.Adbc.AdbcStatusCode>;
using unsafe SetOptionIntFn = delegate* unmanaged<void*, byte*, long, Apache.Arrow.Adbc.C.CAdbcError*, Apache.Arrow.Adbc.AdbcStatusCode>;
#else
using GetOptionFn = Apache.Arrow.Adbc.C.HandleGetOption;
using GetOptionDoubleFn = Apache.Arrow.Adbc.C.HandleGetOptionDouble;
using GetOptionIntFn = Apache.Arrow.Adbc.C.HandleGetOptionInt;
using SetOptionBytesFn = Apache.Arrow.Adbc.C.HandleSetOptionBytes;
using SetOptionDoubleFn = Apache.Arrow.Adbc.C.HandleSetOptionDouble;
using SetOptionIntFn = Apache.Arrow.Adbc.C.HandleSetOptionInt;
#endif

namespace Apache.Arrow.Adbc.C
{
    public static partial class CAdbcDriverImporter
    {
        /// <summary>
        /// The ADBC 1.1.0 typed option functions for one kind of handle (database,
        /// connection or statement). The C signatures differ only in the type of the
        /// handle pointer, so they are stored here with the handle typed as void*.
        /// </summary>
        /// <remarks>
        /// On .NET Framework, <see cref="Marshal.GetDelegateForFunctionPointer{TDelegate}(IntPtr)"/>
        /// returns the original delegate when the pointer refers to a managed function (as it does
        /// for an exported driver or the importer's default stubs), so each pointer must be bound
        /// with its exact delegate type and then adapted to the shared shape.
        /// </remarks>
        internal sealed unsafe class OptionFunctions
        {
            public readonly GetOptionFn GetOption;
            public readonly GetOptionFn GetOptionBytes;
            public readonly GetOptionDoubleFn GetOptionDouble;
            public readonly GetOptionIntFn GetOptionInt;
            public readonly SetOptionBytesFn SetOptionBytes;
            public readonly SetOptionDoubleFn SetOptionDouble;
            public readonly SetOptionIntFn SetOptionInt;

            private OptionFunctions(
                GetOptionFn getOption,
                GetOptionFn getOptionBytes,
                GetOptionDoubleFn getOptionDouble,
                GetOptionIntFn getOptionInt,
                SetOptionBytesFn setOptionBytes,
                SetOptionDoubleFn setOptionDouble,
                SetOptionIntFn setOptionInt)
            {
                GetOption = getOption;
                GetOptionBytes = getOptionBytes;
                GetOptionDouble = getOptionDouble;
                GetOptionInt = getOptionInt;
                SetOptionBytes = setOptionBytes;
                SetOptionDouble = setOptionDouble;
                SetOptionInt = setOptionInt;
            }

#if NET5_0_OR_GREATER
            public static OptionFunctions ForDatabase(ref CAdbcDriver driver) => new OptionFunctions(
                (GetOptionFn)driver.DatabaseGetOption,
                (GetOptionFn)driver.DatabaseGetOptionBytes,
                (GetOptionDoubleFn)driver.DatabaseGetOptionDouble,
                (GetOptionIntFn)driver.DatabaseGetOptionInt,
                (SetOptionBytesFn)driver.DatabaseSetOptionBytes,
                (SetOptionDoubleFn)driver.DatabaseSetOptionDouble,
                (SetOptionIntFn)driver.DatabaseSetOptionInt);

            public static OptionFunctions ForConnection(ref CAdbcDriver driver) => new OptionFunctions(
                (GetOptionFn)driver.ConnectionGetOption,
                (GetOptionFn)driver.ConnectionGetOptionBytes,
                (GetOptionDoubleFn)driver.ConnectionGetOptionDouble,
                (GetOptionIntFn)driver.ConnectionGetOptionInt,
                (SetOptionBytesFn)driver.ConnectionSetOptionBytes,
                (SetOptionDoubleFn)driver.ConnectionSetOptionDouble,
                (SetOptionIntFn)driver.ConnectionSetOptionInt);

            public static OptionFunctions ForStatement(ref CAdbcDriver driver) => new OptionFunctions(
                (GetOptionFn)driver.StatementGetOption,
                (GetOptionFn)driver.StatementGetOptionBytes,
                (GetOptionDoubleFn)driver.StatementGetOptionDouble,
                (GetOptionIntFn)driver.StatementGetOptionInt,
                (SetOptionBytesFn)driver.StatementSetOptionBytes,
                (SetOptionDoubleFn)driver.StatementSetOptionDouble,
                (SetOptionIntFn)driver.StatementSetOptionInt);
#else
            public static OptionFunctions ForDatabase(ref CAdbcDriver driver)
            {
                var getOption = Marshal.GetDelegateForFunctionPointer<DatabaseGetOption>(driver.DatabaseGetOption);
                var getOptionBytes = Marshal.GetDelegateForFunctionPointer<DatabaseGetOptionBytes>(driver.DatabaseGetOptionBytes);
                var getOptionDouble = Marshal.GetDelegateForFunctionPointer<DatabaseGetOptionDouble>(driver.DatabaseGetOptionDouble);
                var getOptionInt = Marshal.GetDelegateForFunctionPointer<DatabaseGetOptionInt>(driver.DatabaseGetOptionInt);
                var setOptionBytes = Marshal.GetDelegateForFunctionPointer<DatabaseSetOptionBytes>(driver.DatabaseSetOptionBytes);
                var setOptionDouble = Marshal.GetDelegateForFunctionPointer<DatabaseSetOptionDouble>(driver.DatabaseSetOptionDouble);
                var setOptionInt = Marshal.GetDelegateForFunctionPointer<DatabaseSetOptionInt>(driver.DatabaseSetOptionInt);
                return new OptionFunctions(
                    (h, k, v, l, e) => getOption((CAdbcDatabase*)h, k, v, l, e),
                    (h, k, v, l, e) => getOptionBytes((CAdbcDatabase*)h, k, v, l, e),
                    (h, k, v, e) => getOptionDouble((CAdbcDatabase*)h, k, v, e),
                    (h, k, v, e) => getOptionInt((CAdbcDatabase*)h, k, v, e),
                    (h, k, v, l, e) => setOptionBytes((CAdbcDatabase*)h, k, v, l, e),
                    (h, k, v, e) => setOptionDouble((CAdbcDatabase*)h, k, v, e),
                    (h, k, v, e) => setOptionInt((CAdbcDatabase*)h, k, v, e));
            }

            public static OptionFunctions ForConnection(ref CAdbcDriver driver)
            {
                var getOption = Marshal.GetDelegateForFunctionPointer<ConnectionGetOption>(driver.ConnectionGetOption);
                var getOptionBytes = Marshal.GetDelegateForFunctionPointer<ConnectionGetOptionBytes>(driver.ConnectionGetOptionBytes);
                var getOptionDouble = Marshal.GetDelegateForFunctionPointer<ConnectionGetOptionDouble>(driver.ConnectionGetOptionDouble);
                var getOptionInt = Marshal.GetDelegateForFunctionPointer<ConnectionGetOptionInt>(driver.ConnectionGetOptionInt);
                var setOptionBytes = Marshal.GetDelegateForFunctionPointer<ConnectionSetOptionBytes>(driver.ConnectionSetOptionBytes);
                var setOptionDouble = Marshal.GetDelegateForFunctionPointer<ConnectionSetOptionDouble>(driver.ConnectionSetOptionDouble);
                var setOptionInt = Marshal.GetDelegateForFunctionPointer<ConnectionSetOptionInt>(driver.ConnectionSetOptionInt);
                return new OptionFunctions(
                    (h, k, v, l, e) => getOption((CAdbcConnection*)h, k, v, l, e),
                    (h, k, v, l, e) => getOptionBytes((CAdbcConnection*)h, k, v, l, e),
                    (h, k, v, e) => getOptionDouble((CAdbcConnection*)h, k, v, e),
                    (h, k, v, e) => getOptionInt((CAdbcConnection*)h, k, v, e),
                    (h, k, v, l, e) => setOptionBytes((CAdbcConnection*)h, k, v, l, e),
                    (h, k, v, e) => setOptionDouble((CAdbcConnection*)h, k, v, e),
                    (h, k, v, e) => setOptionInt((CAdbcConnection*)h, k, v, e));
            }

            public static OptionFunctions ForStatement(ref CAdbcDriver driver)
            {
                var getOption = Marshal.GetDelegateForFunctionPointer<StatementGetOption>(driver.StatementGetOption);
                var getOptionBytes = Marshal.GetDelegateForFunctionPointer<StatementGetOptionBytes>(driver.StatementGetOptionBytes);
                var getOptionDouble = Marshal.GetDelegateForFunctionPointer<StatementGetOptionDouble>(driver.StatementGetOptionDouble);
                var getOptionInt = Marshal.GetDelegateForFunctionPointer<StatementGetOptionInt>(driver.StatementGetOptionInt);
                var setOptionBytes = Marshal.GetDelegateForFunctionPointer<StatementSetOptionBytes>(driver.StatementSetOptionBytes);
                var setOptionDouble = Marshal.GetDelegateForFunctionPointer<StatementSetOptionDouble>(driver.StatementSetOptionDouble);
                var setOptionInt = Marshal.GetDelegateForFunctionPointer<StatementSetOptionInt>(driver.StatementSetOptionInt);
                return new OptionFunctions(
                    (h, k, v, l, e) => getOption((CAdbcStatement*)h, k, v, l, e),
                    (h, k, v, l, e) => getOptionBytes((CAdbcStatement*)h, k, v, l, e),
                    (h, k, v, e) => getOptionDouble((CAdbcStatement*)h, k, v, e),
                    (h, k, v, e) => getOptionInt((CAdbcStatement*)h, k, v, e),
                    (h, k, v, l, e) => setOptionBytes((CAdbcStatement*)h, k, v, l, e),
                    (h, k, v, e) => setOptionDouble((CAdbcStatement*)h, k, v, e),
                    (h, k, v, e) => setOptionInt((CAdbcStatement*)h, k, v, e));
            }
#endif
        }

        private static unsafe string GetOptionString(ImportedAdbcDriver driver, GetOptionFn fn, void* handle, string key)
        {
            byte[] value = GetOptionBuffer(driver, fn, handle, key);

            // The reported length includes the null terminator
            int length = value.Length > 0 && value[value.Length - 1] == 0 ? value.Length - 1 : value.Length;
            return Encoding.UTF8.GetString(value, 0, length);
        }

        /// <summary>
        /// Implements the buffer protocol of the AdbcXxxGetOption and AdbcXxxGetOptionBytes
        /// functions, retrying with a larger buffer if the value doesn't fit.
        /// </summary>
        private static unsafe byte[] GetOptionBuffer(ImportedAdbcDriver driver, GetOptionFn fn, void* handle, string key)
        {
            const int InitialLength = 256;

            using (CallHelper caller = new CallHelper(driver))
            using (Utf8Helper utf8Key = new Utf8Helper(key))
            {
                byte[] buffer = new byte[InitialLength];
                while (true)
                {
                    nint length = buffer.Length;
                    fixed (byte* value = buffer)
                    {
                        caller.TranslateCode(fn(handle, utf8Key, value, &length, &caller._error));
                    }

                    if (length <= buffer.Length)
                    {
                        byte[] result = new byte[length];
                        System.Array.Copy(buffer, result, result.Length);
                        return result;
                    }

                    buffer = new byte[checked((int)length)];
                }
            }
        }

        private static unsafe long GetOptionInt64(ImportedAdbcDriver driver, GetOptionIntFn fn, void* handle, string key)
        {
            using (CallHelper caller = new CallHelper(driver))
            using (Utf8Helper utf8Key = new Utf8Helper(key))
            {
                long value = 0;
                caller.TranslateCode(fn(handle, utf8Key, &value, &caller._error));
                return value;
            }
        }

        private static unsafe double GetOptionDouble(ImportedAdbcDriver driver, GetOptionDoubleFn fn, void* handle, string key)
        {
            using (CallHelper caller = new CallHelper(driver))
            using (Utf8Helper utf8Key = new Utf8Helper(key))
            {
                double value = 0;
                caller.TranslateCode(fn(handle, utf8Key, &value, &caller._error));
                return value;
            }
        }

        private static unsafe void SetOptionBytes(ImportedAdbcDriver driver, SetOptionBytesFn fn, void* handle, string key, ReadOnlySpan<byte> value)
        {
            using (CallHelper caller = new CallHelper(driver))
            using (Utf8Helper utf8Key = new Utf8Helper(key))
            {
                fixed (byte* bytes = value)
                {
                    caller.TranslateCode(fn(handle, utf8Key, bytes, value.Length, &caller._error));
                }
            }
        }

        private static unsafe void SetOptionDouble(ImportedAdbcDriver driver, SetOptionDoubleFn fn, void* handle, string key, double value)
        {
            using (CallHelper caller = new CallHelper(driver))
            using (Utf8Helper utf8Key = new Utf8Helper(key))
            {
                caller.TranslateCode(fn(handle, utf8Key, value, &caller._error));
            }
        }

        private static unsafe void SetOptionInt64(ImportedAdbcDriver driver, SetOptionIntFn fn, void* handle, string key, long value)
        {
            using (CallHelper caller = new CallHelper(driver))
            using (Utf8Helper utf8Key = new Utf8Helper(key))
            {
                caller.TranslateCode(fn(handle, utf8Key, value, &caller._error));
            }
        }

        /// <summary>
        /// Reads the ADBC 1.1.0 error details attached to <paramref name="error"/>, if any.
        /// Must be called before the error is released.
        /// </summary>
        private static unsafe IReadOnlyList<KeyValuePair<string, byte[]>>? ReadErrorDetails(ImportedAdbcDriver? driver, CAdbcError* error)
        {
            if (driver == null ||
                driver.DriverVersion < AdbcVersion.Version_1_1_0 ||
                error->vendor_code != CAdbcDriverExporter.ErrorVendorCodePrivateData ||
                error->private_data == null)
            {
                return null;
            }

            ref CAdbcDriver nativeDriver = ref driver.DriverUnsafe;
#if NET5_0_OR_GREATER
            int count = nativeDriver.ErrorGetDetailCount(error);
#else
            int count = Marshal.GetDelegateForFunctionPointer<ErrorGetDetailCount>(nativeDriver.ErrorGetDetailCount)(error);
#endif
            if (count <= 0)
            {
                return null;
            }

            List<KeyValuePair<string, byte[]>> details = new List<KeyValuePair<string, byte[]>>(count);
            for (int i = 0; i < count; i++)
            {
#if NET5_0_OR_GREATER
                CAdbcErrorDetail detail = nativeDriver.ErrorGetDetail(error, i);
#else
                CAdbcErrorDetail detail = Marshal.GetDelegateForFunctionPointer<ErrorGetDetail>(nativeDriver.ErrorGetDetail)(error, i);
#endif
                // Skip malformed details rather than replacing the driver's error with our own
                if (detail.key == null || detail.length < 0 || detail.length > int.MaxValue ||
                    (detail.value == null && detail.length > 0))
                {
                    continue;
                }

                byte[] value = new byte[(int)detail.length];
                if (value.Length > 0)
                {
                    Marshal.Copy((IntPtr)detail.value, value, 0, value.Length);
                }
                details.Add(new KeyValuePair<string, byte[]>(Extensions.MarshalExtensions.PtrToStringUTF8(detail.key)!, value));
            }
            return details;
        }
    }
}

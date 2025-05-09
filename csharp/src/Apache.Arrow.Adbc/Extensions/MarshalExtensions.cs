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
using System.Text;

namespace Apache.Arrow.Adbc.Extensions
{
    public static class MarshalExtensions
    {
#if NETSTANDARD
        public static unsafe string? PtrToStringUTF8(IntPtr intPtr)
        {
            if (intPtr == IntPtr.Zero)
            {
                return null;
            }

            byte* source = (byte*)intPtr;
            int length = 0;

            while (source[length] != 0)
            {
                length++;
            }

            byte[] bytes = new byte[length];
            Marshal.Copy(intPtr, bytes, 0, length);

            return Encoding.UTF8.GetString(bytes);
        }

        public static TDelegate GetDelegateForFunctionPointer<TDelegate>(IntPtr ptr)
        {
            return (TDelegate)(object)Marshal.GetDelegateForFunctionPointer(ptr, typeof(TDelegate));
        }

        public static unsafe IntPtr StringToCoTaskMemUTF8(string? s)
        {
            if (s is null)
            {
                return IntPtr.Zero;
            }

            int nb = Encoding.UTF8.GetMaxByteCount(s.Length);

            IntPtr pMem = Marshal.AllocHGlobal(nb + 1);

            int nbWritten;
            byte* pbMem = (byte*)pMem;

            fixed (char* firstChar = s)
            {
                nbWritten = Encoding.UTF8.GetBytes(firstChar, s.Length, pbMem, nb);
            }

            pbMem[nbWritten] = 0;

            return pMem;
        }
#else
        public static unsafe string? PtrToStringUTF8(IntPtr intPtr)
        {
            return Marshal.PtrToStringUTF8(intPtr);
        }

        public static IntPtr StringToCoTaskMemUTF8(string? s)
        {
            return Marshal.StringToCoTaskMemUTF8(s);
        }
#endif

        public static unsafe string? PtrToStringUTF8(byte* ptr)
        {
            return PtrToStringUTF8((IntPtr)ptr);
        }

        public static unsafe byte[]? MarshalBuffer(void* ptr, int size)
        {
            if (ptr == null)
            {
                return null;
            }

            byte[] bytes = new byte[size];
            fixed (byte* firstByte = bytes)
            {
                Buffer.MemoryCopy(ptr, firstByte, size, size);
            }

            return bytes;
        }
    }
}

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

namespace Apache.Arrow.Adbc.Interop
{
    unsafe class NativeMemoryManager : MemoryManager<byte>
    {
        private readonly byte * _pointer;
        private readonly int _length;

        public NativeMemoryManager(byte * pointer, int length)
        {
            _pointer = pointer;
            _length = length;
        }

        unsafe public override Span<byte> GetSpan()
        {
            return new Span<byte>(_pointer, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(_pointer + elementIndex);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}

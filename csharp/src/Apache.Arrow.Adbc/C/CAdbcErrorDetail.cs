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

using System.Runtime.InteropServices;

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// Extra key-value metadata for an error.
    ///
    /// Added in ADBC 1.1.0.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CAdbcErrorDetail
    {
        /// <summary>
        /// The metadata key.
        /// </summary>
        public byte* key;

        /// <summary>
        /// The metadata value.
        /// </summary>
        public byte* value;

        /// <summary>
        /// The metadata value length.
        /// </summary>
        public nint length;
    }
}

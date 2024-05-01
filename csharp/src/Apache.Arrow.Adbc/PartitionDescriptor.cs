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

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// A descriptor for a part of a potentially distributed or
    /// partitioned result set.
    /// </summary>
    public struct PartitionDescriptor : IEquatable<PartitionDescriptor>
    {
        readonly byte[] _descriptor;

        public PartitionDescriptor(byte[] descriptor)
        {
            _descriptor = descriptor;
        }

        public ReadOnlySpan<byte> Descriptor => _descriptor;

        public override bool Equals(object? obj)
        {
            PartitionDescriptor? other = obj as PartitionDescriptor?;
            return other != null && Equals(other.Value);
        }

        public bool Equals(PartitionDescriptor other)
        {
            if (_descriptor.Length != other._descriptor.Length)
            {
                return false;
            }
            for (int i = 0; i < _descriptor.Length; i++)
            {
                if (_descriptor[i] != other._descriptor[i])
                {
                    return false;
                }
            }
            return true;
        }

        public override int GetHashCode()
        {
            return _descriptor.GetHashCode();
        }
    }
}

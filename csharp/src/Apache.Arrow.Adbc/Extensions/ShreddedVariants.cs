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
using Apache.Arrow.Scalars.Variant;

namespace Apache.Arrow.Adbc.Extensions
{
    /// <summary>
    /// Enables reading shredded variant columns.
    /// </summary>
    /// <remarks>
    /// Unshredded variants are read without any setup. Reading a shredded variant requires
    /// the shredding support in the Apache.Arrow.Operations package, which this library does
    /// not reference. Applications that expect shredded results can reference that package
    /// and enable it once at startup:
    /// <code>
    /// ShreddedVariants.Reader = (array, index) => array.GetLogicalVariantValue(index);
    /// </code>
    /// </remarks>
    public static class ShreddedVariants
    {
        /// <summary>
        /// Gets or sets the function that reads the logical value at an index of a shredded
        /// <see cref="VariantArray"/>. When null, reading a shredded variant throws
        /// <see cref="NotSupportedException"/>.
        /// </summary>
        public static Func<VariantArray, int, VariantValue>? Reader { get; set; }

        internal static VariantValue Read(VariantArray array, int index)
        {
            Func<VariantArray, int, VariantValue> reader = Reader ?? throw new NotSupportedException(
                "Reading a shredded variant requires the Apache.Arrow.Operations package. Reference it and set " +
                "ShreddedVariants.Reader = (array, index) => array.GetLogicalVariantValue(index).");
            return reader(array, index);
        }
    }
}

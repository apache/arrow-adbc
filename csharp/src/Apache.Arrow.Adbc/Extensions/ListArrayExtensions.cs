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
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Extensions
{
    public static class ListArrayExtensions
    {
        /// <summary>
        /// Builds a <see cref="ListArray"/> from a list of <see cref="IArrowArray"/> data for the given datatype <see cref="IArrowArray"/>.
        /// It concatenates the contained data into a single ListArray.
        /// </summary>
        /// <param name="list">The list of data to build from.</param>
        /// <param name="dataType">The data type of the contained data.</param>
        /// <returns>A <see cref="ListArray"/> of the data.</returns>
        public static ListArray BuildListArrayForType(this IReadOnlyList<IArrowArray?> list, IArrowType dataType)
        {
            ArrowBuffer.Builder<int> valueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
            ArrowBuffer.BitmapBuilder validityBufferBuilder = new ArrowBuffer.BitmapBuilder();
            List<ArrayData> arrayDataList = new List<ArrayData>(list.Count);
            int length = 0;
            int nullCount = 0;

            foreach (IArrowArray? array in list)
            {
                if (array == null)
                {
                    valueOffsetsBufferBuilder.Append(length);
                    validityBufferBuilder.Append(false);
                    nullCount++;
                }
                else
                {
                    valueOffsetsBufferBuilder.Append(length);
                    validityBufferBuilder.Append(true);
                    arrayDataList.Add(array.Data);
                    length += array.Length;
                }
            }

            ArrowBuffer validityBuffer = nullCount > 0
                ? validityBufferBuilder.Build() : ArrowBuffer.Empty;

            ArrayData? data = ArrayDataConcatenator.Concatenate(arrayDataList);

            if (data == null)
            {
                EmptyArrayCreationVisitor visitor = new EmptyArrayCreationVisitor();
                dataType.Accept(visitor);
                data = visitor.Result;
            }

            IArrowArray value = ArrowArrayFactory.BuildArray(data);

            valueOffsetsBufferBuilder.Append(length);

            return new ListArray(new ListType(dataType), list.Count,
                    valueOffsetsBufferBuilder.Build(), value,
                    validityBuffer, nullCount, 0);
        }

        private class EmptyArrayCreationVisitor :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<FixedWidthType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<MapType>
        {
            public ArrayData? Result { get; private set; }

            public void Visit(BooleanType type)
            {
                Result = new BooleanArray.Builder().Build().Data;
            }

            public void Visit(FixedWidthType type)
            {
                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            public void Visit(BinaryType type)
            {
                Result = new BinaryArray.Builder().Build().Data;
            }

            public void Visit(StringType type)
            {
                Result = new StringArray.Builder().Build().Data;
            }

            public void Visit(ListType type)
            {
                type.ValueDataType.Accept(this);
                ArrayData? child = Result;

                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty, MakeInt0Buffer() }, new[] { child });
            }

            public void Visit(FixedSizeListType type)
            {
                type.ValueDataType.Accept(this);
                ArrayData? child = Result;

                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty }, new[] { child });
            }

            public void Visit(StructType type)
            {
                ArrayData?[] children = new ArrayData[type.Fields.Count];
                for (int i = 0; i < type.Fields.Count; i++)
                {
                    type.Fields[i].DataType.Accept(this);
                    children[i] = Result;
                }

                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty }, children);
            }

            public void Visit(MapType type)
            {
                Result = new MapArray.Builder(type).Build().Data;
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"EmptyArrayCreationVisitor for {type.Name} is not supported yet.");
            }

            private static ArrowBuffer MakeInt0Buffer()
            {
                ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>();
                builder.Append(0);
                return builder.Build();
            }
        }
    }
}

using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using System;
using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    static class ArrayDataConcatenator
    {
        public static ArrayData Concatenate(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
        {
            if (arrayDataList == null || arrayDataList.Count == 0)
            {
                return new ArrayData(NullType.Default, 0, 0, 0, System.Array.Empty<ArrowBuffer>(), null);
            }

            if (arrayDataList.Count == 1)
            {
                return arrayDataList[0];
            }

            var arrowArrayConcatenationVisitor = new ArrayDataConcatenationVisitor(arrayDataList, allocator);

            IArrowType type = arrayDataList[0].DataType;
            type.Accept(arrowArrayConcatenationVisitor);

            return arrowArrayConcatenationVisitor.Result;
        }

        private class ArrayDataConcatenationVisitor :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<FixedWidthType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>
        {
            public ArrayData Result { get; private set; }
            private readonly IReadOnlyList<ArrayData> _arrayDataList;
            private readonly int _totalLength;
            private readonly int _totalNullCount;
            private readonly MemoryAllocator _allocator;

            public ArrayDataConcatenationVisitor(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
            {
                _arrayDataList = arrayDataList;
                _allocator = allocator;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    _totalLength += arrayData.Length;
                    _totalNullCount += arrayData.NullCount;
                }
            }

            public void Visit(BooleanType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer valueBuffer = ConcatenateBitmapBuffer(1);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, valueBuffer });
            }

            public void Visit(FixedWidthType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer valueBuffer = ConcatenateFixedWidthTypeValueBuffer(type);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, valueBuffer });
            }

            public void Visit(BinaryType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(StringType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(ListType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateOffsetBuffer();
                ArrayData child = Concatenate(SelectChildren(0), _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer }, new[] { child });
            }

            // feeling confusing with original code, see if having problem
            public void Visit(StructType type)
            {
                CheckData(type, 1);
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    children.Add(Concatenate(SelectChildren(i), _allocator));
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer }, children);
            }

            public void Visit(UnionType type)
            {
                int bufferCount = type.Mode switch
                {
                    UnionMode.Sparse => 1,
                    UnionMode.Dense => 2,
                    _ => throw new InvalidOperationException("TODO"),
                };

                CheckData(type, bufferCount);
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    children.Add(Concatenate(SelectChildren(i), _allocator));
                }

                ArrowBuffer[] buffers = new ArrowBuffer[bufferCount];
                buffers[0] = ConcatenateUnionTypeBuffer();
                if (bufferCount > 1)
                {
                    buffers[1] = ConcatenateUnionOffsetBuffer();
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, buffers, children);
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"Concatenation for {type.Name} is not supported yet.");
            }

            private void CheckData(IArrowType type, int expectedBufferCount)
            {
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    EnsureDataType(arrayData, type.TypeId);
                    EnsureBufferCount(arrayData, expectedBufferCount);
                }
            }

            private void ConcatenateVariableBinaryArrayData(IArrowType type)
            {
                CheckData(type, 3);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateOffsetBuffer();
                ArrowBuffer valueBuffer = ConcatenateVariableBinaryValueBuffer();

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, valueBuffer });
            }

            private ArrowBuffer ConcatenateValidityBuffer()
            {
                if (_totalNullCount == 0)
                {
                    return ArrowBuffer.Empty;
                }

                return ConcatenateBitmapBuffer(0);
            }

            private ArrowBuffer ConcatenateBitmapBuffer(int bufferIndex)
            {
                var builder = new ArrowBuffer.BitmapBuilder(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    ReadOnlySpan<byte> span = arrayData.Buffers[bufferIndex].Span;

                    builder.Append(span, length);
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateFixedWidthTypeValueBuffer(FixedWidthType type)
            {
                int typeByteWidth = type.BitWidth / 8;
                var builder = new ArrowBuffer.Builder<byte>(_totalLength * typeByteWidth);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    int byteLength = length * typeByteWidth;

                    builder.Append(arrayData.Buffers[1].Span.Slice(0, byteLength));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateVariableBinaryValueBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>();

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int lastOffset = arrayData.Buffers[1].Span.CastTo<int>()[arrayData.Length];
                    builder.Append(arrayData.Buffers[2].Span.Slice(0, lastOffset));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength + 1);
                int baseOffset = 0;

                builder.Append(0);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    // The first offset is always 0.
                    // It should be skipped because it duplicate to the last offset of builder.
                    ReadOnlySpan<int> span = arrayData.Buffers[1].Span.CastTo<int>().Slice(1, arrayData.Length);

                    foreach (int offset in span)
                    {
                        builder.Append(baseOffset + offset);
                    }

                    // The next offset must start from the current last offset.
                    baseOffset += span[arrayData.Length - 1];
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateUnionTypeBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    builder.Append(arrayData.Buffers[0]);
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateUnionOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength);
                int baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    ReadOnlySpan<int> span = arrayData.Buffers[1].Span.CastTo<int>();
                    foreach (int offset in span)
                    {
                        builder.Append(baseOffset + offset);
                    }

                    // The next offset must start from the current last offset.
                    baseOffset += span[arrayData.Length];
                }

                return builder.Build(_allocator);
            }

            private List<ArrayData> SelectChildren(int index)
            {
                var children = new List<ArrayData>(_arrayDataList.Count);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    children.Add(arrayData.Children[index]);
                }

                return children;
            }

            private void EnsureBufferCount(ArrayData data, int count)
            {
                if (data.Buffers.Length != count)
                {
                    // TODO: Use localizable string resource
                    throw new ArgumentException(
                        $"Buffer count <{data.Buffers.Length}> must be at least <{count}>",
                        nameof(data.Buffers.Length));
                }
            }

            private void EnsureDataType(ArrayData data, ArrowTypeId id)
            {
                if (data.DataType.TypeId != id)
                {
                    // TODO: Use localizable string resource
                    throw new ArgumentException(
                        $"Specified array type <{data.DataType.TypeId}> does not match expected type(s) <{id}>",
                        nameof(data.DataType.TypeId));
                }
            }
        }
    }
}

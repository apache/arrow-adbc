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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    internal class SchemaParser
    {
        internal static Schema GetArrowSchema(TTableSchema thriftSchema, TProtocolVersion protocolVersion)
        {
            Field[] fields = new Field[thriftSchema.Columns.Count];
            for (int i = 0; i < thriftSchema.Columns.Count; i++)
            {
                TColumnDesc column = thriftSchema.Columns[i];
                // Note: no nullable metadata is returned from the Thrift interface.
                fields[i] = new Field(column.ColumnName, GetArrowType(column.TypeDesc.Types[0], protocolVersion), nullable: true /* assumed */);
            }
            return new Schema(fields, null);
        }

        static IArrowType GetArrowType(TTypeEntry thriftType, TProtocolVersion protocolVersion)
        {
            if (thriftType.PrimitiveEntry != null)
            {
                return GetArrowType(thriftType.PrimitiveEntry, protocolVersion);
            }
            throw new InvalidOperationException();
        }

        public static IArrowType GetArrowType(TPrimitiveTypeEntry thriftType, TProtocolVersion protocolVersion)
        {
            switch (thriftType.Type)
            {
                case TTypeId.BIGINT_TYPE: return Int64Type.Default;
                case TTypeId.BINARY_TYPE: return BinaryType.Default;
                case TTypeId.BOOLEAN_TYPE: return BooleanType.Default;
                case TTypeId.CHAR_TYPE: return StringType.Default;
                case TTypeId.DATE_TYPE: return GetDateType(protocolVersion);
                case TTypeId.DOUBLE_TYPE: return DoubleType.Default;
                case TTypeId.FLOAT_TYPE: return GetFloatType(protocolVersion);
                case TTypeId.INT_TYPE: return Int32Type.Default;
                case TTypeId.NULL_TYPE: return GetNullType(protocolVersion);
                case TTypeId.SMALLINT_TYPE: return Int16Type.Default;
                case TTypeId.STRING_TYPE: return StringType.Default;
                case TTypeId.TIMESTAMP_TYPE: return GetTimestampType(protocolVersion);
                case TTypeId.TINYINT_TYPE: return Int8Type.Default;
                case TTypeId.VARCHAR_TYPE: return StringType.Default;
                case TTypeId.DECIMAL_TYPE: return GetDecimal128Type(thriftType, protocolVersion);
                case TTypeId.INTERVAL_DAY_TIME_TYPE:
                case TTypeId.INTERVAL_YEAR_MONTH_TYPE:
                case TTypeId.ARRAY_TYPE:
                case TTypeId.MAP_TYPE:
                case TTypeId.STRUCT_TYPE:
                case TTypeId.UNION_TYPE:
                case TTypeId.USER_DEFINED_TYPE:
                    return StringType.Default;
                default:
                    throw new NotImplementedException();
            }
        }

        private static IArrowType GetNullType(TProtocolVersion protocolVersion) =>
            HiveServer2Connection.GetIsHiveServer2Protocol(protocolVersion) ? StringType.Default : NullType.Default;

        private static IArrowType GetDateType(TProtocolVersion protocolVersion) =>
            HiveServer2Connection.GetIsHiveServer2Protocol(protocolVersion) ? StringType.Default : Date32Type.Default;

        private static IArrowType GetFloatType(TProtocolVersion protocolVersion) =>
            HiveServer2Connection.GetIsHiveServer2Protocol(protocolVersion) ? DoubleType.Default : FloatType.Default;

        private static IArrowType GetTimestampType(TProtocolVersion protocolVersion) =>
            HiveServer2Connection.GetIsHiveServer2Protocol(protocolVersion) ? StringType.Default : new TimestampType(TimeUnit.Microsecond, (string?)null);

        private static IArrowType GetDecimal128Type(TPrimitiveTypeEntry thriftType, TProtocolVersion protocolVersion)
        {
            if (HiveServer2Connection.GetIsSparkProtocol(protocolVersion))
            {
                int precision = thriftType.TypeQualifiers.Qualifiers["precision"].I32Value;
                int scale = thriftType.TypeQualifiers.Qualifiers["scale"].I32Value;
                return new Decimal128Type(precision, scale);
            }
            return StringType.Default;
        }
    }
}

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
using System.IO;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    internal class DatabricksSchemaParser : SchemaParser
    {
        /// <summary>
        /// Parses an Arrow schema from serialized schema bytes
        /// </summary>
        /// <param name="schemaBytes">The serialized Arrow schema bytes</param>
        /// <returns>The parsed Arrow Schema</returns>
        public Schema? ParseArrowSchema(byte[] schemaBytes)
        {
            if (schemaBytes == null || schemaBytes.Length == 0)
                return null;

            try
            {
                using var stream = new MemoryStream(schemaBytes);
                using var reader = new ArrowStreamReader(stream);
                return reader.Schema;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("Failed to parse Arrow schema", ex);
            }
        }

        public override IArrowType GetArrowType(TPrimitiveTypeEntry thriftType, DataTypeConversion dataTypeConversion)
        {
            return thriftType.Type switch
            {
                TTypeId.BIGINT_TYPE => Int64Type.Default,
                TTypeId.BINARY_TYPE => BinaryType.Default,
                TTypeId.BOOLEAN_TYPE => BooleanType.Default,
                TTypeId.DATE_TYPE => Date32Type.Default,
                TTypeId.DOUBLE_TYPE => DoubleType.Default,
                TTypeId.FLOAT_TYPE => FloatType.Default,
                TTypeId.INT_TYPE => Int32Type.Default,
                TTypeId.NULL_TYPE => NullType.Default,
                TTypeId.SMALLINT_TYPE => Int16Type.Default,
                TTypeId.TIMESTAMP_TYPE => new TimestampType(TimeUnit.Microsecond, (string?)null),
                // TIMESTAMP_TYPE could be a string if SQLConf.get.arrowThriftTimestampToString is configured differently
                TTypeId.TINYINT_TYPE => Int8Type.Default,
                TTypeId.DECIMAL_TYPE
                or TTypeId.CHAR_TYPE
                or TTypeId.STRING_TYPE
                or TTypeId.VARCHAR_TYPE
                or TTypeId.INTERVAL_DAY_TIME_TYPE
                or TTypeId.INTERVAL_YEAR_MONTH_TYPE
                or TTypeId.ARRAY_TYPE
                or TTypeId.MAP_TYPE
                or TTypeId.STRUCT_TYPE
                or TTypeId.UNION_TYPE
                or TTypeId.USER_DEFINED_TYPE => StringType.Default,
                TTypeId.TIMESTAMPLOCALTZ_TYPE => throw new NotImplementedException(),
                _ => throw new NotImplementedException(),
            };
        }
    }
}

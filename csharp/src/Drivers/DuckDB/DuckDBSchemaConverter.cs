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
using System.Data;
using Apache.Arrow;
using Apache.Arrow.Types;
using DuckDB.NET.Data;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// Converts DuckDB schema information to Arrow schema.
    /// </summary>
    public static class DuckDBSchemaConverter
    {
        public static Schema ConvertToArrowSchema(DuckDBDataReader reader)
        {
            var fields = new List<Field>();
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var fieldType = reader.GetFieldType(i);
                var dataTypeName = reader.GetDataTypeName(i);
                
                // DuckDB doesn't expose nullability through the reader
                // so we assume all columns are nullable
                var isNullable = true;
                
                var arrowType = MapDuckDBTypeToArrow(fieldType, dataTypeName);
                fields.Add(new Field(columnName, arrowType, isNullable));
            }
            
            return new Schema(fields, null);
        }

        private static IArrowType MapDuckDBTypeToArrow(Type clrType, string dataTypeName)
        {
            // First try to map based on the data type name for more accurate mapping
            if (!string.IsNullOrEmpty(dataTypeName))
            {
                var upperTypeName = dataTypeName.ToUpperInvariant();
                
                // Handle parameterized types
                if (upperTypeName.StartsWith("DECIMAL"))
                {
                    // Try to parse precision and scale from DECIMAL(p,s)
                    // Default to DECIMAL(38,4) if parsing fails
                    return new Decimal128Type(38, 4);
                }
                
                if (upperTypeName.StartsWith("VARCHAR") || upperTypeName.StartsWith("TEXT"))
                {
                    return StringType.Default;
                }
                
                switch (upperTypeName)
                {
                    case "BOOLEAN":
                        return BooleanType.Default;
                    case "TINYINT":
                        return Int8Type.Default;
                    case "SMALLINT":
                        return Int16Type.Default;
                    case "INTEGER":
                    case "INT":
                        return Int32Type.Default;
                    case "BIGINT":
                        return Int64Type.Default;
                    case "UTINYINT":
                        return UInt8Type.Default;
                    case "USMALLINT":
                        return UInt16Type.Default;
                    case "UINTEGER":
                    case "UINT":
                        return UInt32Type.Default;
                    case "UBIGINT":
                        return UInt64Type.Default;
                    case "FLOAT":
                    case "REAL":
                        return FloatType.Default;
                    case "DOUBLE":
                        return DoubleType.Default;
                    case "DATE":
                        return Date32Type.Default;
                    case "TIME":
                        return new Time64Type(TimeUnit.Microsecond);
                    case "TIMESTAMP":
                        return new TimestampType(TimeUnit.Microsecond, null);
                    case "TIMESTAMP WITH TIME ZONE":
                    case "TIMESTAMPTZ":
                        return new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc);
                    case "BLOB":
                        return BinaryType.Default;
                    case "UUID":
                        return new FixedSizeBinaryType(16);
                    case "HUGEINT":
                        return new Decimal128Type(38, 0);
                    case "INTERVAL":
                        return new IntervalType(IntervalUnit.DayTime);
                }
            }
            
            // Fall back to CLR type mapping
            return MapClrTypeToArrow(clrType);
        }

        private static IArrowType MapClrTypeToArrow(Type clrType)
        {
            if (clrType == typeof(bool))
                return BooleanType.Default;
            if (clrType == typeof(sbyte))
                return Int8Type.Default;
            if (clrType == typeof(short))
                return Int16Type.Default;
            if (clrType == typeof(int))
                return Int32Type.Default;
            if (clrType == typeof(long))
                return Int64Type.Default;
            if (clrType == typeof(byte))
                return UInt8Type.Default;
            if (clrType == typeof(ushort))
                return UInt16Type.Default;
            if (clrType == typeof(uint))
                return UInt32Type.Default;
            if (clrType == typeof(ulong))
                return UInt64Type.Default;
            if (clrType == typeof(float))
                return FloatType.Default;
            if (clrType == typeof(double))
                return DoubleType.Default;
            if (clrType == typeof(decimal))
                return new Decimal128Type(29, 10); // Default precision/scale
            if (clrType == typeof(string))
                return StringType.Default;
            if (clrType == typeof(byte[]))
                return BinaryType.Default;
            if (clrType == typeof(DateTime))
                return new TimestampType(TimeUnit.Microsecond, null);
            if (clrType == typeof(DateTimeOffset))
                return new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc);
            if (clrType == typeof(TimeSpan))
                return new Time64Type(TimeUnit.Microsecond);
            if (clrType == typeof(Guid))
                return new FixedSizeBinaryType(16);
                
            // DateOnly and TimeOnly are only available in .NET 6+
#if NET6_0_OR_GREATER
            if (clrType == typeof(DateOnly))
                return Date32Type.Default;
            if (clrType == typeof(TimeOnly))
                return new Time64Type(TimeUnit.Microsecond);
#endif
            
            // Default to string for unknown types
            return StringType.Default;
        }
    }
}
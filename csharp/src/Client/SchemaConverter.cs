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
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Client
{
    internal class SchemaConverter
    {
        /// <summary>
        /// Converts an Arrow <see cref="Schema"/> to a
        /// <see cref="DataTable"/> schema.
        /// </summary>
        /// <param name="schema">The Arrow schema</param>
        /// <param name="adbcStatement">The AdbcStatement to use</param>
        /// <exception cref="ArgumentNullException"></exception>
        public static DataTable ConvertArrowSchema(Schema schema, AdbcStatement adbcStatement, DecimalBehavior decimalBehavior)
        {
            if (schema == null)
                throw new ArgumentNullException(nameof(schema));

            if (adbcStatement == null)
                throw new ArgumentNullException(nameof(adbcStatement));

            DataTable table = new DataTable();

            table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            table.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            table.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
            table.Columns.Add(SchemaTableColumn.ProviderType, typeof(IArrowType));
            table.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(int));
            table.Columns.Add(SchemaTableColumn.NumericScale, typeof(int));

            int columnOrdinal = 0;

            foreach (Field f in schema.FieldsList)
            {
                DataRow row = table.NewRow();

                row[SchemaTableColumn.ColumnName] = f.Name;
                row[SchemaTableColumn.ColumnOrdinal] = columnOrdinal;
                row[SchemaTableColumn.AllowDBNull] = f.IsNullable;
                row[SchemaTableColumn.ProviderType] = f.DataType;
                Type t = ConvertArrowType(f, decimalBehavior);

                row[SchemaTableColumn.DataType] = t;

                if
                (
                    (t == typeof(decimal) || t == typeof(double) || t == typeof(float)) &&
                    f.HasMetadata
                )
                {
                    if (f.Metadata.TryGetValue("precision", out string? precisionValue))
                    {
                        if (!string.IsNullOrEmpty(precisionValue))
                            row[SchemaTableColumn.NumericPrecision] = Convert.ToInt32(precisionValue);
                    }

                    if (f.Metadata.TryGetValue("scale", out string? scaleValue))
                    {
                        if (!string.IsNullOrEmpty(scaleValue))
                            row[SchemaTableColumn.NumericScale] = Convert.ToInt32(scaleValue);
                    }
                }
                else if (f.DataType is Decimal128Type decimal128Type)
                {
                    row[SchemaTableColumn.NumericPrecision] = decimal128Type.Precision;
                    row[SchemaTableColumn.NumericScale] = decimal128Type.Scale;
                }
                else if (f.DataType is Decimal256Type decimal256Type)
                {
                    row[SchemaTableColumn.NumericPrecision] = decimal256Type.Precision;
                    row[SchemaTableColumn.NumericScale] = decimal256Type.Scale;
                }
                else
                {
                    row[SchemaTableColumn.NumericPrecision] = DBNull.Value;
                    row[SchemaTableColumn.NumericScale] = DBNull.Value;
                }

                table.Rows.Add(row);
                columnOrdinal++;
            }

            return table;
        }

        /// <summary>
        /// Convert types
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public static Type ConvertArrowType(Field f, DecimalBehavior decimalBehavior)
        {
            switch (f.DataType.TypeId)
            {
                case ArrowTypeId.List:
                    ListType list = (ListType)f.DataType;
                    IArrowType valueType = list.ValueDataType;
                    return GetArrowArrayType(valueType);
                default:
                    return GetArrowType(f, decimalBehavior);
            }
        }

        public static Type GetArrowType(Field f, DecimalBehavior decimalBehavior)
        {
            switch (f.DataType.TypeId)
            {
                case ArrowTypeId.Binary:
                    return typeof(byte[]);

                case ArrowTypeId.Boolean:
                    return typeof(bool);

                case ArrowTypeId.Decimal128:
                    if (decimalBehavior == DecimalBehavior.UseSqlDecimal)
                        return typeof(SqlDecimal);
                    else
                        return typeof(decimal);

                case ArrowTypeId.Decimal256:
                    return typeof(string);

#if NET6_0_OR_GREATER
                case ArrowTypeId.Time32:
                case ArrowTypeId.Time64:
                    return typeof(TimeOnly);
#else
                case ArrowTypeId.Time32:
                case ArrowTypeId.Time64:
                    return typeof(TimeSpan);
#endif

                case ArrowTypeId.Date32:
                case ArrowTypeId.Date64:
                    return typeof(DateTime);

                case ArrowTypeId.Double:
                    return typeof(double);

#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    return typeof(Half);
#endif
                case ArrowTypeId.Float:
                    return typeof(float);

                case ArrowTypeId.Int8:
                    return typeof(sbyte);

                case ArrowTypeId.Int16:
                    return typeof(short);

                case ArrowTypeId.Int32:
                    return typeof(int);

                case ArrowTypeId.Int64:
                    return typeof(long);

                case ArrowTypeId.String:
                    return typeof(string);

                case ArrowTypeId.Struct:
                    goto default;

                case ArrowTypeId.Timestamp:
                    return typeof(DateTimeOffset);

                case ArrowTypeId.Null:
                    return typeof(DBNull);

                default:
                    return f.DataType.GetType();
            }
        }

        public static Type GetArrowArrayType(IArrowType dataType)
        {
            switch (dataType.TypeId)
            {
                case ArrowTypeId.Binary:
                    return typeof(BinaryArray);
                case ArrowTypeId.Boolean:
                    return typeof(BooleanArray);
                case ArrowTypeId.Decimal128:
                        return typeof(Decimal128Array);
                case ArrowTypeId.Decimal256:
                    return typeof(Decimal256Array);
                case ArrowTypeId.Time32:
                    return typeof(Time32Array);
                case ArrowTypeId.Time64:
                    return typeof(Time64Array);
                case ArrowTypeId.Date32:
                    return typeof(Date32Array);
                case ArrowTypeId.Date64:
                    return typeof(Date64Array);
                case ArrowTypeId.Double:
                    return typeof(DoubleArray);

#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    return typeof(HalfFloatArray);
#endif
                case ArrowTypeId.Float:
                    return typeof(FloatArray);
                case ArrowTypeId.Int8:
                    return typeof(Int8Array);
                case ArrowTypeId.Int16:
                    return typeof(Int16Array);
                case ArrowTypeId.Int32:
                    return typeof(Int32Array);
                case ArrowTypeId.Int64:
                    return typeof(Int64Array);
                case ArrowTypeId.String:
                    return typeof(StringArray);
                case ArrowTypeId.Struct:
                    return typeof(StructArray);
                case ArrowTypeId.Timestamp:
                    return typeof(TimestampArray);
                case ArrowTypeId.Null:
                    return typeof(NullArray);
                case ArrowTypeId.List:
                    return typeof(ListArray);
            }

            throw new InvalidCastException($"Cannot determine the array type for {dataType.Name}");
        }
    }
}

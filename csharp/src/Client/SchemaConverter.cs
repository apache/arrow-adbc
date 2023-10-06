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
        public static DataTable ConvertArrowSchema(Schema schema, AdbcStatement adbcStatement)
        {
            if(schema == null)
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
                Type t = ConvertArrowType(f);

                row[SchemaTableColumn.DataType] = t;

                if
                (
                    (t == typeof(decimal) || t == typeof(double) || t == typeof(float)) &&
                    f.HasMetadata
                )
                {
                    row[SchemaTableColumn.NumericPrecision] = Convert.ToInt32(f.Metadata["precision"]);
                    row[SchemaTableColumn.NumericScale] = Convert.ToInt32(f.Metadata["scale"]);
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
        /// Convert types for Snowflake only
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public static Type ConvertArrowType(Field f)
        {
            switch (f.DataType.TypeId)
            {
                case ArrowTypeId.Binary:
                    return typeof(byte[]);

                case ArrowTypeId.Boolean:
                    return typeof(bool);

                case ArrowTypeId.Decimal128:
                case ArrowTypeId.Decimal256:
                    return typeof(decimal);

                case ArrowTypeId.Time32:
                case ArrowTypeId.Time64:
                    return typeof(long);

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
                    return null;

                default:
                    return f.DataType.GetType();
            }
        }
    }
}

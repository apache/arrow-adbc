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
using System.Diagnostics;
using Apache.Arrow.Types;
using ParquetSharp;

namespace Apache.Arrow.Adbc.FlightSql.Tests
{
    internal class ParquetParser
    {
        /// <summary>
        /// Parses a parquet file and returns a RecordBatch for each RowGroup.
        /// </summary>
        /// <param name="path">The path of the parquet file.</param>
        public static List<RecordBatch> ParseParquetFile(string path)
        {
            List<RecordBatch> recordBatches = new List<RecordBatch>();

            List<IArrowArray> records = new List<IArrowArray>();

            using (ParquetFileReader file = new ParquetFileReader(path))
            {
                Arrow.Schema arrowSchema = ConvertParquetSchemaDescripterToArrowSchema(file.FileMetaData.Schema);

                for (int rowGroup = 0; rowGroup < file.FileMetaData.NumRowGroups; rowGroup++)
                {
                    using (RowGroupReader rowGroupReader = file.RowGroup(rowGroup))
                    {
                        int groupNumRows = checked((int)rowGroupReader.MetaData.NumRows);

                        for (int i = 0; i < rowGroupReader.MetaData.NumColumns; i++)
                        {
                            ColumnReader columnReader = rowGroupReader.Column(i);
                            LogicalType columnLogicalType = columnReader.ColumnDescriptor.LogicalType;

                            if (columnReader.ColumnDescriptor.PhysicalType.Equals(PhysicalType.Double))
                            {
                                var values = columnReader.LogicalReader<double>().ReadAll(groupNumRows);

                                var builder = new DoubleArray.Builder();
                                builder.AppendRange(values);
                                records.Add(builder.Build());
                            }
                            else if (columnLogicalType.Equals(LogicalType.Int(8, true)))
                            {
                                var values = columnReader.LogicalReader<sbyte>().ReadAll(groupNumRows);

                                var builder = new Int8Array.Builder();
                                builder.AppendRange(values);
                                records.Add(builder.Build());
                            }
                            else if (columnLogicalType.Equals(LogicalType.Int(16, true)))
                            {
                                var values = columnReader.LogicalReader<short>().ReadAll(groupNumRows);

                                var builder = new Int16Array.Builder();
                                builder.AppendRange(values);
                                records.Add(builder.Build());
                            }
                            else if (columnLogicalType.Equals(LogicalType.Int(32, true)))
                            {
                                var values = columnReader.LogicalReader<int>().ReadAll(groupNumRows);

                                var builder = new Int32Array.Builder();
                                builder.AppendRange(values);
                                records.Add(builder.Build());
                            }
                            else if (columnLogicalType.Equals(LogicalType.Int(64, true)))
                            {
                                var values = columnReader.LogicalReader<long>().ReadAll(groupNumRows);

                                var builder = new Int64Array.Builder();
                                builder.AppendRange(values);
                                records.Add(builder.Build());
                            }
                            else if (columnLogicalType.Equals(LogicalType.String()))
                            {
                                try
                                {
                                    var values = columnReader.LogicalReader<string>().ReadAll(groupNumRows);

                                    var builder = new StringArray.Builder();
                                    builder.AppendRange(values);
                                    records.Add(builder.Build());
                                }
                                catch (Exception ex)
                                {
                                    Debug.WriteLine(ex.ToString());
                                }
                            }
                            else if (columnLogicalType.Equals(LogicalType.Timestamp(true, ParquetSharp.TimeUnit.Millis)))
                            {
                                var values = columnReader.LogicalReader<DateTime>().ReadAll(groupNumRows);

                                var builder = new Date64Array.Builder();
                                builder.AppendRange(values);
                                records.Add(builder.Build());
                            }
                            else
                            {
                                Debug.WriteLine($"Batch {i} has {groupNumRows} values but {columnReader.ElementType} is not currently supported for parsing parquet files");
                            }
                        }

                        RecordBatch recordBatch = new RecordBatch(arrowSchema, records, groupNumRows);

                        recordBatches.Add(recordBatch);
                    }
                }

                file.Close();
            }

            return recordBatches;
        }

        private static Arrow.Schema ConvertParquetSchemaDescripterToArrowSchema(SchemaDescriptor parquetSchema)
        {
            List<Field> fields = new List<Field>();

            // TODO: additional types, nullable

            for (int i = 0; i < parquetSchema.NumColumns; i++)
            {
                ColumnDescriptor columnDescriptor = parquetSchema.Column(i);

                // need to use .Equals (not == )

                if (columnDescriptor.PhysicalType.Equals(PhysicalType.Double))
                {
                    fields.Add(new Field(columnDescriptor.Name, DoubleType.Default, true));
                }
                else if (columnDescriptor.LogicalType.Equals(LogicalType.Int(8, true)))
                {
                    fields.Add(new Field(columnDescriptor.Name, Int8Type.Default, true));
                }
                else if (columnDescriptor.LogicalType.Equals(LogicalType.Int(16, true)))
                {
                    fields.Add(new Field(columnDescriptor.Name, Int16Type.Default, true));
                }
                else if (columnDescriptor.LogicalType.Equals(LogicalType.Int(32, true)))
                {
                    fields.Add(new Field(columnDescriptor.Name, Int32Type.Default, true));
                }
                else if (columnDescriptor.LogicalType.Equals(LogicalType.Int(64, true)))
                {
                    fields.Add(new Field(columnDescriptor.Name, Int64Type.Default, true));
                }
                else if (columnDescriptor.LogicalType.Equals(LogicalType.String()))
                {
                    fields.Add(new Field(columnDescriptor.Name, StringType.Default, true));
                }
                else if (columnDescriptor.LogicalType.Equals(LogicalType.Timestamp(true, ParquetSharp.TimeUnit.Millis)))
                {
                    fields.Add(new Field(columnDescriptor.Name, Date64Type.Default, true));
                }
                else
                {
                    Debug.WriteLine($"Column {i} is called {columnDescriptor.Name} with a logical type of {columnDescriptor.LogicalType} and a physical type of {columnDescriptor.PhysicalType}. This logical type cannot be parsed.");

                }
            }

            Arrow.Schema arrowSchema = new Schema(fields, null);

            return arrowSchema;
        }
    }
}

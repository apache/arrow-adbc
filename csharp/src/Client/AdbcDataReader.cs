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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Represents a DbDataReader over Arrow record batches
    /// </summary>
    public sealed class AdbcDataReader : DbDataReader, IDbColumnSchemaGenerator
    {
        private readonly AdbcCommand adbcCommand;
        private QueryResult adbcQueryResult;
        private RecordBatch recordBatch;
        private int currentRowInRecordBatch;
        private Schema schema = null;
        private bool isClosed;
        private int recordsEffected = -1;

        internal AdbcDataReader(AdbcCommand adbcCommand, QueryResult adbcQueryResult)
        {
            if (adbcCommand == null)
                throw new ArgumentNullException(nameof(adbcCommand));

            if (adbcQueryResult == null)
                throw new ArgumentNullException(nameof(adbcQueryResult));

            this.adbcCommand = adbcCommand;
            this.adbcQueryResult = adbcQueryResult;
            this.schema = this.adbcQueryResult.Stream.Schema;

            if (this.schema == null)
                throw new ArgumentException("A Schema must be set for the AdbcQueryResult.Stream property");

            this.isClosed = false;
        }

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] =>  GetValue(this.recordBatch?.Column(name), GetOrdinal(name)) ;

        public override int Depth => 0;

        public override int FieldCount => this.schema == null ? 0 : this.schema.FieldsList.Count;

        public override bool HasRows => this.recordBatch?.Length > 0;

        public override bool IsClosed => this.isClosed;

        /// <summary>
        /// The Arrow schema under the reader
        /// </summary>
        public Schema ArrowSchema => this.schema;

        public override int RecordsAffected => this.recordsEffected;

        /// <summary>
        /// The total number of record batches in the result.
        /// </summary>
        public int TotalBatches { get; set; }

        public override void Close()
        {
            this.adbcCommand?.Connection?.Close();
            this.adbcQueryResult = null;
            this.isClosed = true;
        }

        public override bool GetBoolean(int ordinal)
        {
            return Convert.ToBoolean(GetValue(ordinal));
        }

        public override byte GetByte(int ordinal)
        {
            return Convert.ToByte(GetValue(ordinal));
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            return GetString(ordinal)[0];
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return this.GetAdbcColumnSchema()[ordinal].DataTypeName;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime) GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal) GetValue(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return (double) GetValue(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

#if NET5_0_OR_GREATER
        [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
#endif
        public override Type GetFieldType(int ordinal)
        {
            return this.GetAdbcColumnSchema()[ordinal].DataType;
        }

        public IArrowType GetFieldArrowType(int ordinal)
        {
            return this.GetAdbcColumnSchema()[ordinal].ArrowType;
        }

        public override float GetFloat(int ordinal)
        {
            return (float) GetValue(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return (Guid) GetValue(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return Convert.ToInt16(GetValue(ordinal));
        }

        public override int GetInt32(int ordinal)
        {
            return Convert.ToInt32(GetValue(ordinal));
        }

        public override long GetInt64(int ordinal)
        {
            return Convert.ToInt64(GetValue(ordinal));
        }

        public override string GetName(int ordinal)
        {
           return this.recordBatch?.Schema.GetFieldByIndex(ordinal)?.Name;
        }

        public override int GetOrdinal(string name)
        {
            return this.recordBatch.Schema.GetFieldIndex(name);
        }

        public override string GetString(int ordinal)
        {
            return Convert.ToString(GetValue(ordinal));
        }

        public override object GetValue(int ordinal)
        {
            return GetValue(this.recordBatch?.Column(ordinal), ordinal);
        }

        public override int GetValues(object[] values)
        {
            int count = Math.Min(FieldCount, values.Length);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            return GetValue(ordinal) == null;
        }

        public override bool NextResult()
        {
            this.recordBatch = ReadNextRecordBatchAsync().Result;

            if (this.recordBatch != null)
            {
                return true;
            }

            return false;
        }

        public new void Dispose()
        {
            this.recordBatch?.Dispose();
        }

        public override bool Read()
        {
            if (this.recordBatch != null && this.currentRowInRecordBatch < this.recordBatch.Length - 1)
            {
                this.currentRowInRecordBatch++;
                return true;
            }

            this.recordBatch = ReadNextRecordBatchAsync().Result;

            return this.recordBatch != null;
        }

        public override DataTable GetSchemaTable()
        {
            if (this.schema != null)
            {
                return SchemaConverter.ConvertArrowSchema(this.schema, this.adbcCommand.AdbcStatement);
            }
            else
            {
                throw new InvalidOperationException("Cannot obtain schema table because the Arrow schema is null");
            }
        }

#if NET5_0_OR_GREATER
        public override Task<DataTable> GetSchemaTableAsync(CancellationToken cancellationToken = default)
        {
            return Task.Run(() => GetSchemaTable(), cancellationToken);
        }
#endif
        public ReadOnlyCollection<DbColumn> GetColumnSchema()
        {
            return GetAdbcColumnSchema().Select(x => (DbColumn)x).ToList().AsReadOnly();
        }

        public ReadOnlyCollection<AdbcColumn> GetAdbcColumnSchema()
        {
            if (this.schema != null)
            {
                List<AdbcColumn> dbColumns = new List<AdbcColumn>();

                foreach (Field f in this.schema.FieldsList)
                {
                    Type t = SchemaConverter.ConvertArrowType(f);

                    if(f.HasMetadata &&
                       f.Metadata.ContainsKey("precision") &&
                       f.Metadata.ContainsKey("scale"))
                    {
                        int precision = Convert.ToInt32(f.Metadata["precision"]);
                        int scale = Convert.ToInt32(f.Metadata["scale"]);
                        dbColumns.Add(new AdbcColumn(f.Name, t, f.DataType, f.IsNullable, precision, scale));
                    }
                    else
                    {
                        dbColumns.Add(new AdbcColumn(f.Name, t, f.DataType, f.IsNullable));
                    }
                }

                return dbColumns.AsReadOnly();
            }
            else
            {
                throw new InvalidOperationException("Cannot obtain column schema because the Arrow schema is null");
            }
        }

        /// <summary>
        /// Gets the value from an IArrowArray at the current row index
        /// </summary>
        /// <param name="arrowArray"></param>
        /// <returns></returns>
        public object GetValue(IArrowArray arrowArray, int ordinal)
        {
            Field field = this.schema.GetFieldByIndex(ordinal);
            return this.adbcCommand.AdbcStatement.GetValue(arrowArray, field, this.currentRowInRecordBatch);
        }

        /// <summary>
        /// Retrieves the next record batch.
        /// </summary>
        /// <param name="cancellationToken">An optional cancellation token</param>
        /// <returns><see cref="RecordBatch"/> or null</returns>
        private ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            this.currentRowInRecordBatch = 0;

            RecordBatch recordBatch = this.adbcQueryResult.Stream.ReadNextRecordBatchAsync(cancellationToken).Result;

            if( recordBatch != null )
            {
                this.TotalBatches += 1;
            }

            return new ValueTask<RecordBatch>(recordBatch);
        }

    }
}

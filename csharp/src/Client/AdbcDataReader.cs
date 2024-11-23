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
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Invoked when a value is read from an Arrow array.
    /// </summary>
    /// <param name="arrowArray">The Arrow array.</param>
    /// <param name="index">The item index.</param>
    /// <returns>The value at the index.</returns>
    public delegate object GetValueEventHandler(IArrowArray arrowArray, int index);

    /// <summary>
    /// Represents a DbDataReader over Arrow record batches
    /// </summary>
    public sealed class AdbcDataReader : DbDataReader, IDbColumnSchemaGenerator
    {
        private readonly AdbcCommand adbcCommand;
        private readonly bool closeConnection;
        private readonly QueryResult adbcQueryResult;
        private RecordBatch? recordBatch;
        private int currentRowInRecordBatch;
        private readonly Schema schema;
        private bool isClosed;
        private int recordsAffected = -1;

        /// <summary>
        /// An event that is raised when a value is read from an IArrowArray.
        /// </summary>
        /// <remarks>
        /// Callers may opt to provide overrides for parsing values.
        /// </remarks>
        public event GetValueEventHandler? OnGetValue;

        internal AdbcDataReader(AdbcCommand adbcCommand, QueryResult adbcQueryResult, DecimalBehavior decimalBehavior, StructBehavior structBehavior, bool closeConnection)
        {
            if (adbcCommand == null)
                throw new ArgumentNullException(nameof(adbcCommand));

            if (adbcQueryResult == null)
                throw new ArgumentNullException(nameof(adbcQueryResult));

            if (adbcQueryResult.Stream == null)
                throw new ArgumentNullException(nameof(adbcQueryResult.Stream));

            this.adbcCommand = adbcCommand;
            this.adbcQueryResult = adbcQueryResult;
            this.schema = this.adbcQueryResult.Stream.Schema;

            if (this.schema == null)
                throw new ArgumentException("A Schema must be set for the AdbcQueryResult.Stream property");

            this.closeConnection = closeConnection;
            this.isClosed = false;
            this.DecimalBehavior = decimalBehavior;
            this.StructBehavior = structBehavior;
        }

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(this.RecordBatch.Column(name)) ?? DBNull.Value;

        public override int Depth => 0;

        public override int FieldCount => this.schema.FieldsList.Count;

        public override bool HasRows => this.RecordBatch.Length > 0;

        public override bool IsClosed => this.isClosed;

        /// <summary>
        /// The Arrow schema under the reader
        /// </summary>
        public Schema ArrowSchema => this.schema;

        public DecimalBehavior DecimalBehavior { get; set; }

        public StructBehavior StructBehavior { get; set; }

        public override int RecordsAffected => this.recordsAffected;

        /// <summary>
        /// The total number of record batches in the result.
        /// </summary>
        public int TotalBatches { get; private set; }

        private RecordBatch RecordBatch
        {
            get
            {
                if (this.recordBatch == null) { throw new InvalidOperationException("reader has been closed"); }
                return this.recordBatch;
            }
        }

        public override void Close()
        {
            if (this.closeConnection)
            {
                this.adbcCommand?.Connection?.Close();
            }
            this.adbcQueryResult.Stream?.Dispose();
            this.adbcQueryResult.Stream = null;
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

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            return GetString(ordinal)[0];
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return this.GetAdbcColumnSchema()[ordinal].DataTypeName!;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime) GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return Convert.ToDecimal(GetValue(ordinal));
        }

        public SqlDecimal GetSqlDecimal(int ordinal)
        {
            if (this.DecimalBehavior == DecimalBehavior.UseSqlDecimal)
            {
                return (SqlDecimal)GetValue(ordinal);
            }
            else
            {
                throw new InvalidOperationException("Cannot convert to SqlDecimal if DecimalBehavior.UseSqlDecimal is not configured");
            }
        }

        public override double GetDouble(int ordinal)
        {
            return Convert.ToDouble(GetValue(ordinal));
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
            return this.GetAdbcColumnSchema()[ordinal].DataType!;
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
           return this.schema.GetFieldByIndex(ordinal)?.Name ?? string.Empty;
        }

        public override int GetOrdinal(string name)
        {
            return this.schema.GetFieldIndex(name);
        }

        public override string GetString(int ordinal)
        {
            return Convert.ToString(GetValue(ordinal)) ?? throw new InvalidCastException();
        }

        public override object GetValue(int ordinal)
        {
            object? value = GetValue(this.RecordBatch.Column(ordinal));

            if (value == null)
                return DBNull.Value;

            if (value is SqlDecimal dValue && this.DecimalBehavior == DecimalBehavior.OverflowDecimalAsString)
            {
                try
                {
                    return dValue.Value;
                }
                catch(OverflowException)
                {
                    return dValue.ToString();
                }
            }

            return value;
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

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.recordBatch?.Dispose();
                this.recordBatch = null;
            }
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

        public override DataTable? GetSchemaTable()
        {
            return SchemaConverter.ConvertArrowSchema(this.schema, this.adbcCommand.AdbcStatement, this.DecimalBehavior, this.StructBehavior);
        }

#if NET5_0_OR_GREATER
        public override Task<DataTable?> GetSchemaTableAsync(CancellationToken cancellationToken = default)
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
            List<AdbcColumn> dbColumns = new List<AdbcColumn>();

            foreach (Field f in this.schema.FieldsList)
            {
                Type t = SchemaConverter.ConvertArrowType(f, this.DecimalBehavior, this.StructBehavior);

                if (f.HasMetadata &&
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

        /// <summary>
        /// Gets the value from an IArrowArray at the current row index.
        /// </summary>
        /// <param name="arrowArray"></param>
        /// <returns></returns>
        private object? GetValue(IArrowArray arrowArray)
        {
            // if the OnGetValue event is set, call it
            object? result = OnGetValue?.Invoke(arrowArray, this.currentRowInRecordBatch);

            // if the value is null, try to get the value from the ArrowArray
            result = result ?? arrowArray.ValueAt(this.currentRowInRecordBatch);

            return result;
        }

        /// <summary>
        /// Retrieves the next record batch.
        /// </summary>
        /// <param name="cancellationToken">An optional cancellation token</param>
        /// <returns><see cref="RecordBatch"/> or null</returns>
        private ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            this.currentRowInRecordBatch = 0;

            RecordBatch? recordBatch = this.adbcQueryResult.Stream?.ReadNextRecordBatchAsync(cancellationToken).Result;

            if (recordBatch != null)
            {
                this.TotalBatches += 1;
            }

            return new ValueTask<RecordBatch?>(recordBatch);
        }
    }
}

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
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Creates an ADO.NET command over an Adbc statement.
    /// </summary>
    public sealed class AdbcCommand : DbCommand
    {
        private AdbcStatement _adbcStatement;
        private AdbcParameterCollection? _dbParameterCollection;
        private int _timeout = 30;
        private bool _disposed;

        /// <summary>
        /// Overloaded. Initializes <see cref="AdbcCommand"/>.
        /// </summary>
        /// <param name="adbcConnection">
        /// The <see cref="AdbcConnection"/> to use.
        /// </param>
        /// <exception cref="ArgumentNullException"></exception>
        public AdbcCommand(AdbcConnection adbcConnection) : base()
        {
            if (adbcConnection == null)
                throw new ArgumentNullException(nameof(adbcConnection));

            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
            this._adbcStatement = adbcConnection.CreateStatement();
        }

        /// <summary>
        /// Overloaded. Initializes <see cref="AdbcCommand"/>.
        /// </summary>
        /// <param name="query">The command text to use.</param>
        /// <param name="adbcConnection">The <see cref="AdbcConnection"/> to use.</param>
        public AdbcCommand(string query, AdbcConnection adbcConnection) : base()
        {
            if (string.IsNullOrEmpty(query))
                throw new ArgumentNullException(nameof(query));

            if (adbcConnection == null)
                throw new ArgumentNullException(nameof(adbcConnection));

            this._adbcStatement = adbcConnection.CreateStatement();
            this.CommandText = query;

            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
        }

        // For testing
        internal AdbcCommand(AdbcStatement adbcStatement, AdbcConnection adbcConnection)
        {
            this._adbcStatement = adbcStatement;
            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
        }

        /// <summary>
        /// Gets the <see cref="AdbcStatement"/> associated with
        /// this <see cref="AdbcCommand"/>.
        /// </summary>
        public AdbcStatement AdbcStatement => _disposed ? throw new ObjectDisposedException(nameof(AdbcCommand)) : this._adbcStatement;

        public DecimalBehavior DecimalBehavior { get; set; }

        public override string CommandText
        {
            get => AdbcStatement.SqlQuery ?? string.Empty;
#nullable disable
            set => AdbcStatement.SqlQuery = string.IsNullOrEmpty(value) ? null : value;
#nullable restore
        }

        public override CommandType CommandType
        {
            get
            {
                return CommandType.Text;
            }

            set
            {
                if (value != CommandType.Text)
                {
                    throw new AdbcException("Only CommandType.Text is supported");
                }
            }
        }

        public override int CommandTimeout
        {
            get => _timeout;
            set => _timeout = value;
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                if (_dbParameterCollection == null)
                {
                    _dbParameterCollection = new AdbcParameterCollection();
                }
                return _dbParameterCollection;
            }
        }

        /// <summary>
        /// Gets or sets the Substrait plan used by the command.
        /// </summary>
        public byte[]? SubstraitPlan
        {
            get => AdbcStatement.SubstraitPlan;
            set => AdbcStatement.SubstraitPlan = value;
        }

        protected override DbConnection? DbConnection { get; set; }

        public override int ExecuteNonQuery()
        {
            BindParameters();
            return Convert.ToInt32(AdbcStatement.ExecuteUpdate().AffectedRows);
        }

        /// <summary>
        /// Similar to <see cref="ExecuteNonQuery"/> but returns Int64
        /// instead of Int32.
        /// </summary>
        /// <returns></returns>
        public long ExecuteUpdate()
        {
            BindParameters();
            return AdbcStatement.ExecuteUpdate().AffectedRows;
        }

        /// <summary>
        /// Executes the query
        /// </summary>
        /// <returns><see cref="Result"></returns>
        public QueryResult ExecuteQuery()
        {
            BindParameters();
            QueryResult executed = AdbcStatement.ExecuteQuery();

            return executed;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return ExecuteReader(behavior);
        }

        /// <summary>
        /// Executes the reader with the default behavior.
        /// </summary>
        /// <returns><see cref="AdbcDataReader"/></returns>
        public new AdbcDataReader ExecuteReader()
        {
            return ExecuteReader(CommandBehavior.Default);
        }

        /// <summary>
        /// Executes the reader with the specified behavior.
        /// </summary>
        /// <param name="behavior">
        /// The <see cref="CommandBehavior"/>
        /// </param>
        /// <returns><see cref="AdbcDataReader"/></returns>
        public new AdbcDataReader ExecuteReader(CommandBehavior behavior)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(AdbcCommand));

            bool closeConnection = (behavior & CommandBehavior.CloseConnection) != 0;
            switch (behavior & ~CommandBehavior.CloseConnection)
            {
                case CommandBehavior.SchemaOnly:   // The schema is not known until a read happens
                case CommandBehavior.Default:
                    QueryResult result = this.ExecuteQuery();
                    return new AdbcDataReader(this, result, this.DecimalBehavior, closeConnection);

                default:
                    throw new InvalidOperationException($"{behavior} is not supported with this provider");
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                // TODO: ensure not in the middle of pulling
                this._adbcStatement.Dispose();
                _disposed = true;
            }

            base.Dispose(disposing);
        }

        private void BindParameters()
        {
            if (_dbParameterCollection?.Count > 0)
            {
                Field[] fields = new Field[_dbParameterCollection.Count];
                IArrowArray[] parameters = new IArrowArray[_dbParameterCollection.Count];
                for (int i = 0; i < fields.Length; i++)
                {
                    AdbcParameter param = (AdbcParameter)_dbParameterCollection[i];
                    ArrowType type;
                    switch (param.DbType)
                    {
                        case DbType.Binary:
                            type = BinaryType.Default;
                            var binaryBuilder = new BinaryArray.Builder();
                            if (param.Value == null)
                            {
                                binaryBuilder.AppendNull();
                            }
                            else
                            {
                                binaryBuilder.Append(((byte[])param.Value).AsSpan());
                            }
                            parameters[i] = binaryBuilder.Build();
                            break;
                        case DbType.Boolean:
                            type = BooleanType.Default;
                            var boolBuilder = new BooleanArray.Builder();
                            if (param.Value == null)
                            {
                                boolBuilder.AppendNull();
                            }
                            else
                            {
                                boolBuilder.Append((bool)param.Value);
                            }
                            parameters[i] = boolBuilder.Build();
                            break;
                        case DbType.Byte:
                            type = UInt8Type.Default;
                            parameters[i] = new UInt8Array.Builder().Append((byte?)param.Value).Build();
                            break;
                        case DbType.Date:
                            type = Date32Type.Default;
                            var dateBuilder = new Date32Array.Builder();
                            if (param.Value == null)
                            {
                                dateBuilder.AppendNull();
                            }
#if NET5_0_OR_GREATER
                            else if (param.Value is DateOnly)
                            {
                                dateBuilder.Append((DateOnly)param.Value);
                            }
#endif
                            else
                            {
                                dateBuilder.Append((DateTime)param.Value);
                            }
                            parameters[i] = dateBuilder.Build();
                            break;
                        case DbType.DateTime:
                            type = TimestampType.Default;
                            var timestampBuilder = new TimestampArray.Builder();
                            if (param.Value == null)
                            {
                                timestampBuilder.AppendNull();
                            }
                            else
                            {
                                timestampBuilder.Append((DateTime)param.Value);
                            }
                            break;
                        // TODO: case DbType.Decimal:
                        case DbType.Double:
                            type = DoubleType.Default;
                            parameters[i] = new DoubleArray.Builder().Append((double?)param.Value).Build();
                            break;
                        case DbType.Int16:
                            type = Int16Type.Default;
                            parameters[i] = new Int16Array.Builder().Append((short?)param.Value).Build();
                            break;
                        case DbType.Int32:
                            type = Int32Type.Default;
                            parameters[i] = new Int32Array.Builder().Append((int?)param.Value).Build();
                            break;
                        case DbType.Int64:
                            type = Int64Type.Default;
                            parameters[i] = new Int64Array.Builder().Append((long?)param.Value).Build();
                            break;
                        case DbType.SByte:
                            type = Int8Type.Default;
                            parameters[i] = new Int8Array.Builder().Append((sbyte?)param.Value).Build();
                            break;
                        case DbType.Single:
                            type = FloatType.Default;
                            parameters[i] = new FloatArray.Builder().Append((float?)param.Value).Build();
                            break;
                        case DbType.String:
                            type = StringType.Default;
                            parameters[i] = new StringArray.Builder().Append((string)param.Value!).Build();
                            break;
                        // TODO: case DbType.Time:
                        case DbType.UInt16:
                            type = UInt16Type.Default;
                            parameters[i] = new UInt16Array.Builder().Append((ushort?)param.Value).Build();
                            break;
                        case DbType.UInt32:
                            type = UInt32Type.Default;
                            parameters[i] = new UInt32Array.Builder().Append((uint?)param.Value).Build();
                            break;
                        case DbType.UInt64:
                            type = UInt64Type.Default;
                            parameters[i] = new UInt64Array.Builder().Append((ulong?)param.Value).Build();
                            break;
                        default:
                            throw new NotSupportedException($"Parameters of type {param.DbType} are not supported");
                    }

                    fields[i] = new Field(
                        string.IsNullOrWhiteSpace(param.ParameterName) ? Guid.NewGuid().ToString() : param.ParameterName,
                        type,
                        param.IsNullable || param.Value == null);
                }

                Schema schema = new Schema(fields, null);
                AdbcStatement.Bind(new RecordBatch(schema, parameters, 1), schema);
            }
        }

#if NET5_0_OR_GREATER
        public override ValueTask DisposeAsync()
        {
            return base.DisposeAsync();
        }
#endif
        #region NOT_IMPLEMENTED

        public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override UpdateRowSource UpdatedRowSource { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        protected override DbTransaction? DbTransaction { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        public override object ExecuteScalar()
        {
            throw new NotImplementedException();
        }

        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            return new AdbcParameter();
        }

        #endregion

        private class AdbcParameterCollection : DbParameterCollection
        {
            readonly List<AdbcParameter> _parameters = new List<AdbcParameter>();

            public override int Count => _parameters.Count;

            public override object SyncRoot => throw new NotImplementedException();

            public override int Add(object value)
            {
                int result = _parameters.Count;
                _parameters.Add((AdbcParameter)value);
                return result;
            }

            public override void AddRange(System.Array values) => _parameters.AddRange(values.Cast<AdbcParameter>());
            public override void Clear() => _parameters.Clear();
            public override bool Contains(object value) => _parameters.Contains((AdbcParameter)value);
            public override bool Contains(string value) => IndexOf(value) >= 0;
            public override void CopyTo(System.Array array, int index) => throw new NotImplementedException();
            public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();
            public override int IndexOf(object value) => _parameters.IndexOf((AdbcParameter)value);
            public override int IndexOf(string parameterName) => GetParameterIndex(parameterName, throwOnFailure: false);
            public override void Insert(int index, object value) => _parameters.Insert(index, (AdbcParameter)value);
            public override void Remove(object value) => _parameters.Remove((AdbcParameter)value);
            public override void RemoveAt(int index) => _parameters.RemoveAt(index);
            public override void RemoveAt(string parameterName) => _parameters.RemoveAt(GetParameterIndex(parameterName));
            protected override DbParameter GetParameter(int index) => _parameters[index];
            protected override DbParameter GetParameter(string parameterName) => _parameters[GetParameterIndex(parameterName)];
            protected override void SetParameter(int index, DbParameter value) => _parameters[index] = (AdbcParameter)value;
            protected override void SetParameter(string parameterName, DbParameter value) => throw new NotImplementedException();

            private int GetParameterIndex(string parameterName, bool throwOnFailure = true)
            {
                for (int i = 0; i < _parameters.Count; i++)
                {
                    if (parameterName == _parameters[i].ParameterName)
                    {
                        return i;
                    }
                }

                if (throwOnFailure)
                {
                    throw new IndexOutOfRangeException("parameterName not found");
                }

                return -1;
            }
        }
    }
}

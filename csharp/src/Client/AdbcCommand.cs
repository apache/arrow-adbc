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
using System.Data.SqlTypes;
using System.Globalization;
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
        private readonly AdbcStatement _adbcStatement;
        private AdbcParameterCollection? _dbParameterCollection;
        private int _timeout = 30;
        private bool _disposed;
        private string? _commandTimeoutProperty;

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
            this.StructBehavior = adbcConnection.StructBehavior;
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
            this.StructBehavior = adbcConnection.StructBehavior;
        }

        // For testing
        internal AdbcCommand(AdbcStatement adbcStatement, AdbcConnection adbcConnection)
        {
            this._adbcStatement = adbcStatement;
            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
            this.StructBehavior = adbcConnection.StructBehavior;

            if (adbcConnection.CommandTimeoutValue != null)
            {
                this.AdbcCommandTimeoutProperty = adbcConnection.CommandTimeoutValue.DriverPropertyName;
                this.CommandTimeout = adbcConnection.CommandTimeoutValue.Value;
            }
        }

        /// <summary>
        /// Gets the <see cref="AdbcStatement"/> associated with
        /// this <see cref="AdbcCommand"/>.
        /// </summary>
        public AdbcStatement AdbcStatement => _disposed ? throw new ObjectDisposedException(nameof(AdbcCommand)) : this._adbcStatement;

        public DecimalBehavior DecimalBehavior { get; set; }

        public StructBehavior StructBehavior { get; set; }

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

        /// <summary>
        /// Gets or sets the name of the command timeout property for the underlying ADBC driver.
        /// </summary>
        public string AdbcCommandTimeoutProperty
        {
            get
            {
                if (string.IsNullOrEmpty(_commandTimeoutProperty))
                    throw new InvalidOperationException("CommandTimeoutProperty is not set.");

                return _commandTimeoutProperty!;
            }
            set => _commandTimeoutProperty = value;
        }

        public override int CommandTimeout
        {
            get => _timeout;
            set
            {
                // ensures the property exists before setting the CommandTimeout value
                string property = AdbcCommandTimeoutProperty;
                _adbcStatement.SetOption(property, value.ToString(CultureInfo.InvariantCulture));
                _timeout = value;
            }
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
                    return new AdbcDataReader(this, result, this.DecimalBehavior, this.StructBehavior, closeConnection);

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
                    switch (param.DbType)
                    {
                        case DbType.Binary:
                            var binaryBuilder = new BinaryArray.Builder();
                            switch (param.Value)
                            {
                                case null: binaryBuilder.AppendNull(); break;
                                case byte[] array: binaryBuilder.Append(array.AsSpan()); break;
                                default: throw new NotSupportedException($"Values of type {param.Value.GetType().Name} cannot be bound as binary");
                            }
                            parameters[i] = binaryBuilder.Build();
                            break;
                        case DbType.Boolean:
                            var boolBuilder = new BooleanArray.Builder();
                            switch (param.Value)
                            {
                                case null: boolBuilder.AppendNull(); break;
                                case bool boolValue: boolBuilder.Append(boolValue); break;
                                default: boolBuilder.Append(ConvertValue(param.Value, Convert.ToBoolean, DbType.Boolean)); break;
                            }
                            parameters[i] = boolBuilder.Build();
                            break;
                        case DbType.Byte:
                            var uint8Builder = new UInt8Array.Builder();
                            switch (param.Value)
                            {
                                case null: uint8Builder.AppendNull(); break;
                                case byte byteValue: uint8Builder.Append(byteValue); break;
                                default: uint8Builder.Append(ConvertValue(param.Value, Convert.ToByte, DbType.Byte)); break;
                            }
                            parameters[i] = uint8Builder.Build();
                            break;
                        case DbType.Date:
                            var dateBuilder = new Date32Array.Builder();
                            switch (param.Value)
                            {
                                case null: dateBuilder.AppendNull(); break;
                                case DateTime datetime: dateBuilder.Append(datetime); break;
#if NET5_0_OR_GREATER
                                case DateOnly dateonly: dateBuilder.Append(dateonly); break;
#endif
                                default: dateBuilder.Append(ConvertValue(param.Value, Convert.ToDateTime, DbType.Date)); break;
                            }
                            parameters[i] = dateBuilder.Build();
                            break;
                        case DbType.DateTime:
                            var timestampBuilder = new TimestampArray.Builder();
                            switch (param.Value)
                            {
                                case null: timestampBuilder.AppendNull(); break;
                                case DateTime datetime: timestampBuilder.Append(datetime); break;
                                default: timestampBuilder.Append(ConvertValue(param.Value, Convert.ToDateTime, DbType.DateTime)); break;
                            }
                            parameters[i] = timestampBuilder.Build();
                            break;
                        case DbType.Decimal:
                            var value = param.Value switch
                            {
                                null => (SqlDecimal?)null,
                                SqlDecimal sqlDecimal => sqlDecimal,
                                decimal d => new SqlDecimal(d),
                                _ => new SqlDecimal(ConvertValue(param.Value, Convert.ToDecimal, DbType.Decimal)),
                            };
                            var decimalBuilder = new Decimal128Array.Builder(new Decimal128Type(value?.Precision ?? 10, value?.Scale ?? 0));
                            if (value is null)
                            {
                                decimalBuilder.AppendNull();
                            }
                            else
                            {
                                decimalBuilder.Append(value.Value);
                            }
                            parameters[i] = decimalBuilder.Build();
                            break;
                        case DbType.Double:
                            var doubleBuilder = new DoubleArray.Builder();
                            switch (param.Value)
                            {
                                case null: doubleBuilder.AppendNull(); break;
                                case double dbl: doubleBuilder.Append(dbl); break;
                                default: doubleBuilder.Append(ConvertValue(param.Value, Convert.ToDouble, DbType.Double)); break;
                            }
                            parameters[i] = doubleBuilder.Build();
                            break;
                        case DbType.Int16:
                            var int16Builder = new Int16Array.Builder();
                            switch (param.Value)
                            {
                                case null: int16Builder.AppendNull(); break;
                                case short shortValue: int16Builder.Append(shortValue); break;
                                default: int16Builder.Append(ConvertValue(param.Value, Convert.ToInt16, DbType.Int16)); break;
                            }
                            parameters[i] = int16Builder.Build();
                            break;
                        case DbType.Int32:
                            var int32Builder = new Int32Array.Builder();
                            switch (param.Value)
                            {
                                case null: int32Builder.AppendNull(); break;
                                case int intValue: int32Builder.Append(intValue); break;
                                default: int32Builder.Append(ConvertValue(param.Value, Convert.ToInt32, DbType.Int32)); break;
                            }
                            parameters[i] = int32Builder.Build();
                            break;
                        case DbType.Int64:
                            var int64Builder = new Int64Array.Builder();
                            switch (param.Value)
                            {
                                case null: int64Builder.AppendNull(); break;
                                case long longValue: int64Builder.Append(longValue); break;
                                default: int64Builder.Append(ConvertValue(param.Value, Convert.ToInt64, DbType.Int64)); break;
                            }
                            parameters[i] = int64Builder.Build();
                            break;
                        case DbType.SByte:
                            var int8Builder = new Int8Array.Builder();
                            switch (param.Value)
                            {
                                case null: int8Builder.AppendNull(); break;
                                case sbyte sbyteValue: int8Builder.Append(sbyteValue); break;
                                default: int8Builder.Append(ConvertValue(param.Value, Convert.ToSByte, DbType.SByte)); break;
                            }
                            parameters[i] = int8Builder.Build();
                            break;
                        case DbType.Single:
                            var floatBuilder = new FloatArray.Builder();
                            switch (param.Value)
                            {
                                case null: floatBuilder.AppendNull(); break;
                                case float floatValue: floatBuilder.Append(floatValue); break;
                                default: floatBuilder.Append(ConvertValue(param.Value, Convert.ToSingle, DbType.Single)); break;
                            }
                            parameters[i] = floatBuilder.Build();
                            break;
                        case DbType.String:
                            var stringBuilder = new StringArray.Builder();
                            switch (param.Value)
                            {
                                case null: stringBuilder.AppendNull(); break;
                                case string stringValue: stringBuilder.Append(stringValue); break;
                                default: stringBuilder.Append(ConvertValue(param.Value, Convert.ToString, DbType.String)); break;
                            }
                            parameters[i] = stringBuilder.Build();
                            break;
                        case DbType.Time:
                            var timeBuilder = new Time32Array.Builder();
                            switch (param.Value)
                            {
                                case null: timeBuilder.AppendNull(); break;
                                case DateTime datetime: timeBuilder.Append((int)(datetime.TimeOfDay.Ticks / TimeSpan.TicksPerMillisecond)); break;
#if NET5_0_OR_GREATER
                                case TimeOnly timeonly: timeBuilder.Append(timeonly); break;
#endif
                                default:
                                    DateTime convertedDateTime = ConvertValue(param.Value, Convert.ToDateTime, DbType.Time);
                                    timeBuilder.Append((int)(convertedDateTime.TimeOfDay.Ticks / TimeSpan.TicksPerMillisecond));
                                    break;
                            }
                            parameters[i] = timeBuilder.Build();
                            break;
                        case DbType.UInt16:
                            var uint16Builder = new UInt16Array.Builder();
                            switch (param.Value)
                            {
                                case null: uint16Builder.AppendNull(); break;
                                case ushort ushortValue: uint16Builder.Append(ushortValue); break;
                                default: uint16Builder.Append(ConvertValue(param.Value, Convert.ToUInt16, DbType.UInt16)); break;
                            }
                            parameters[i] = uint16Builder.Build();
                            break;
                        case DbType.UInt32:
                            var uint32Builder = new UInt32Array.Builder();
                            switch (param.Value)
                            {
                                case null: uint32Builder.AppendNull(); break;
                                case uint uintValue: uint32Builder.Append(uintValue); break;
                                default: uint32Builder.Append(ConvertValue(param.Value, Convert.ToUInt32, DbType.UInt32)); break;
                            }
                            parameters[i] = uint32Builder.Build();
                            break;
                        case DbType.UInt64:
                            var uint64Builder = new UInt64Array.Builder();
                            switch (param.Value)
                            {
                                case null: uint64Builder.AppendNull(); break;
                                case ulong ulongValue: uint64Builder.Append(ulongValue); break;
                                default: uint64Builder.Append(ConvertValue(param.Value, Convert.ToUInt64, DbType.UInt64)); break;
                            }
                            parameters[i] = uint64Builder.Build();
                            break;
                        default:
                            throw new NotSupportedException($"Parameters of type {param.DbType} are not supported");
                    }

                    fields[i] = new Field(
                        string.IsNullOrWhiteSpace(param.ParameterName) ? Guid.NewGuid().ToString() : param.ParameterName,
                        parameters[i].Data.DataType,
                        param.IsNullable || param.Value == null);
                }

                Schema schema = new Schema(fields, null);
                AdbcStatement.Bind(new RecordBatch(schema, parameters, 1), schema);
            }
        }

        private static T ConvertValue<T>(object value, Func<object, T> converter, DbType type)
        {
            try
            {
                return converter(value);
            }
            catch (Exception)
            {
                throw new NotSupportedException($"Values of type {value.GetType().Name} cannot be bound as {type}.");
            }
        }

        public override void Prepare()
        {
            _adbcStatement.Prepare();
            var schema = _adbcStatement.GetParameterSchema();

            DbParameterCollection.Clear();

            foreach (Field field in schema.FieldsList)
            {
                AdbcParameter parameter = new AdbcParameter
                {
                    ParameterName = field.Name,
                    IsNullable = field.IsNullable,
                    DbType = field.DataType.TypeId switch
                    {
                        ArrowTypeId.UInt8 => DbType.Byte,
                        ArrowTypeId.UInt16 => DbType.UInt16,
                        ArrowTypeId.UInt32 => DbType.UInt32,
                        ArrowTypeId.UInt64 => DbType.UInt64,
                        ArrowTypeId.Int8 => DbType.SByte,
                        ArrowTypeId.Int16 => DbType.Int16,
                        ArrowTypeId.Int32 => DbType.Int32,
                        ArrowTypeId.Int64 => DbType.Int64,
                        ArrowTypeId.Float => DbType.Single,
                        ArrowTypeId.Double => DbType.Double,
                        ArrowTypeId.Boolean => DbType.Boolean,
                        ArrowTypeId.String => DbType.String,
                        ArrowTypeId.Date32 => DbType.Date,
                        ArrowTypeId.Date64 => DbType.DateTime,
                        ArrowTypeId.Time32 => DbType.Time,
                        ArrowTypeId.Time64 => DbType.Time,
                        ArrowTypeId.Timestamp => DbType.DateTime,
                        ArrowTypeId.Decimal32 or
                        ArrowTypeId.Decimal64 or
                        ArrowTypeId.Decimal128 or
                        ArrowTypeId.Decimal256 => DbType.Decimal,
                        _ => DbType.Object,
                    },
                };
                DbParameterCollection.Add(parameter);
            }
        }

        protected override DbParameter CreateDbParameter()
        {
            return new AdbcParameter();
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

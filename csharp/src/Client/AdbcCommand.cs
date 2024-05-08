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
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Creates an ADO.NET command over an Adbc statement.
    /// </summary>
    public sealed class AdbcCommand : DbCommand
    {
        private AdbcStatement adbcStatement;
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
            this.adbcStatement = adbcConnection.CreateStatement();
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

            this.adbcStatement = adbcConnection.CreateStatement();
            this.CommandText = query;

            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
        }

        // For testing
        internal AdbcCommand(AdbcStatement adbcStatement, AdbcConnection adbcConnection)
        {
            this.adbcStatement = adbcStatement;
            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
        }

        /// <summary>
        /// Gets the <see cref="AdbcStatement"/> associated with
        /// this <see cref="AdbcCommand"/>.
        /// </summary>
        public AdbcStatement AdbcStatement => _disposed ? throw new ObjectDisposedException(nameof(AdbcCommand)) : this.adbcStatement;

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
            return Convert.ToInt32(AdbcStatement.ExecuteUpdate().AffectedRows);
        }

        /// <summary>
        /// Similar to <see cref="ExecuteNonQuery"/> but returns Int64
        /// instead of Int32.
        /// </summary>
        /// <returns></returns>
        public long ExecuteUpdate()
        {
            return AdbcStatement.ExecuteUpdate().AffectedRows;
        }

        /// <summary>
        /// Executes the query
        /// </summary>
        /// <returns><see cref="Result"></returns>
        public QueryResult ExecuteQuery()
        {
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
                this.adbcStatement.Dispose();
                _disposed = true;
            }

            base.Dispose(disposing);
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

        protected override DbParameterCollection DbParameterCollection => throw new NotImplementedException();

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
            throw new NotImplementedException();
        }

        #endregion
    }
}

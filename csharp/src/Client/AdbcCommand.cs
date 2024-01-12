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
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Creates an ADO.NET command over an Adbc statement.
    /// </summary>
    public sealed class AdbcCommand : DbCommand
    {
        private AdbcStatement _adbcStatement;
        private int _timeout = 30;
        public QueryConfiguration _queryConfiguration = new QueryConfiguration();

        /// <summary>
        /// Overloaded. Initializes <see cref="AdbcCommand"/>.
        /// </summary>
        /// <param name="adbcStatement">
        /// The <see cref="AdbcStatement"/> to use.
        /// </param>
        /// <param name="adbcConnection">
        /// The <see cref="AdbcConnection"/> to use.
        /// </param>
        /// <exception cref="ArgumentNullException"></exception>
        public AdbcCommand(AdbcStatement adbcStatement, AdbcConnection adbcConnection) : base()
        {
            if(adbcStatement == null)
                throw new ArgumentNullException(nameof(adbcStatement));

            if(adbcConnection == null)
                throw new ArgumentNullException(nameof(adbcConnection));

            this._adbcStatement = adbcStatement;
            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
        }

        /// <summary>
        /// Overloaded. Initializes <see cref="AdbcCommand"/>.
        /// </summary>
        /// <param name="query">The command text to use.</param>
        /// <param name="adbcConnection">The <see cref="AdbcConnection"/> to use.</param>
        public AdbcCommand(string query, AdbcConnection adbcConnection) : base()
        {
            if (string.IsNullOrEmpty(query))
                throw new ArgumentNullException(nameof(_adbcStatement));

            if (adbcConnection == null)
                throw new ArgumentNullException(nameof(adbcConnection));

            this._adbcStatement = adbcConnection.AdbcStatement;
            this.CommandText = query;

            this.DbConnection = adbcConnection;
            this.DecimalBehavior = adbcConnection.DecimalBehavior;
        }

        /// <summary>
        /// Gets the <see cref="AdbcStatement"/> associated with
        /// this <see cref="AdbcCommand"/>.
        /// </summary>
        public AdbcStatement AdbcStatement => this._adbcStatement;

        public DecimalBehavior DecimalBehavior { get; set; }

        public override string CommandText
        {
            get => this._adbcStatement.SqlQuery;
            set => this._adbcStatement.SqlQuery = value;
        }

        public QueryConfiguration QueryConfiguration
        {
            get => this._queryConfiguration;
            set => this._queryConfiguration = value;
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
        public byte[] SubstraitPlan
        {
            get => this._adbcStatement.SubstraitPlan;
            set => this._adbcStatement.SubstraitPlan = value;
        }

        protected override DbConnection DbConnection { get; set; }

        public override int ExecuteNonQuery()
        {
            return Convert.ToInt32(this.ExecuteUpdate().AffectedRows);
        }

        /// <summary>
        /// Returns <see cref="UpdateResult"/>.
        /// </summary>
        public UpdateResult ExecuteUpdate()
        {
            return this._adbcStatement.ExecuteUpdate();
        }

        /// <summary>
        /// Executes the query
        /// </summary>
        /// <returns><see cref="Result"></returns>
        public QueryResult ExecuteQuery()
        {
            QueryResult executed = this._adbcStatement.ExecuteQuery();

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
            switch (behavior)
            {
                case CommandBehavior.SchemaOnly:   // The schema is not known until a read happens
                case CommandBehavior.Default:

                    // ADBC doesn't have very good support for multi-statements
                    // see https://github.com/apache/arrow-adbc/issues/1358
                    // so this attempts to work around that by making multiple calls
                    // it will return the first result set and the "RecordsAffected" for any other type of calls

                    if (this.QueryConfiguration != null)
                    {
                        QueryParser queryParser = new QueryParser(this.QueryConfiguration);
                        List<Query> queries = queryParser.ParseQuery(this.CommandText);

                        QueryResult queryResult = null;
                        int recordsEffected = -1;

                        foreach(Query q in queries)
                        {
                            if (q.Type == QueryReturnType.RecordSet)
                            {
                                if(queryResult == null)
                                {
                                    this._adbcStatement.SqlQuery = q.Text;
                                    queryResult = this.ExecuteQuery();
                                }
                            }
                            else
                            {
                                if(recordsEffected == -1)
                                    recordsEffected++;

                                this._adbcStatement.SqlQuery = q.Text;
                                recordsEffected += this.ExecuteNonQuery();
                            }
                        }

                        if (queryResult != null)
                            return new AdbcDataReader(this, queryResult, this.DecimalBehavior, recordsEffected);
                        else
                            return new AdbcDataReader(recordsEffected);
                    }
                    else
                    {
                        QueryResult result = this.ExecuteQuery();
                        return new AdbcDataReader(this, result, this.DecimalBehavior);
                    }

                default:
                    throw new InvalidOperationException($"{behavior} is not supported with this provider");
            }
        }

        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                // TODO: ensure not in the middle of pulling
                this._adbcStatement?.Dispose();
            }

            base.Dispose(disposing);

            GC.SuppressFinalize(this);
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

        protected override DbTransaction DbTransaction { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

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

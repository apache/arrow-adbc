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

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// Creates an ADO.NET connection over an Adbc driver.
    /// </summary>
    public sealed class AdbcConnection : DbConnection
    {
        private AdbcDatabase adbcDatabase;
        private Adbc.AdbcConnection adbcConnectionInternal;

        private readonly Dictionary<string, string> adbcConnectionParameters;
        private readonly Dictionary<string, string> adbcConnectionOptions;

        private AdbcStatement adbcStatement;

        /// <summary>
        /// Overloaded. Intializes an <see cref="AdbcConnection"/>.
        /// </summary>
        public AdbcConnection()
        {
           this.AdbcDriver = null;
           this.adbcConnectionParameters = new Dictionary<string, string>();
           this.adbcConnectionOptions = new Dictionary<string, string>();
        }

        /// <summary>
        /// Overloaded. Intializes an <see cref="AdbcConnection"/>.
        /// <param name="connectionString">The connection string to use.</param>
        /// </summary>
        public AdbcConnection(string connectionString) : this()
        {
            this.ConnectionString = connectionString;
        }

        /// <summary>
        /// Overloaded. Intializes an <see cref="AdbcConnection"/>.
        /// </summary>
        /// <param name="adbcDriver">
        /// The <see cref="AdbcDriver"/> to use for connecting. This value
        /// must be set before a connection can be established.
        /// </param>
        public AdbcConnection(AdbcDriver adbcDriver) : this()
        {
            this.AdbcDriver = adbcDriver;
        }

        /// <summary>
        /// Overloaded. Intializes an <see cref="AdbcConnection"/>.
        /// </summary>
        /// <param name="adbcDriver">
        /// The <see cref="AdbcDriver"/> to use for connecting. This value
        /// must be set before a connection can be established.
        /// </param>
        /// <param name="parameters">
        /// The connection parameters to use (similar to connection string).
        /// </param>
        /// <param name="options">
        /// Any additional options to apply when connection to the
        /// <see cref="AdbcDatabase"/>.
        /// </param>
        public AdbcConnection(AdbcDriver adbcDriver, Dictionary<string, string> parameters, Dictionary<string, string> options)
        {
            this.AdbcDriver = adbcDriver;
            this.adbcConnectionParameters = parameters;
            this.adbcConnectionOptions = options;
        }

        /// <summary>
        /// Creates a new <see cref="AdbcCommand"/>.
        /// </summary>
        /// <returns><see cref="AdbcCommand"/></returns>
        public new AdbcCommand CreateCommand() => CreateDbCommand() as AdbcCommand;

        /// <summary>
        /// Gets or sets the <see cref="AdbcDriver"/> associated with this
        /// connection.
        /// </summary>
        public AdbcDriver AdbcDriver { get; set; }

        /// <summary>
        /// Gets the <see cref="AdbcStatement"/> associated with the
        /// connection.
        /// </summary>
        internal AdbcStatement AdbcStatement
        {
            get
            {
                if (this.adbcStatement == null)
                {
                    // need to have a connection in order to have a statement
                    EnsureConnectionOpen();
                    this.adbcStatement = this.adbcConnectionInternal.CreateStatement();
                }

                return this.adbcStatement;
            }
        }

        public override string ConnectionString { get => GetConnectionString(); set => SetConnectionProperties(value); }

        protected override DbCommand CreateDbCommand()
        {
            EnsureConnectionOpen();

            return new AdbcCommand(this.AdbcStatement, this);
        }

        /// <summary>
        /// Ensures the connection is open.
        /// </summary>
        private void EnsureConnectionOpen()
        {
            if (this.State == ConnectionState.Closed)
                this.Open();
        }

        protected override void Dispose(bool disposing)
        {
            this.adbcConnectionInternal?.Dispose();
            this.adbcConnectionInternal = null;

            base.Dispose(disposing);
        }

        public override void Open()
        {
            if (this.State == ConnectionState.Closed)
            {
                if (this.adbcConnectionParameters.Keys.Count == 0)
                {
                    throw new InvalidOperationException("No connection values are present to connect with");
                }

                if(this.AdbcDriver == null)
                {
                    throw new InvalidOperationException("The ADBC driver is not specified");
                }

                this.adbcDatabase = this.AdbcDriver.Open(this.adbcConnectionParameters);
                this.adbcConnectionInternal = this.adbcDatabase.Connect(this.adbcConnectionOptions);
            }
        }

        public override void Close()
        {
           this.Dispose();
        }

        public override ConnectionState State
        {
            get
            {
                return this.adbcConnectionInternal != null ? ConnectionState.Open : ConnectionState.Closed;
            }
        }

        /// <summary>
        /// Builds a connection string based on the adbcConnectionParameters.
        /// </summary>
        /// <returns>connection string</returns>
        private string GetConnectionString()
        {
            DbConnectionStringBuilder builder = new DbConnectionStringBuilder();

            foreach (string key in this.adbcConnectionParameters.Keys)
            {
                builder.Add(key, this.adbcConnectionParameters[key]);
            }

            return builder.ConnectionString;
        }

        /// <summary>
        /// Sets the adbcConnectionParameters based on a connection string.
        /// </summary>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException"></exception>
        private void SetConnectionProperties(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentNullException(nameof(value));
            }

            DbConnectionStringBuilder builder = new DbConnectionStringBuilder();
            builder.ConnectionString = value;

            this.adbcConnectionParameters.Clear();

            foreach(string key in builder.Keys)
            {
                this.adbcConnectionParameters.Add(key, Convert.ToString(builder[key]));
            }
        }

        public override DataTable GetSchema()
        {
            return GetSchema(null);
        }

        //GetSchema("TABLES")
        //GetSchema("VIEWS")

        public override DataTable GetSchema(string collectionName)
        {
            return GetSchema(collectionName, null);
        }

        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            Schema arrowSchema = this.adbcConnectionInternal.GetTableSchema("", "", "");
            return SchemaConverter.ConvertArrowSchema(arrowSchema, this.AdbcStatement);
        }

        #region NOT_IMPLEMENTED

        public override string Database => throw new NotImplementedException();

        public override string DataSource => throw new NotImplementedException();

        public override string ServerVersion => throw new NotImplementedException();

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}

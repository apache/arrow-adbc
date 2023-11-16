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
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using GetObjectsDepth = Apache.Arrow.Adbc.AdbcConnection.GetObjectsDepth;

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
            this.DecimalBehavior = DecimalBehavior.UseSqlDecimal;
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
        /// Overloaded. Initializes an <see cref="AdbcConnection"/>.
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
        /// Overloaded. Initializes an <see cref="AdbcConnection"/>.
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

        /// <summary>
        /// Gets or sets the behavior of decimals.
        /// </summary>
        public DecimalBehavior DecimalBehavior { get; set; }

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
            this.adbcStatement = null;

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

                if (this.AdbcDriver == null)
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

        private Adbc.AdbcConnection Connection => this.adbcConnectionInternal ?? throw new InvalidOperationException("Invalid operation. The connection is closed.");

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

            foreach (string key in builder.Keys)
            {
                this.adbcConnectionParameters.Add(key, Convert.ToString(builder[key]));
            }
        }

        public override DataTable GetSchema()
        {
            return GetSchema("metadatacollections", null);
        }

        public override DataTable GetSchema(string collectionName)
        {
            return GetSchema(collectionName, null);
        }

        public override DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            SchemaCollection collection;
            if (!SchemaCollection.TryGetCollection(collectionName, out collection))
            {
                throw new ArgumentException(
                    $"The requested collection ('{collectionName}') is not defined",
                    nameof(collectionName));
            }

            if (restrictionValues != null && restrictionValues.Length > collection.Restrictions.Length)
            {
                throw new ArgumentException(
                    $"More restrictions were provided than the requested schema ('{collectionName}') supports.",
                    nameof(restrictionValues));
            }

            return collection.GetSchema(this.Connection, restrictionValues);
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

#if false
        static DataTable Translate(IArrowArrayStream stream)
        {
            var result = new DataTable("Catalogs");
            result.Columns.Add("TABLE_CATALOG", typeof(string));

            while (true)
            {
                var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) { return result; }

                var catalogNames = (StringArray)batch.Column(0);
                for (int i = 0; i < catalogNames.Length; i++)
                {
                    result.Rows.Add(catalogNames.GetString(i));
                }
            }
        }

        static DataTable Translate2(IArrowArrayStream stream)
        {
            var result = new DataTable("Schemas");
            result.Columns.Add("TABLE_CATALOG", typeof(string));
            result.Columns.Add("TABLE_SCHEMA", typeof(string));

            while (true)
            {
                var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) { return result; }

                var catalogNames = (StringArray)batch.Column(0);
                var schemaIndexes = (ListArray)batch.Column(1);
                var schemas = (StructArray)schemaIndexes.Values;
                var schemaNames = (StringArray)schemas.Fields[0];
                int schemaIndex = 0;
                for (int i = 0; i < catalogNames.Length; i++)
                {
                    string catalog = catalogNames.GetString(i);
                    int schemaCount = schemaIndexes.GetValueLength(i);
                    for (int j = 0; j < schemaCount; j++)
                    {
                        result.Rows.Add(catalog, schemaNames.GetString(schemaIndex++));
                    }
                }
            }
        }

        static DataTable Translate3(IArrowArrayStream stream)
        {
            var result = new DataTable("Tables");
            result.Columns.Add("TABLE_CATALOG", typeof(string));
            result.Columns.Add("TABLE_SCHEMA", typeof(string));
            result.Columns.Add("TABLE_NAME", typeof(string));
            result.Columns.Add("TABLE_TYPE", typeof(string));

            while (true)
            {
                var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) { return result; }

                var catalogNames = (StringArray)batch.Column(0);
                var schemaIndexes = (ListArray)batch.Column(1);
                var schemas = (StructArray)schemaIndexes.Values;
                var schemaNames = (StringArray)schemas.Fields[0];
                var tableIndexes = (ListArray)schemas.Fields[1];
                var tables = (StructArray)tableIndexes.Values;
                var tableNames = (StringArray)tables.Fields[0];
                var tableTypes = (StringArray)tables.Fields[1];
                int schemaIndex = 0;
                int tableIndex = 0;
                for (int i = 0; i < catalogNames.Length; i++)
                {
                    string catalog = catalogNames.GetString(i);
                    int schemaCount = schemaIndexes.GetValueLength(i);
                    for (int j = 0; j < schemaCount; j++)
                    {
                        string schema = schemaNames.GetString(schemaIndex);
                        int tableCount = tableIndexes.GetValueLength(schemaIndex++);
                        for (int k = 0; k < tableCount; k++)
                        {
                            string table = tableNames.GetString(tableIndex);
                            string tableType = tableTypes.GetString(tableIndex++);
                            result.Rows.Add(catalog, schema, table, tableType);
                        }
                    }
                }
            }
        }

        static DataTable Translate4(IArrowArrayStream stream)
        {
            var result = new DataTable("Columns");
            result.Columns.Add("TABLE_CATALOG", typeof(string));
            result.Columns.Add("TABLE_SCHEMA", typeof(string));
            result.Columns.Add("TABLE_NAME", typeof(string));
            result.Columns.Add("COLUMN_NAME", typeof(string));
            result.Columns.Add("ORDINAL_POSITION", typeof(int));

            while (true)
            {
                var batch = stream.ReadNextRecordBatchAsync().Result;
                if (batch == null) { return result; }

                var catalogNames = (StringArray)batch.Column(0);
                var schemaIndexes = (ListArray)batch.Column(1);
                var schemas = (StructArray)schemaIndexes.Values;
                var schemaNames = (StringArray)schemas.Fields[0];
                var tableIndexes = (ListArray)schemas.Fields[1];
                var tables = (StructArray)tableIndexes.Values;
                var tableNames = (StringArray)tables.Fields[0];
                var columnIndexes = (ListArray)tables.Fields[2];
                var columns = (StructArray)columnIndexes.Values;
                var columnNames = (StringArray)columns.Fields[0];
                var ordinalPositions = (Int32Array)columns.Fields[1];
                int schemaIndex = 0;
                int tableIndex = 0;
                int columnIndex = 0;
                for (int i = 0; i < catalogNames.Length; i++)
                {
                    string catalog = catalogNames.GetString(i);
                    int schemaCount = schemaIndexes.GetValueLength(i);
                    for (int j = 0; j < schemaCount; j++)
                    {
                        string schema = schemaNames.GetString(schemaIndex);
                        int tableCount = tableIndexes.GetValueLength(schemaIndex++);
                        for (int k = 0; k < tableCount; k++)
                        {
                            string table = tableNames.GetString(tableIndex);
                            int columnCount = columnIndexes.GetValueLength(tableIndex++);
                            for (int l = 0; l < columnCount; l++)
                            {
                                string column = columnNames.GetString(columnIndex);
                                int? ordinalPosition = ordinalPositions.GetValue(columnIndex++);
                                result.Rows.Add(catalog, schema, table, column, ordinalPosition);
                            }
                        }
                    }
                }
            }
        }
#endif

        abstract class SchemaCollection
        {
            protected static readonly List<SchemaCollection> collections;
            static readonly SortedDictionary<string, SchemaCollection> schemaCollections;

            static SchemaCollection()
            {
                schemaCollections = new SortedDictionary<string, SchemaCollection>(StringComparer.OrdinalIgnoreCase);
                Add(new MetadataCollection());
                Add(new RestrictionsCollection());
                Add(new CatalogsCollection());
                Add(new SchemasCollection());
                Add(new TablesCollection());
                Add(new ColumnsCollection());
                Add(new PrimaryKeysCollection());
                Add(new ForeignKeysCollection());
            }

            static void Add(SchemaCollection collection)
            {
                collections.Add(collection);
                schemaCollections.Add(collection.Name, collection);
            }

            public static bool TryGetCollection(string name, out SchemaCollection collection)
            {
                return schemaCollections.TryGetValue(name, out collection);
            }

            public abstract string Name { get; }
            public abstract string[] Restrictions { get; }

            public abstract DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string[] restrictions);
        }

        sealed class MetadataCollection : SchemaCollection
        {
            public override string Name => "MetaDataCollections";
            public override string[] Restrictions => new string[0];

            public override DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string[] restrictions)
            {
                var result = new DataTable(Name);
                result.Columns.Add("CollectionName", typeof(string));
                result.Columns.Add("NumberOfRestrictions", typeof(int));

                foreach (var collection in collections)
                {
                    result.Rows.Add(collection.Name, collection.Restrictions.Length);
                }

                return result;
            }
        }

        sealed class RestrictionsCollection : SchemaCollection
        {
            public override string Name => "Restrictions";
            public override string[] Restrictions => new string[0];

            public override DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string[] restrictions)
            {
                var result = new DataTable(Name);
                result.Columns.Add("CollectionName", typeof(string));
                result.Columns.Add("RestrictionName", typeof(string));
                result.Columns.Add("RestrictionNumber", typeof(int));

                foreach (var collection in collections)
                {
                    var collectionRestrictions = collection.Restrictions;
                    for (int i = 0; i < collectionRestrictions.Length; i++)
                    {
                        result.Rows.Add(collection.Name, collectionRestrictions[i], i + 1);
                    }
                }

                return result;
            }
        }

        abstract class ArrowCollection : SchemaCollection
        {
            protected abstract MapItem[] Map { get; }

            protected abstract IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions);

            public override DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string[] restrictions)
            {
                // Flattens the hierarchical ADBC schema into a DataTable

                var map = this.Map;
                var result = new DataTable(Name);
                List<string> path = new List<string>();
                int[] counters = null;
                foreach (MapItem item in map)
                {
                    result.Columns.Add(item.AdoName, item.Type);

                    for (int i = 1; i < item.AdbcPath.Length; i++)
                    {
                        if (path.Count < i)
                        {
                            path.Add(item.AdbcPath[i]);
                        }
                        else if (!StringComparer.OrdinalIgnoreCase.Equals(path[i - 1], item.AdbcPath[i]))
                        {
                            throw new InvalidOperationException($"expected '{path[i = 1]}' found '{item.AdbcPath[i]}'");
                        }
                    }
                }

                // counters = new int[maxDepth];

                using (var stream = Invoke(adbcConnection, restrictions))
                {
                    while (true)
                    {
                        using (var batch = stream.ReadNextRecordBatchAsync().Result)
                        {
                            if (batch == null) { return result; }


                        }
                    }
                }
            }

            Func<RecordBatch, int[], object> Something(string[] parts, Type type)
            {
                int last = parts.Length - 1;
                Func<IArrowArray, int[], object> valueGetter;
                switch (Type.GetTypeCode(type))
                {
                    case TypeCode.Boolean:
                        valueGetter = (array, index) => ((BooleanArray)array).GetValue(index[last]);
                        break;
                    case TypeCode.Int16:
                        valueGetter = (array, index) => ((Int16Array)array).GetValue(index[last]);
                        break;
                    case TypeCode.Int32:
                        valueGetter = (array, index) => ((Int32Array)array).GetValue(index[last]);
                        break;
                    case TypeCode.Int64:
                        valueGetter = (array, index) => ((Int64Array)array).GetValue(index[last]);
                        break;
                    case TypeCode.String:
                        valueGetter = (array, index) => ((StringArray)array).GetString(index[last]);
                        break;
                    default:
                        throw new NotSupportedException($"Type {type.FullName} is not supported.");
                }

                for (int i = parts.Length - 1; i > 0; i--)
                {
                    valueGetter = (array, index) => 
                }

                while (parts)
            }

            protected struct MapItem
            {
                public readonly string AdoName;
                public readonly string[] AdbcPath;
                public readonly Type Type;

                public MapItem(string adoName, string[] adbcPath, Type type)
                {
                    this.AdoName = adoName;
                    this.AdbcPath = adbcPath;
                    this.Type = type;
                }
            }
        }

        sealed class CatalogsCollection : ArrowCollection
        {
            public override string Name => "Catalogs";
            public override string[] Restrictions => new[] { "Catalog" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new[] { "catalog_name" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions)
            {
                string catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                return connection.GetObjects(GetObjectsDepth.Catalogs, catalog, null, null, null, null);
            }
        }


        class SchemasCollection : ArrowCollection
        {
            public override string Name => "Schemas";
            public override string[] Restrictions => new[] { "Catalog", "Schema" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new [] { "catalog_name" }, typeof(string)),
                new MapItem("TABLE_SCHEMA", new [] { "catalog_db_schemas", "db_schema_name" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions)
            {
                string catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string schema = restrictions?.Length > 1 ? restrictions[1] : null;
                return connection.GetObjects(GetObjectsDepth.DbSchemas, catalog, schema, null, null, null);
            }
        }

        class TableTypesCollection : ArrowCollection
        {
            public override string Name => "TableTypes";
            public override string[] Restrictions => new string[0];

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_TYPE", new [] { "table_type" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions)
            {
                return connection.GetTableTypes();
            }
        }

        class TablesCollection : ArrowCollection
        {
            public override string Name => "Tables";
            public override string[] Restrictions => new[] { "Catalog", "Schema", "Table", "TableType" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new [] { "catalog_name" }, typeof(string)),
                new MapItem("TABLE_SCHEMA", new [] { "catalog_db_schemas", "db_schema_name" }, typeof(string)),
                new MapItem("TABLE_NAME", new [] { "catalog_db_schemas", "db_schema_tables", "table_name" }, typeof(string)),
                new MapItem("TABLE_TYPE", new [] { "catalog_db_schemas", "db_schema_tables", "table_type" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions)
            {
                string catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string schema = restrictions?.Length > 1 ? restrictions[1] : null;
                string table = restrictions?.Length > 2 ? restrictions[2] : null;
                List<string> tableTypes = restrictions?.Length > 3 ? restrictions[3].Split(',').ToList() : null;
                return connection.GetObjects(GetObjectsDepth.Tables, catalog, schema, table, tableTypes, null);
            }
        }

        class ColumnsCollection : ArrowCollection
        {
            public override string Name => "Columns";
            public override string[] Restrictions => new[] { "Catalog", "Schema", "Table", "Column" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new [] { "catalog_name" }, typeof(string)),
                new MapItem("TABLE_SCHEMA", new [] { "catalog_db_schemas", "db_schema_name" }, typeof(string)),
                new MapItem("TABLE_NAME", new [] { "catalog_db_schemas", "db_schema_tables", "table_name" }, typeof(string)),
                new MapItem("COLUMN_NAME", new [] { "catalog_db_schemas", "db_schema_tables", "table_name" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions)
            {
                string catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string schema = restrictions?.Length > 1 ? restrictions[1] : null;
                string table = restrictions?.Length > 2 ? restrictions[2] : null;
                string column = restrictions?.Length > 3 ? restrictions[3] : null;
                return connection.GetObjects(GetObjectsDepth.All, catalog, schema, table, null, column);
            }
        }

        class PrimaryKeysCollection : ArrowCollection
        {
            public override string Name => "PrimaryKeys";
            public override string[] Restrictions => new[] { "Catalog", "Schema", "Table" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new [] { "catalog_name" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions)
            {
                string catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string schema = restrictions?.Length > 1 ? restrictions[1] : null;
                string table = restrictions?.Length > 2 ? restrictions[2] : null;
                return connection.GetObjects(GetObjectsDepth.All, catalog, schema, table, null, null);
            }
        }

        class ForeignKeysCollection : ArrowCollection
        {
            public override string Name => "ForeignKeys";
            public override string[] Restrictions => new[] { "Catalog", "Schema", "Table" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new [] { "catalog_name" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string[] restrictions)
            {
                string catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string schema = restrictions?.Length > 1 ? restrictions[1] : null;
                string table = restrictions?.Length > 2 ? restrictions[2] : null;
                return connection.GetObjects(GetObjectsDepth.All, catalog, schema, table, null, null);
            }
        }
    }
}

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
using System.Diagnostics.CodeAnalysis;
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
        private AdbcDatabase? adbcDatabase;
        private Adbc.AdbcConnection? adbcConnectionInternal;

        private readonly Dictionary<string, string> adbcConnectionParameters;
        private readonly Dictionary<string, string> adbcConnectionOptions;

        private AdbcTransaction? currentTransaction;

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

        // For testing
        internal AdbcConnection(AdbcDriver driver, AdbcDatabase database, Adbc.AdbcConnection connection)
            : this(driver)
        {
            this.adbcDatabase = database;
            this.adbcConnectionInternal = connection;
        }

        /// <summary>
        /// Creates a new <see cref="AdbcCommand"/>.
        /// </summary>
        /// <returns><see cref="AdbcCommand"/></returns>
        public new AdbcCommand CreateCommand() => (AdbcCommand)CreateDbCommand();

        /// <summary>
        /// Gets or sets the <see cref="AdbcDriver"/> associated with this
        /// connection.
        /// </summary>
        public AdbcDriver? AdbcDriver { get; set; }

        /// <summary>
        /// Creates an <see cref="AdbcStatement"/> for the connection.
        /// </summary>
        internal AdbcStatement CreateStatement()
        {
            EnsureConnectionOpen();
            return this.adbcConnectionInternal!.CreateStatement();
        }

#if NET5_0_OR_GREATER
        [AllowNull]
#endif
        public override string ConnectionString { get => GetConnectionString(); set => SetConnectionProperties(value!); }

        /// <summary>
        /// Gets or sets the behavior of decimals.
        /// </summary>
        public DecimalBehavior DecimalBehavior { get; set; }

        protected override DbCommand CreateDbCommand()
        {
            EnsureConnectionOpen();

            return new AdbcCommand(this);
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
            if (this.currentTransaction != null)
            {
                this.currentTransaction.Rollback();
            }
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
                object? builderValue = builder[key];
                if (builderValue != null)
                {
                    this.adbcConnectionParameters.Add(key, Convert.ToString(builderValue)!);
                }
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

        public override DataTable GetSchema(string collectionName, string?[]? restrictionValues)
        {
            SchemaCollection? collection;
            if (!SchemaCollection.TryGetCollection(collectionName, out collection))
            {
                throw new ArgumentException(
                    $"The requested collection ('{collectionName}') is not defined",
                    nameof(collectionName));
            }

            if (restrictionValues != null && restrictionValues.Length > collection!.Restrictions.Length)
            {
                throw new ArgumentException(
                    $"More restrictions were provided than the requested schema ('{collectionName}') supports.",
                    nameof(restrictionValues));
            }

            return collection!.GetSchema(this.Connection, restrictionValues);
        }

        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel)
        {
            if (this.currentTransaction != null) throw new InvalidOperationException("connection is already enlisted in a transaction");

            this.Connection.AutoCommit = false;

            if (isolationLevel != System.Data.IsolationLevel.Unspecified)
            {
                this.Connection.IsolationLevel = GetIsolationLevel(isolationLevel);
            }

            this.currentTransaction = new AdbcTransaction(this, isolationLevel);
            return this.currentTransaction;
        }

        private static Adbc.IsolationLevel GetIsolationLevel(System.Data.IsolationLevel isolationLevel)
        {
            return isolationLevel switch
            {
                System.Data.IsolationLevel.Unspecified => Adbc.IsolationLevel.Default,
                System.Data.IsolationLevel.ReadUncommitted => Adbc.IsolationLevel.ReadUncommitted,
                System.Data.IsolationLevel.ReadCommitted => Adbc.IsolationLevel.ReadCommitted,
                System.Data.IsolationLevel.RepeatableRead => Adbc.IsolationLevel.RepeatableRead,
                System.Data.IsolationLevel.Snapshot => Adbc.IsolationLevel.Snapshot,
                System.Data.IsolationLevel.Serializable => Adbc.IsolationLevel.Serializable,
                _ => throw new NotSupportedException("unknown isolation level"),
            };
        }

        private void Commit()
        {
            if (this.currentTransaction == null) throw new InvalidOperationException("connection is not enlisted in a transaction");
            System.Data.IsolationLevel isolationLevel = this.currentTransaction.IsolationLevel;

            this.Connection.Commit();

            this.currentTransaction = null;
            this.Connection.AutoCommit = true;
            if (isolationLevel != System.Data.IsolationLevel.Unspecified)
            {
                this.adbcConnectionInternal!.IsolationLevel = IsolationLevel.Default;
            }
        }

        private void Rollback()
        {
            if (this.currentTransaction == null) throw new InvalidOperationException("connection is not enlisted in a transaction");
            System.Data.IsolationLevel isolationLevel = this.currentTransaction.IsolationLevel;

            this.Connection.Rollback();

            this.currentTransaction = null;
            this.Connection.AutoCommit = true;
            if (isolationLevel != System.Data.IsolationLevel.Unspecified)
            {
                this.adbcConnectionInternal!.IsolationLevel = IsolationLevel.Default;
            }
        }

        #region NOT_IMPLEMENTED

        public override string Database => throw new NotImplementedException();

        public override string DataSource => throw new NotImplementedException();

        public override string ServerVersion => throw new NotImplementedException();

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        #endregion

        sealed class AdbcTransaction : DbTransaction
        {
            readonly AdbcConnection connection;
            readonly System.Data.IsolationLevel isolationLevel;

            public AdbcTransaction(AdbcConnection connection, System.Data.IsolationLevel isolationLevel)
            {
                this.connection = connection;
                this.isolationLevel = isolationLevel;
            }

            public override System.Data.IsolationLevel IsolationLevel => this.isolationLevel;

            protected override DbConnection DbConnection => this.connection;

            public override void Commit() => this.connection.Commit();
            public override void Rollback() => this.connection.Rollback();
        }

        abstract class SchemaCollection
        {
            protected static readonly List<SchemaCollection> collections;
            private static readonly SortedDictionary<string, SchemaCollection> schemaCollections;

            static SchemaCollection()
            {
                collections = new List<SchemaCollection>();
                schemaCollections = new SortedDictionary<string, SchemaCollection>(StringComparer.OrdinalIgnoreCase);

                Add(new MetadataCollection());
                Add(new RestrictionsCollection());
                Add(new CatalogsCollection());
                Add(new SchemasCollection());
                Add(new TableTypesCollection());
                Add(new TablesCollection());
                Add(new ColumnsCollection());
            }

            private static void Add(SchemaCollection collection)
            {
                collections.Add(collection);
                schemaCollections.Add(collection.Name, collection);
            }

            public static bool TryGetCollection(string name, out SchemaCollection? collection)
            {
                return schemaCollections.TryGetValue(name, out collection);
            }

            public abstract string Name { get; }
            public abstract string[] Restrictions { get; }

            public abstract DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string?[]? restrictions);
        }

        private sealed class MetadataCollection : SchemaCollection
        {
            public override string Name => "MetaDataCollections";
            public override string[] Restrictions => [];

            public override DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string?[]? restrictions)
            {
                DataTable result = new DataTable(Name);
                result.Columns.Add("CollectionName", typeof(string));
                result.Columns.Add("NumberOfRestrictions", typeof(int));

                foreach (SchemaCollection collection in collections)
                {
                    result.Rows.Add(collection.Name, collection.Restrictions.Length);
                }

                return result;
            }
        }

        private sealed class RestrictionsCollection : SchemaCollection
        {
            public override string Name => "Restrictions";
            public override string[] Restrictions => [];

            public override DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string?[]? restrictions)
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

        private abstract class ArrowCollection : SchemaCollection
        {
            protected abstract MapItem[] Map { get; }

            protected abstract IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string?[]? restrictions);

            public override DataTable GetSchema(Adbc.AdbcConnection adbcConnection, string?[]? restrictions)
            {
                // Flattens the hierarchical ADBC schema into a DataTable

                using (IArrowArrayStream stream = Invoke(adbcConnection, restrictions))
                {
                    MapItem[] map = this.Map;
                    DataTable result = new DataTable(Name);
                    List<int> indices = new List<int>();
                    List<IRecordType> types = new List<IRecordType>();
                    List<string> path = new List<string>();
                    List<Action<State>> loaders = new List<Action<State>>();

                    types.Add(stream.Schema);

                    for (int targetIndex = 0; targetIndex < map.Length; targetIndex++)
                    {
                        MapItem item = map[targetIndex];
                        result.Columns.Add(item.AdoName, item.Type);

                        for (int i = 0; i < item.AdbcPath.Length - 1; i++)
                        {
                            string part = item.AdbcPath[i];
                            if (i == path.Count)
                            {
                                int index = types[i].GetFieldIndex(part, null);
                                if (index < 0)
                                {
                                    throw new InvalidOperationException($"Unable to find '{part}'");
                                }
                                ListType? listType = types[i].GetFieldByIndex(index).DataType as ListType;
                                if (listType == null || listType.ValueDataType.TypeId != ArrowTypeId.Struct)
                                {
                                    throw new InvalidOperationException($"Field '{part}' has unexpected type.");
                                }

                                path.Add(part);
                                indices.Add(index);
                                types.Add((IRecordType)listType.ValueDataType);
                            }
                            else if (!StringComparer.OrdinalIgnoreCase.Equals(path[i], part))
                            {
                                throw new InvalidOperationException($"expected '{path[i]}' found '{part}'");
                            }
                        }

                        int srcIndex = types[types.Count - 1].GetFieldIndex(item.AdbcPath[item.AdbcPath.Length - 1], null);
                        if (srcIndex < 0)
                        {
                            throw new InvalidOperationException($"Unable to find '{item.AdbcPath[item.AdbcPath.Length - 1]}'");
                        }
                        loaders.Add(State.CreateLoader(item.Type, item.AdbcPath.Length - 1, srcIndex, targetIndex));
                    }

                    State state = new State(result, indices.ToArray(), loaders.ToArray());
                    while (true)
                    {
                        using (RecordBatch? batch = stream.ReadNextRecordBatchAsync().Result)
                        {
                            if (batch == null) { return result; }

                            state.AddRecords(batch);
                        }
                    }
                }
            }

            private class State
            {
                private readonly DataTable table;
                private readonly int[] indices;
                private readonly Action<State>[] loaders;
                private readonly object?[] buffer;
                private readonly int[] offsets;
                private readonly IArrowRecord[] records;

                public State(DataTable table, int[] indices, Action<State>[] loaders)
                {
                    this.table = table;
                    this.indices = indices;
                    this.loaders = loaders;
                    this.buffer = new object[loaders.Length];

                    this.offsets = new int[indices.Length + 1];
                    this.records = new IArrowRecord[indices.Length + 1];
                }

                public void AddRecords(RecordBatch batch)
                {
                    ListArray[] lists = new ListArray[this.indices.Length];

                    this.records[0] = batch;
                    this.offsets[0] = 0;
                    for (int i = 0; i < indices.Length; i++)
                    {
                        lists[i] = (ListArray)this.records[i].Column(indices[i]);
                        this.records[i + 1] = (StructArray)lists[i].Values;
                        this.offsets[i + 1] = 0;
                    }

                    Loop(lists, 0, batch.Length);
                }

                private void Loop(ListArray[] lists, int ptr, int count)
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (ptr == lists.Length)
                        {
                            AddRow();
                        }
                        else
                        {
                            Loop(lists, ptr + 1, lists[ptr].GetValueLength(this.offsets[ptr]));
                        }
                        this.offsets[ptr]++;
                    }
                }

                private void AddRow()
                {
                    foreach (Action<State> loader in this.loaders)
                    {
                        loader(this);
                    }
                    this.table.Rows.Add(this.buffer);
                }

                public static Action<State> CreateLoader(Type type, int srcLevel, int srcIndex, int targetIndex)
                {
                    return Type.GetTypeCode(type) switch
                    {
                        TypeCode.Boolean => state =>
                            state.buffer[targetIndex] = ((BooleanArray)state.records[srcLevel].Column(srcIndex)).GetValue(state.offsets[srcLevel]),
                        TypeCode.Int16 => state =>
                            state.buffer[targetIndex] = ((Int16Array)state.records[srcLevel].Column(srcIndex)).GetValue(state.offsets[srcLevel]),
                        TypeCode.Int32 => state =>
                            state.buffer[targetIndex] = ((Int32Array)state.records[srcLevel].Column(srcIndex)).GetValue(state.offsets[srcLevel]),
                        TypeCode.Int64 => state =>
                            state.buffer[targetIndex] = ((Int64Array)state.records[srcLevel].Column(srcIndex)).GetValue(state.offsets[srcLevel]),
                        TypeCode.String => state =>
                            state.buffer[targetIndex] = ((StringArray)state.records[srcLevel].Column(srcIndex)).GetString(state.offsets[srcLevel]),
                        _ => throw new NotSupportedException($"Type {type.FullName} is not supported."),
                    };
                }
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

        private sealed class CatalogsCollection : ArrowCollection
        {
            public override string Name => "Catalogs";
            public override string[] Restrictions => new[] { "Catalog" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new[] { "catalog_name" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string?[]? restrictions)
            {
                string? catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                return connection.GetObjects(GetObjectsDepth.Catalogs, catalog, null, null, null, null);
            }
        }

        private class SchemasCollection : ArrowCollection
        {
            public override string Name => "Schemas";
            public override string[] Restrictions => new[] { "Catalog", "Schema" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new [] { "catalog_name" }, typeof(string)),
                new MapItem("TABLE_SCHEMA", new [] { "catalog_db_schemas", "db_schema_name" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string?[]? restrictions)
            {
                string? catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string? schema = restrictions?.Length > 1 ? restrictions[1] : null;
                return connection.GetObjects(GetObjectsDepth.DbSchemas, catalog, schema, null, null, null);
            }
        }

        private class TableTypesCollection : ArrowCollection
        {
            public override string Name => "TableTypes";
            public override string[] Restrictions => [];

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_TYPE", new [] { "table_type" }, typeof(string)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string?[]? restrictions)
            {
                return connection.GetTableTypes();
            }
        }

        private class TablesCollection : ArrowCollection
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

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string?[]? restrictions)
            {
                string? catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string? schema = restrictions?.Length > 1 ? restrictions[1] : null;
                string? table = restrictions?.Length > 2 ? restrictions[2] : null;
                List<string>? tableTypes = restrictions?.Length > 3 ? restrictions[3]?.Split(',').ToList() : null;
                return connection.GetObjects(GetObjectsDepth.Tables, catalog, schema, table, tableTypes, null);
            }
        }

        private class ColumnsCollection : ArrowCollection
        {
            public override string Name => "Columns";
            public override string[] Restrictions => new[] { "Catalog", "Schema", "Table", "Column" };

            protected override MapItem[] Map => new[]
            {
                new MapItem("TABLE_CATALOG", new [] { "catalog_name" }, typeof(string)),
                new MapItem("TABLE_SCHEMA", new [] { "catalog_db_schemas", "db_schema_name" }, typeof(string)),
                new MapItem("TABLE_NAME", new [] { "catalog_db_schemas", "db_schema_tables", "table_name" }, typeof(string)),
                new MapItem("COLUMN_NAME", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "column_name" }, typeof(string)),
                new MapItem("ORDINAL_POSITION", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "ordinal_position" }, typeof(int)),
                new MapItem("REMARKS", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "remarks" }, typeof(string)),
                new MapItem("DATA_TYPE", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_type_name" }, typeof(string)),
                new MapItem("IS_NULLABLE", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_is_nullable" }, typeof(string)),
                new MapItem("COLUMN_DEFAULT", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_column_def" }, typeof(string)),
                new MapItem("IS_AUTOINCREMENT", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_is_autoincrement" }, typeof(bool)),
                new MapItem("IS_GENERATED", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_is_generatedcolumn" }, typeof(bool)),
                new MapItem("CHARACTER_OCTET_LENGTH", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_char_octet_length" }, typeof(int)),
                new MapItem("CHARACTER_MAXIMUM_LENGTH", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_column_size" }, typeof(int)),
                new MapItem("NUMERIC_PRECISION", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_decimal_digits" }, typeof(short)),
                new MapItem("NUMERIC_PRECISION_RADIX", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_num_prec_radix" }, typeof(short)),
                new MapItem("DATETIME_PRECISION", new [] { "catalog_db_schemas", "db_schema_tables", "table_columns", "xdbc_datetime_sub" }, typeof(short)),
            };

            protected override IArrowArrayStream Invoke(Adbc.AdbcConnection connection, string?[]? restrictions)
            {
                string? catalog = restrictions?.Length > 0 ? restrictions[0] : null;
                string? schema = restrictions?.Length > 1 ? restrictions[1] : null;
                string? table = restrictions?.Length > 2 ? restrictions[2] : null;
                string? column = restrictions?.Length > 3 ? restrictions[3] : null;
                return connection.GetObjects(GetObjectsDepth.All, catalog, schema, table, null, column);
            }
        }
    }
}

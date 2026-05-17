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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.TestFixture.Native
{
    /// <summary>
    /// Managed driver exposed across the C ABI by the AOT-published shared library.
    ///
    /// <para>The driver is deliberately deterministic and self-contained so that the
    /// consumer test process — which loads this library through
    /// <c>CAdbcDriverImporter.Load(path)</c> — can assert on exact schemas and values
    /// without needing any external state. Every method returns or echoes data that
    /// makes a particular C-ABI call observable from the importer side.</para>
    /// </summary>
    internal sealed class FixtureDriver : AdbcDriver
    {
        public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
            => new FixtureDatabase(parameters);
    }

    internal sealed class FixtureDatabase : AdbcDatabase
    {
        private readonly Dictionary<string, string> _options;

        public FixtureDatabase(IReadOnlyDictionary<string, string> parameters)
        {
            _options = new Dictionary<string, string>(parameters.Count);
            foreach (KeyValuePair<string, string> pair in parameters)
            {
                _options.Add(pair.Key, pair.Value);
            }
        }

        public override void SetOption(string key, string value) => _options[key] = value;

        public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
        {
            // Connection-scoped options (set via ConnectionSetOption before init)
            // arrive here in `options`. Merge them into the option bag the connection
            // exposes so the consumer can see that ConnectionSetOption propagated.
            var merged = new Dictionary<string, string>(_options);
            if (options != null)
            {
                foreach (KeyValuePair<string, string> pair in options)
                {
                    merged[pair.Key] = pair.Value;
                }
            }
            return new FixtureConnection(merged);
        }
    }

    internal sealed class FixtureConnection : AdbcConnection
    {
        private readonly Dictionary<string, string> _options;
        private bool _cancelled;

        public FixtureConnection(Dictionary<string, string> options) { _options = options; }

        public override void SetOption(string key, string value) => _options[key] = value;

        public override void Cancel() => _cancelled = true;

        // Echoes the driver options under a fixed schema so the consumer can assert
        // that DatabaseSetOption and ConnectionSetOption propagated across the ABI.
        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern,
            string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
            => throw AdbcException.NotImplemented("fixture does not support GetObjects");

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
            => throw AdbcException.NotImplemented("fixture does not support GetTableSchema");

        public override IArrowArrayStream GetTableTypes()
        {
            var schema = new Schema.Builder()
                .Field(f => f.Name("table_type").DataType(StringType.Default).Nullable(false))
                .Build();
            var values = new StringArray.Builder().Append("TABLE").Append("VIEW").Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { values }, 2);
            return new SingleBatchStream(schema, batch);
        }

        public override AdbcStatement CreateStatement() => new FixtureStatement(this, _options);

        internal bool WasCancelled => _cancelled;
    }

    internal sealed class FixtureStatement : AdbcStatement
    {
        private readonly FixtureConnection _connection;
        private readonly Dictionary<string, string> _connectionOptions;
        private readonly Dictionary<string, string> _statementOptions = new Dictionary<string, string>();
        private string? _sqlQuery;
        private bool _cancelled;
        private bool _executed;

        public FixtureStatement(FixtureConnection connection, Dictionary<string, string> connectionOptions)
        {
            _connection = connection;
            _connectionOptions = connectionOptions;
        }

        public override string? SqlQuery
        {
            get => _sqlQuery;
            set => _sqlQuery = value;
        }

        public override void SetOption(string key, string value) => _statementOptions[key] = value;

        public override void Cancel() => _cancelled = true;

        // Returns a fixed schema independent of the query. ExecuteSchema must NOT
        // execute the query, so the schema is computable without side effects.
        public override Schema ExecuteSchema() => ResultSchema();

        public override QueryResult ExecuteQuery()
        {
            _executed = true;

            // The result encodes the inputs the C ABI carried into this method so the
            // consumer can assert that SqlQuery, statement options, and database/
            // connection options all round-tripped.
            Schema schema = ResultSchema();
            var keys = new StringArray.Builder();
            var values = new StringArray.Builder();

            keys.Append("sql"); values.Append(_sqlQuery ?? string.Empty);
            keys.Append("stmt_option_count"); values.Append(_statementOptions.Count.ToString());
            keys.Append("db_option_count"); values.Append(_connectionOptions.Count.ToString());

            // Surface user-supplied options verbatim. Sort for stability.
            string[] stmtKeys = SortedKeys(_statementOptions);
            for (int i = 0; i < stmtKeys.Length; i++)
            {
                keys.Append("stmt:" + stmtKeys[i]);
                values.Append(_statementOptions[stmtKeys[i]]);
            }
            string[] dbKeys = SortedKeys(_connectionOptions);
            for (int i = 0; i < dbKeys.Length; i++)
            {
                keys.Append("db:" + dbKeys[i]);
                values.Append(_connectionOptions[dbKeys[i]]);
            }

            StringArray keyArray = keys.Build();
            StringArray valueArray = values.Build();
            var batch = new RecordBatch(schema, new IArrowArray[] { keyArray, valueArray }, keyArray.Length);
            return new QueryResult(keyArray.Length, new SingleBatchStream(schema, batch));
        }

        public override UpdateResult ExecuteUpdate()
            => throw AdbcException.NotImplemented("fixture does not support ExecuteUpdate");

        private static Schema ResultSchema() => new Schema.Builder()
            .Field(f => f.Name("key").DataType(StringType.Default).Nullable(false))
            .Field(f => f.Name("value").DataType(StringType.Default).Nullable(true))
            .Build();

        private static string[] SortedKeys(Dictionary<string, string> dict)
        {
            var keys = new List<string>(dict.Count);
            foreach (KeyValuePair<string, string> pair in dict)
            {
                keys.Add(pair.Key);
            }
            keys.Sort(StringComparer.Ordinal);
            return keys.ToArray();
        }

        internal bool WasCancelled => _cancelled;
        internal bool WasExecuted => _executed;
    }

    /// <summary>
    /// Single-batch IArrowArrayStream used to return deterministic data.
    /// </summary>
    internal sealed class SingleBatchStream : IArrowArrayStream
    {
        private readonly Schema _schema;
        private RecordBatch? _batch;

        public SingleBatchStream(Schema schema, RecordBatch batch)
        {
            _schema = schema;
            _batch = batch;
        }

        public Schema Schema => _schema;

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? result = _batch;
            _batch = null;
            return new ValueTask<RecordBatch?>(result);
        }

        public void Dispose() { _batch?.Dispose(); _batch = null; }
    }
}

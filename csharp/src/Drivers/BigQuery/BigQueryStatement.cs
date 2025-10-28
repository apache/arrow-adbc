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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Cloud.BigQuery.V2;
using TableFieldSchema = Google.Apis.Bigquery.v2.Data.TableFieldSchema;
using TableSchema = Google.Apis.Bigquery.v2.Data.TableSchema;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    /// <summary>
    /// BigQuery-specific implementation of <see cref="AdbcStatement"/>
    /// </summary>
    class BigQueryStatement : TracingStatement, ITokenProtectedResource, IDisposable
    {
        readonly BigQueryConnection bigQueryConnection;

        public BigQueryStatement(BigQueryConnection bigQueryConnection) : base(bigQueryConnection)
        {
            if (bigQueryConnection == null) { throw new AdbcException($"{nameof(bigQueryConnection)} cannot be null", AdbcStatusCode.InvalidArgument); }

            // pass on the handler since this isn't accessible publicly
            UpdateToken = bigQueryConnection.UpdateToken;

            this.bigQueryConnection = bigQueryConnection;
        }

        public Func<Task>? UpdateToken { get; set; }

        internal Dictionary<string, string>? Options { get; set; }

        private BigQueryClient Client => this.bigQueryConnection.Client ?? throw new AdbcException("Client cannot be null");

        private GoogleCredential Credential => this.bigQueryConnection.Credential ?? throw new AdbcException("Credential cannot be null");

        private int MaxRetryAttempts => this.bigQueryConnection.MaxRetryAttempts;

        private int RetryDelayMs => this.bigQueryConnection.RetryDelayMs;

        public override string AssemblyVersion => BigQueryUtils.BigQueryAssemblyVersion;

        public override string AssemblyName => BigQueryUtils.BigQueryAssemblyName;

        public override void SetOption(string key, string value)
        {
            if (Options == null)
            {
                Options = new Dictionary<string, string>();
            }

            Options[key] = value;
        }

        public override QueryResult ExecuteQuery()
        {
            return ExecuteQueryInternalAsync().GetAwaiter().GetResult();
        }

        private async Task<QueryResult> ExecuteQueryInternalAsync()
        {
            return await this.TraceActivityAsync(async activity =>
            {
                QueryOptions queryOptions = ValidateOptions(activity);

                activity?.AddConditionalTag(SemanticConventions.Db.Query.Text, SqlQuery, this.bigQueryConnection.IsSafeToTrace);

                BigQueryJob job = await Client.CreateQueryJobAsync(SqlQuery, null, queryOptions);
                JobReference jobReference = job.Reference;
                GetQueryResultsOptions getQueryResultsOptions = new GetQueryResultsOptions();

                if (Options?.TryGetValue(BigQueryParameters.GetQueryResultsOptionsTimeout, out string? timeoutSeconds) == true &&
                    int.TryParse(timeoutSeconds, out int seconds) &&
                    seconds >= 0)
                {
                    getQueryResultsOptions.Timeout = TimeSpan.FromSeconds(seconds);
                    activity?.AddBigQueryParameterTag(BigQueryParameters.GetQueryResultsOptionsTimeout, seconds);
                }

                // We can't checkJobStatus, Otherwise, the timeout in QueryResultsOptions is meaningless.
                // When encountering a long-running job, it should be controlled by the timeout in the Google SDK instead of blocking in a while loop.
                Func<Task<BigQueryResults>> getJobResults = async () =>
                {
                    // if the authentication token was reset, then we need a new job with the latest token
                    BigQueryJob completedJob = await Client.GetJobAsync(jobReference);
                    return await completedJob.GetQueryResultsAsync(getQueryResultsOptions);
                };

                BigQueryResults results = await ExecuteWithRetriesAsync(getJobResults, activity);

                TokenProtectedReadClientManger clientMgr = new TokenProtectedReadClientManger(Credential);
                clientMgr.UpdateToken = () => Task.Run(() =>
                {
                    this.bigQueryConnection.SetCredential();
                    clientMgr.UpdateCredential(Credential);
                });

                // For multi-statement queries, StatementType == "SCRIPT"
                if (results.TableReference == null || job.Statistics.Query.StatementType.Equals("SCRIPT", StringComparison.OrdinalIgnoreCase))
                {
                    string statementType = string.Empty;
                    if (Options?.TryGetValue(BigQueryParameters.StatementType, out string? statementTypeString) == true)
                    {
                        statementType = statementTypeString;
                    }
                    int statementIndex = 1;
                    if (Options?.TryGetValue(BigQueryParameters.StatementIndex, out string? statementIndexString) == true &&
                        int.TryParse(statementIndexString, out int statementIndexInt) &&
                        statementIndexInt > 0)
                    {
                        statementIndex = statementIndexInt;
                    }
                    string evaluationKind = string.Empty;
                    if (Options?.TryGetValue(BigQueryParameters.EvaluationKind, out string? evaluationKindString) == true)
                    {
                        evaluationKind = evaluationKindString;
                    }

                    Func<Task<BigQueryResults>> getMultiJobResults = async () =>
                    {
                        // To get the results of all statements in a multi-statement query, enumerate the child jobs. Related public docs: https://cloud.google.com/bigquery/docs/multi-statement-queries#get_all_executed_statements.
                        // Can filter by StatementType and EvaluationKind. Related public docs: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobstatistics2, https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#evaluationkind
                        ListJobsOptions listJobsOptions = new ListJobsOptions();
                        listJobsOptions.ParentJobId = results.JobReference.JobId;
                        var joblist = Client.ListJobs(listJobsOptions)
                            .Select(job => Client.GetJob(job.Reference))
                            .Where(job => string.IsNullOrEmpty(evaluationKind) || job.Statistics.ScriptStatistics.EvaluationKind.Equals(evaluationKind, StringComparison.OrdinalIgnoreCase))
                            .Where(job => string.IsNullOrEmpty(statementType) || job.Statistics.Query.StatementType.Equals(statementType, StringComparison.OrdinalIgnoreCase))
                            .OrderBy(job => job.Resource.Statistics.CreationTime)
                            .ToList();

                        if (joblist.Count > 0)
                        {
                            if (statementIndex < 1 || statementIndex > joblist.Count)
                            {
                                throw new ArgumentOutOfRangeException($"The specified index {statementIndex} is out of range. There are {joblist.Count} jobs available.");
                            }
                            return await joblist[statementIndex - 1].GetQueryResultsAsync(getQueryResultsOptions);
                        }

                        throw new AdbcException($"Unable to obtain result from statement [{statementIndex}]", AdbcStatusCode.InvalidData);
                    };

                    results = await ExecuteWithRetriesAsync(getMultiJobResults, activity);
                }

                if (results?.TableReference == null)
                {
                    throw new AdbcException("There is no query statement");
                }

                string table = $"projects/{results.TableReference.ProjectId}/datasets/{results.TableReference.DatasetId}/tables/{results.TableReference.TableId}";

                int maxStreamCount = 1;

                if (Options?.TryGetValue(BigQueryParameters.MaxFetchConcurrency, out string? maxStreamCountString) == true)
                {
                    if (int.TryParse(maxStreamCountString, out int count))
                    {
                        if (count >= 0)
                        {
                            maxStreamCount = count;
                        }
                    }
                }

                long totalRows = results.TotalRows == null ? -1L : (long)results.TotalRows.Value;

                Func<Task<IEnumerable<IArrowReader>>> func = () => GetArrowReaders(clientMgr, table, results.TableReference.ProjectId, maxStreamCount, activity);
                IEnumerable<IArrowReader> readers = await ExecuteWithRetriesAsync<IEnumerable<IArrowReader>>(func, activity);

                IArrowArrayStream stream = new MultiArrowReader(this, TranslateSchema(results.Schema), readers);
                activity?.AddTag(SemanticConventions.Db.Response.ReturnedRows, totalRows);
                return new QueryResult(totalRows, stream);
            });
        }

        private async Task<IEnumerable<IArrowReader>> GetArrowReaders(
            TokenProtectedReadClientManger clientMgr,
            string table,
            string projectId,
            int maxStreamCount,
            Activity? activity)
        {
            ReadSession rs = new ReadSession { Table = table, DataFormat = DataFormat.Arrow };
            BigQueryReadClient bigQueryReadClient = clientMgr.ReadClient;
            ReadSession rrs = await bigQueryReadClient.CreateReadSessionAsync("projects/" + projectId, rs, maxStreamCount);

            var readers = rrs.Streams
                             .Select(s => ReadChunk(bigQueryReadClient, s.Name, activity, this.bigQueryConnection.IsSafeToTrace))
                             .Where(chunk => chunk != null)
                             .Cast<IArrowReader>();

            return readers;
        }

        public override UpdateResult ExecuteUpdate()
        {
            return ExecuteUpdateInternalAsync().GetAwaiter().GetResult();
        }

        private async Task<UpdateResult> ExecuteUpdateInternalAsync()
        {
            return await this.TraceActivityAsync(async activity =>
            {
                GetQueryResultsOptions getQueryResultsOptions = new GetQueryResultsOptions();

                if (Options?.TryGetValue(BigQueryParameters.GetQueryResultsOptionsTimeout, out string? timeoutSeconds) == true &&
                    int.TryParse(timeoutSeconds, out int seconds) &&
                    seconds >= 0)
                {
                    getQueryResultsOptions.Timeout = TimeSpan.FromSeconds(seconds);
                    activity?.AddBigQueryParameterTag(BigQueryParameters.GetQueryResultsOptionsTimeout, seconds);
                }

                activity?.AddConditionalTag(SemanticConventions.Db.Query.Text, SqlQuery, this.bigQueryConnection.IsSafeToTrace);

                // Cannot set destination table in jobs with DDL statements, otherwise an error will be prompted
                Func<Task<BigQueryResults?>> func = () => Client.ExecuteQueryAsync(SqlQuery, null, null, getQueryResultsOptions);
                BigQueryResults? result = await ExecuteWithRetriesAsync<BigQueryResults?>(func, activity);
                long updatedRows = result?.NumDmlAffectedRows.HasValue == true ? result.NumDmlAffectedRows.Value : -1L;

                activity?.AddTag(SemanticConventions.Db.Response.ReturnedRows, updatedRows);
                return new UpdateResult(updatedRows);
            });
        }

        private Schema TranslateSchema(TableSchema schema)
        {
            return new Schema(schema.Fields.Select(TranslateField), null);
        }

        private Field TranslateField(TableFieldSchema field)
        {
            List<KeyValuePair<string, string>> metadata = new List<KeyValuePair<string, string>>()
            {
                new KeyValuePair<string, string>("BIGQUERY_TYPE", field.Type),
                new KeyValuePair<string, string>("BIGQUERY_MODE", field.Mode)
            };

            return new Field(field.Name, TranslateType(field), field.Mode == "NULLABLE", metadata);
        }

        private IArrowType TranslateType(TableFieldSchema field)
        {
            // per https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableFieldSchema.html#getType--

            switch (field.Type)
            {
                case "INTEGER" or "INT64":
                    return GetType(field, Int64Type.Default);
                case "FLOAT" or "FLOAT64":
                    return GetType(field, DoubleType.Default);
                case "BOOL" or "BOOLEAN":
                    return GetType(field, BooleanType.Default);
                case "STRING":
                    return GetType(field, StringType.Default);
                case "BYTES":
                    return GetType(field, BinaryType.Default);
                case "DATETIME":
                    return GetType(field, TimestampType.Default);
                case "TIMESTAMP":
                    return GetType(field, TimestampType.Default);
                case "TIME":
                    return GetType(field, Time64Type.Microsecond);
                case "DATE":
                    return GetType(field, Date32Type.Default);
                case "RECORD" or "STRUCT":
                    return GetType(field, BuildStructType(field));

                // treat these values as strings
                case "GEOGRAPHY" or "JSON":
                    return GetType(field, StringType.Default);

                // get schema cannot get precision and scale for NUMERIC or BIGNUMERIC types
                // instead, the max values are returned from BigQuery
                // see 'precision' on https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
                // and discussion in https://github.com/apache/arrow-adbc/pull/1192#discussion_r1365987279

                case "NUMERIC" or "DECIMAL":
                    return GetType(field, new Decimal128Type(38, 9));

                case "BIGNUMERIC" or "BIGDECIMAL":
                    if (Options != null)
                        return bool.Parse(Options[BigQueryParameters.LargeDecimalsAsString]) ? GetType(field, StringType.Default) : GetType(field, new Decimal256Type(76, 38));
                    else
                        return GetType(field, StringType.Default);

                default: throw new InvalidOperationException($"{field.Type} cannot be translated");
            }
        }

        private StructType BuildStructType(TableFieldSchema field)
        {
            List<Field> arrowFields = new List<Field>();

            foreach (TableFieldSchema subfield in field.Fields)
            {
                Field arrowField = TranslateField(subfield);
                arrowFields.Add(arrowField);
            }

            return new StructType(arrowFields.AsReadOnly());
        }

        private IArrowType GetType(TableFieldSchema field, IArrowType type)
        {
            if (field.Mode == "REPEATED")
                return new ListType(type);

            return type;
        }

        private static IArrowReader? ReadChunk(BigQueryReadClient client, string streamName, Activity? activity, bool isSafeToTrace)
        {
            // Ideally we wouldn't need to indirect through a stream, but the necessary APIs in Arrow
            // are internal. (TODO: consider changing Arrow).
            activity?.AddConditionalBigQueryTag("read_stream", streamName, isSafeToTrace);
            BigQueryReadClient.ReadRowsStream readRowsStream = client.ReadRows(new ReadRowsRequest { ReadStream = streamName });
            IAsyncEnumerator<ReadRowsResponse> enumerator = readRowsStream.GetResponseStream().GetAsyncEnumerator();

            ReadRowsStream stream = new ReadRowsStream(enumerator);
            activity?.AddBigQueryTag("read_stream.has_rows", stream.HasRows);

            return stream.HasRows ? stream : null;
        }

        private QueryOptions ValidateOptions(Activity? activity)
        {
            QueryOptions options = new QueryOptions();

            if (Client.ProjectId == BigQueryConstants.DetectProjectId)
            {
                activity?.AddBigQueryTag("client_project_id", BigQueryConstants.DetectProjectId);

                // An error occurs when calling CreateQueryJob without the ID set,
                // so use the first one that is found. This does not prevent from calling
                // to other 'project IDs' (catalogs) with a query.
                Func<Task<PagedEnumerable<ProjectList, CloudProject>?>> func = () => Task.Run(() =>
                {
                    return Client?.ListProjects();
                });

                PagedEnumerable<ProjectList, CloudProject>? projects = ExecuteWithRetriesAsync<PagedEnumerable<ProjectList, CloudProject>?>(func, activity).GetAwaiter().GetResult();

                if (projects != null)
                {
                    string? firstProjectId = projects.Select(x => x.ProjectId).FirstOrDefault();

                    if (firstProjectId != null)
                    {
                        options.ProjectId = firstProjectId;
                        activity?.AddBigQueryTag("detected_client_project_id", firstProjectId);
                        // need to reopen the Client with the projectId specified
                        this.bigQueryConnection.Open(firstProjectId);
                    }
                }
            }

            if (Options == null || Options.Count == 0)
                return options;

            string largeResultDatasetId = BigQueryConstants.DefaultLargeDatasetId;

            foreach (KeyValuePair<string, string> keyValuePair in Options)
            {
                switch (keyValuePair.Key)
                {
                    case BigQueryParameters.AllowLargeResults:
                        options.AllowLargeResults = true ? keyValuePair.Value.Equals("true", StringComparison.OrdinalIgnoreCase) : false;
                        activity?.AddBigQueryParameterTag(BigQueryParameters.AllowLargeResults, options.AllowLargeResults);
                        break;
                    case BigQueryParameters.LargeResultsDataset:
                        largeResultDatasetId = keyValuePair.Value;
                        activity?.AddBigQueryParameterTag(BigQueryParameters.LargeResultsDataset, largeResultDatasetId);
                        break;
                    case BigQueryParameters.LargeResultsDestinationTable:
                        string destinationTable = keyValuePair.Value;

                        if (!destinationTable.Contains("."))
                            throw new InvalidOperationException($"{BigQueryParameters.LargeResultsDestinationTable} is invalid");

                        string projectId = string.Empty;
                        string datasetId = string.Empty;
                        string tableId = string.Empty;

                        string[] segments = destinationTable.Split('.');

                        if (segments.Length != 3)
                            throw new InvalidOperationException($"{BigQueryParameters.LargeResultsDestinationTable} cannot be parsed");

                        projectId = segments[0];
                        datasetId = segments[1];
                        tableId = segments[2];

                        if (string.IsNullOrEmpty(projectId.Trim()) || string.IsNullOrEmpty(datasetId.Trim()) || string.IsNullOrEmpty(tableId.Trim()))
                            throw new InvalidOperationException($"{BigQueryParameters.LargeResultsDestinationTable} contains invalid values");

                        options.DestinationTable = new TableReference()
                        {
                            ProjectId = projectId,
                            DatasetId = datasetId,
                            TableId = tableId
                        };
                        activity?.AddBigQueryParameterTag(BigQueryParameters.LargeResultsDestinationTable, destinationTable);
                        break;
                    case BigQueryParameters.UseLegacySQL:
                        options.UseLegacySql = true ? keyValuePair.Value.Equals("true", StringComparison.OrdinalIgnoreCase) : false;
                        activity?.AddBigQueryParameterTag(BigQueryParameters.UseLegacySQL, options.UseLegacySql);
                        break;
                }
            }

            if (options.AllowLargeResults == true && options.DestinationTable == null)
            {
                options.DestinationTable = TryGetLargeDestinationTableReference(largeResultDatasetId, activity);
            }

            return options;
        }

        /// <summary>
        /// Attempts to retrieve or create the specified dataset.
        /// </summary>
        /// <param name="datasetId">The name of the dataset.</param>
        /// <returns>A <see cref="TableReference"/> to a randomly generated table name in the specified dataset.</returns>
        private TableReference TryGetLargeDestinationTableReference(string datasetId, Activity? activity)
        {
            BigQueryDataset? dataset = null;

            try
            {
                activity?.AddBigQueryTag("large_results.dataset.try_find", datasetId);
                dataset = this.Client.GetDataset(datasetId);
                activity?.AddBigQueryTag("large_results.dataset.found", datasetId);
            }
            catch (GoogleApiException gaEx)
            {
                if (gaEx.HttpStatusCode != System.Net.HttpStatusCode.NotFound)
                {
                    activity?.AddException(gaEx);
                    throw new AdbcException($"Failure trying to retrieve dataset {datasetId}", gaEx);
                }
            }

            if (dataset == null)
            {
                try
                {
                    activity?.AddBigQueryTag("large_results.dataset.try_create", datasetId);
                    activity?.AddBigQueryTag("large_results.dataset.try_create_region", this.Client.DefaultLocation);
                    DatasetReference reference = this.Client.GetDatasetReference(datasetId);

                    // The location is not set here because it will use the DefaultLocation from the client.
                    // Similar to the DefaultLocation for the client, if the caller attempts to use a public
                    // dataset from a multi-region but set the destination to a specific location,
                    // a similar permission error is thrown.
                    BigQueryDataset bigQueryDataset = new BigQueryDataset(this.Client, new Dataset()
                    {
                        DatasetReference = reference,
                        DefaultTableExpirationMs = (long)TimeSpan.FromDays(1).TotalMilliseconds
                    });

                    dataset = this.Client.CreateDataset(datasetId, bigQueryDataset.Resource);
                    activity?.AddBigQueryTag("large_results.dataset.created", datasetId);
                }
                catch (Exception ex)
                {
                    activity?.AddException(ex);
                    throw new AdbcException($"Could not create dataset {datasetId} in {this.Client.DefaultLocation}", ex);
                }
            }

            if (dataset == null)
            {
                throw new AdbcException($"Could not find dataset {datasetId}", AdbcStatusCode.NotFound);
            }
            else
            {
                TableReference reference = new TableReference()
                {
                    ProjectId = this.Client.ProjectId,
                    DatasetId = datasetId,
                    TableId = "lg_" + Guid.NewGuid().ToString().Replace("-", "")
                };

                activity?.AddBigQueryTag("large_results.table_reference", reference.ToString());

                return reference;
            }
        }

        public bool TokenRequiresUpdate(Exception ex) => BigQueryUtils.TokenRequiresUpdate(ex);

        private async Task<T> ExecuteWithRetriesAsync<T>(Func<Task<T>> action, Activity? activity) => await RetryManager.ExecuteWithRetriesAsync<T>(this, action, activity, MaxRetryAttempts, RetryDelayMs);

        private class MultiArrowReader : TracingReader
        {
            private static readonly string s_assemblyName = BigQueryUtils.GetAssemblyName(typeof(BigQueryStatement));
            private static readonly string s_assemblyVersion = BigQueryUtils.GetAssemblyVersion(typeof(BigQueryStatement));

            readonly Schema schema;
            IEnumerator<IArrowReader>? readers;
            IArrowReader? reader;

            public MultiArrowReader(BigQueryStatement statement, Schema schema, IEnumerable<IArrowReader> readers) : base(statement)
            {
                this.schema = schema;
                this.readers = readers.GetEnumerator();
            }

            public override Schema Schema { get { return this.schema; } }

            public override string AssemblyVersion => s_assemblyVersion;

            public override string AssemblyName => s_assemblyName;

            public override async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                return await this.TraceActivityAsync(async activity =>
                {
                    if (this.readers == null)
                    {
                        return null;
                    }

                    while (true)
                    {
                        if (this.reader == null)
                        {
                            if (!this.readers.MoveNext())
                            {
                                Dispose(); // TODO: Remove this line
                                return null;
                            }
                            this.reader = this.readers.Current;
                        }

                        RecordBatch result = await this.reader.ReadNextRecordBatchAsync(cancellationToken);

                        if (result != null)
                        {
                            return result;
                        }

                        this.reader = null;
                    }
                });
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    if (this.readers != null)
                    {
                        this.readers.Dispose();
                        this.readers = null;
                    }
                }

                base.Dispose(disposing);
            }
        }

        sealed class ReadRowsStream : IArrowArrayStream
        {
            readonly Schema? schema;
            readonly IAsyncEnumerator<ReadRowsResponse> response;
            bool first;
            bool disposed;

            public ReadRowsStream(IAsyncEnumerator<ReadRowsResponse> response)
            {
                try
                {
                    if (response.MoveNextAsync().Result && response.Current != null)
                    {
                        this.schema = ArrowSerializationHelpers.DeserializeSchema(response.Current.ArrowSchema.SerializedSchema.Memory);
                    }
                }
                catch (InvalidOperationException)
                {
                }

                this.response = response;
                this.first = true;
            }

            public Schema Schema => this.schema ?? throw new InvalidOperationException("Stream has no rows");
            public bool HasRows => this.schema != null;

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
            {
                if (this.first)
                {
                    this.first = false;
                }
                else if (this.disposed || !await this.response.MoveNextAsync())
                {
                    return null;
                }

                return ArrowSerializationHelpers.DeserializeRecordBatch(this.schema, this.response.Current.ArrowRecordBatch.SerializedRecordBatch.Memory);
            }

            public void Dispose()
            {
                if (!this.disposed)
                {
                    this.response.DisposeAsync().GetAwaiter().GetResult();
                    this.disposed = true;
                }
            }
        }
    }
}

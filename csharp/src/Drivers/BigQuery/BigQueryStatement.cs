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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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
    public class BigQueryStatement : AdbcStatement
    {
        readonly BigQueryClient client;
        readonly GoogleCredential credential;

        public BigQueryStatement(BigQueryClient client, GoogleCredential credential)
        {
            this.client = client;
            this.credential = credential;
        }

        public IReadOnlyDictionary<string, string>? Options { get; set; }

        public override QueryResult ExecuteQuery()
        {
            QueryOptions? queryOptions = ValidateOptions();
            BigQueryJob job = this.client.CreateQueryJob(SqlQuery, null, queryOptions);

            GetQueryResultsOptions getQueryResultsOptions = new GetQueryResultsOptions();

            if (this.Options?.TryGetValue(BigQueryParameters.GetQueryResultsOptionsTimeoutMinutes, out string? timeoutMinutes) == true)
            {
                if (int.TryParse(timeoutMinutes, out int minutes))
                {
                    if (minutes >= 0)
                    {
                        getQueryResultsOptions.Timeout = TimeSpan.FromMinutes(minutes);
                    }
                }
            }

            BigQueryResults results = job.GetQueryResults(getQueryResultsOptions);

            BigQueryReadClientBuilder readClientBuilder = new BigQueryReadClientBuilder();
            readClientBuilder.Credential = this.credential;
            BigQueryReadClient readClient = readClientBuilder.Build();

            if (results.TableReference == null)
            {
                // To get the results of all statements in a multi-statement query, enumerate the child jobs and call jobs.getQueryResults on each of them.
                // Related public docs: https://cloud.google.com/bigquery/docs/multi-statement-queries#get_all_executed_statements
                ListJobsOptions listJobsOptions = new ListJobsOptions();
                listJobsOptions.ParentJobId = results.JobReference.JobId;
                PagedEnumerable<JobList, BigQueryJob> joblist = client.ListJobs(listJobsOptions);
                BigQueryJob firstQueryJob = new BigQueryJob(client, job.Resource);
                foreach (BigQueryJob childJob in joblist)
                {
                    var tempJob = client.GetJob(childJob.Reference.JobId);
                    var query = tempJob.Resource?.Configuration?.Query;
                    if (query != null && query.DestinationTable != null && query.DestinationTable.ProjectId != null && query.DestinationTable.DatasetId != null && query.DestinationTable.TableId != null)
                    {
                        firstQueryJob = tempJob;
                    }
                }
                results = firstQueryJob.GetQueryResults();
            }

            if (results.TableReference == null)
            {
                throw new AdbcException("There is no query statement");
            }

            string table = $"projects/{results.TableReference.ProjectId}/datasets/{results.TableReference.DatasetId}/tables/{results.TableReference.TableId}";

            int maxStreamCount = 1;
            if (this.Options?.TryGetValue(BigQueryParameters.MaxFetchConcurrency, out string? maxStreamCountString) == true)
            {
                if (int.TryParse(maxStreamCountString, out int count))
                {
                    if (count >= 0)
                    {
                        maxStreamCount = count;
                    }
                }
            }
            ReadSession rs = new ReadSession { Table = table, DataFormat = DataFormat.Arrow };
            ReadSession rrs = readClient.CreateReadSession("projects/" + results.TableReference.ProjectId, rs, maxStreamCount);

            long totalRows = results.TotalRows == null ? -1L : (long)results.TotalRows.Value;
            IArrowArrayStream stream = new MultiArrowReader(TranslateSchema(results.Schema), rrs.Streams.Select(s => ReadChunk(readClient, s.Name)));

            return new QueryResult(totalRows, stream);
        }

        public override UpdateResult ExecuteUpdate()
        {
            BigQueryResults result = this.client.ExecuteQuery(SqlQuery, parameters: null);
            long updatedRows = result.NumDmlAffectedRows == null ? -1L : result.NumDmlAffectedRows.Value;

            return new UpdateResult(updatedRows);
        }

        private Schema TranslateSchema(TableSchema schema)
        {
            return new Schema(schema.Fields.Select(TranslateField), null);
        }

        private Field TranslateField(TableFieldSchema field)
        {
            return new Field(field.Name, TranslateType(field), field.Mode == "NULLABLE");
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
                    return GetType(field, Time64Type.Default);
                case "DATE":
                    return GetType(field, Date64Type.Default);
                case "RECORD" or "STRUCT":
                    // its a json string
                    return GetType(field, StringType.Default);

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
                    if (this.Options != null)
                        return bool.Parse(this.Options[BigQueryParameters.LargeDecimalsAsString]) ? GetType(field, StringType.Default) : GetType(field, new Decimal256Type(76, 38));
                    else
                        return GetType(field, StringType.Default);

                default: throw new InvalidOperationException($"{field.Type} cannot be translated");
            }
        }

        private IArrowType GetType(TableFieldSchema field, IArrowType type)
        {
            if (field.Mode == "REPEATED")
                return new ListType(type);

            return type;
        }

        static IArrowReader ReadChunk(BigQueryReadClient readClient, string streamName)
        {
            // Ideally we wouldn't need to indirect through a stream, but the necessary APIs in Arrow
            // are internal. (TODO: consider changing Arrow).
            BigQueryReadClient.ReadRowsStream readRowsStream = readClient.ReadRows(new ReadRowsRequest { ReadStream = streamName });
            IAsyncEnumerator<ReadRowsResponse> enumerator = readRowsStream.GetResponseStream().GetAsyncEnumerator();

            ReadRowsStream stream = new ReadRowsStream(enumerator);

            return new ArrowStreamReader(stream);
        }

        private QueryOptions? ValidateOptions()
        {
            if (this.Options == null || this.Options.Count == 0)
                return null;

            QueryOptions options = new QueryOptions();

            foreach (KeyValuePair<string, string> keyValuePair in this.Options)
            {
                if (keyValuePair.Key == BigQueryParameters.AllowLargeResults)
                {
                    options.AllowLargeResults = true ? keyValuePair.Value.ToLower().Equals("true") : false;
                }
                if (keyValuePair.Key == BigQueryParameters.LargeResultsDestinationTable)
                {
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
                }
                if (keyValuePair.Key == BigQueryParameters.UseLegacySQL)
                {
                    options.UseLegacySql = true ? keyValuePair.Value.ToLower().Equals("true") : false;
                }
            }
            return options;
        }

        class MultiArrowReader : IArrowArrayStream
        {
            readonly Schema schema;
            IEnumerator<IArrowReader>? readers;
            IArrowReader? reader;

            public MultiArrowReader(Schema schema, IEnumerable<IArrowReader> readers)
            {
                this.schema = schema;
                this.readers = readers.GetEnumerator();
            }

            public Schema Schema { get { return schema; } }

            public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
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
            }

            public void Dispose()
            {
                if (this.readers != null)
                {
                    this.readers.Dispose();
                    this.readers = null;
                }
            }
        }

        sealed class ReadRowsStream : Stream
        {
            IAsyncEnumerator<ReadRowsResponse> response;
            ReadOnlyMemory<byte> currentBuffer;
            bool first;
            int position;

            public ReadRowsStream(IAsyncEnumerator<ReadRowsResponse> response)
            {
                if (!response.MoveNextAsync().Result) { }
                this.currentBuffer = response.Current.ArrowSchema.SerializedSchema.Memory;
                this.response = response;
                this.first = true;
            }

            public override bool CanRead => true;

            public override bool CanSeek => false;

            public override bool CanWrite => false;

            public override long Length => throw new NotSupportedException();

            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                int remaining = this.currentBuffer.Length - this.position;
                if (remaining == 0)
                {
                    if (this.first)
                    {
                        this.first = false;
                    }
                    else if (!this.response.MoveNextAsync().Result)
                    {
                        return 0;
                    }
                    this.currentBuffer = this.response.Current.ArrowRecordBatch.SerializedRecordBatch.Memory;
                    this.position = 0;
                    remaining = this.currentBuffer.Length - this.position;
                }

                int bytes = Math.Min(remaining, count);
                this.currentBuffer.Slice(this.position, bytes).CopyTo(new Memory<byte>(buffer, offset, bytes));
                this.position += bytes;
                return bytes;
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return base.ReadAsync(buffer, offset, count, cancellationToken);
            }

            public override int ReadByte()
            {
                return base.ReadByte();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }
        }
    }
}

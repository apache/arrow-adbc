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
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.StatementExecution
{
    // Session Management Models

    /// <summary>
    /// Request to create a new session in Databricks.
    /// </summary>
    public class CreateSessionRequest
    {
        /// <summary>
        /// The warehouse ID to use for the session.
        /// </summary>
        [JsonPropertyName("warehouse_id")]
        public string WarehouseId { get; set; } = string.Empty;

        /// <summary>
        /// Optional catalog to set as default for the session.
        /// </summary>
        [JsonPropertyName("catalog")]
        public string? Catalog { get; set; }

        /// <summary>
        /// Optional schema to set as default for the session.
        /// </summary>
        [JsonPropertyName("schema")]
        public string? Schema { get; set; }

        /// <summary>
        /// Optional session configuration parameters.
        /// </summary>
        [JsonPropertyName("session_confs")]
        public Dictionary<string, string>? SessionConfigs { get; set; }
    }

    /// <summary>
    /// Response from creating a new session.
    /// </summary>
    public class CreateSessionResponse
    {
        /// <summary>
        /// The unique identifier for the created session.
        /// </summary>
        [JsonPropertyName("session_id")]
        public string SessionId { get; set; } = string.Empty;
    }

    // Statement Execution Models

    /// <summary>
    /// Request to execute a SQL statement.
    /// </summary>
    public class ExecuteStatementRequest
    {
        /// <summary>
        /// The warehouse ID (required if session_id is not provided).
        /// </summary>
        [JsonPropertyName("warehouse_id")]
        public string? WarehouseId { get; set; }

        /// <summary>
        /// The session ID (required if warehouse_id is not provided).
        /// </summary>
        [JsonPropertyName("session_id")]
        public string? SessionId { get; set; }

        /// <summary>
        /// The SQL statement to execute.
        /// </summary>
        [JsonPropertyName("statement")]
        public string Statement { get; set; } = string.Empty;

        /// <summary>
        /// Optional catalog context for the statement.
        /// </summary>
        [JsonPropertyName("catalog")]
        public string? Catalog { get; set; }

        /// <summary>
        /// Optional schema context for the statement.
        /// </summary>
        [JsonPropertyName("schema")]
        public string? Schema { get; set; }

        /// <summary>
        /// Optional statement parameters for parameterized queries.
        /// </summary>
        [JsonPropertyName("parameters")]
        public List<StatementParameter>? Parameters { get; set; }

        /// <summary>
        /// Result disposition: "inline", "external_links", or "inline_or_external_links".
        /// </summary>
        [JsonPropertyName("disposition")]
        public string Disposition { get; set; } = DatabricksConstants.ResultDispositions.InlineOrExternalLinks;

        /// <summary>
        /// Result format: "arrow_stream", "json_array", or "csv".
        /// </summary>
        [JsonPropertyName("format")]
        public string Format { get; set; } = DatabricksConstants.ResultFormats.ArrowStream;

        /// <summary>
        /// Optional result compression: "lz4", "gzip", or "none".
        /// </summary>
        [JsonPropertyName("result_compression")]
        public string? ResultCompression { get; set; }

        /// <summary>
        /// Wait timeout (e.g., "10s"). Omit for direct results mode.
        /// </summary>
        [JsonPropertyName("wait_timeout")]
        public string? WaitTimeout { get; set; }

        /// <summary>
        /// Behavior on wait timeout: "CONTINUE" or "CANCEL".
        /// </summary>
        [JsonPropertyName("on_wait_timeout")]
        public string? OnWaitTimeout { get; set; }

        /// <summary>
        /// Optional maximum number of rows to return.
        /// </summary>
        [JsonPropertyName("row_limit")]
        public long? RowLimit { get; set; }

        /// <summary>
        /// Optional maximum number of bytes to return.
        /// </summary>
        [JsonPropertyName("byte_limit")]
        public long? ByteLimit { get; set; }
    }

    /// <summary>
    /// Response from executing a statement.
    /// </summary>
    public class ExecuteStatementResponse
    {
        /// <summary>
        /// The unique identifier for the statement.
        /// </summary>
        [JsonPropertyName("statement_id")]
        public string StatementId { get; set; } = string.Empty;

        /// <summary>
        /// The current status of the statement.
        /// </summary>
        [JsonPropertyName("status")]
        public StatementStatus Status { get; set; } = new StatementStatus();

        /// <summary>
        /// Result manifest (available when status is SUCCEEDED).
        /// </summary>
        [JsonPropertyName("manifest")]
        public ResultManifest? Manifest { get; set; }

        /// <summary>
        /// Statement result data (for inline results).
        /// </summary>
        [JsonPropertyName("result")]
        public StatementResult? Result { get; set; }
    }

    /// <summary>
    /// Response from getting statement status/results.
    /// </summary>
    public class GetStatementResponse
    {
        /// <summary>
        /// The unique identifier for the statement.
        /// </summary>
        [JsonPropertyName("statement_id")]
        public string StatementId { get; set; } = string.Empty;

        /// <summary>
        /// The current status of the statement.
        /// </summary>
        [JsonPropertyName("status")]
        public StatementStatus Status { get; set; } = new StatementStatus();

        /// <summary>
        /// Result manifest (available when status is SUCCEEDED).
        /// </summary>
        [JsonPropertyName("manifest")]
        public ResultManifest? Manifest { get; set; }

        /// <summary>
        /// Statement result data (for inline results).
        /// </summary>
        [JsonPropertyName("result")]
        public StatementResult? Result { get; set; }
    }

    /// <summary>
    /// Statement execution status.
    /// </summary>
    public class StatementStatus
    {
        /// <summary>
        /// State: "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED".
        /// </summary>
        [JsonPropertyName("state")]
        public string State { get; set; } = string.Empty;

        /// <summary>
        /// Error information if the statement failed.
        /// </summary>
        [JsonPropertyName("error")]
        public StatementError? Error { get; set; }
    }

    /// <summary>
    /// Statement execution error details.
    /// </summary>
    public class StatementError
    {
        /// <summary>
        /// Error code.
        /// </summary>
        [JsonPropertyName("error_code")]
        public string ErrorCode { get; set; } = string.Empty;

        /// <summary>
        /// Human-readable error message.
        /// </summary>
        [JsonPropertyName("message")]
        public string Message { get; set; } = string.Empty;

        /// <summary>
        /// SQL state code.
        /// </summary>
        [JsonPropertyName("sql_state")]
        public string? SqlState { get; set; }
    }

    // Result Models

    /// <summary>
    /// Result manifest describing the structure and location of result data.
    /// </summary>
    public class ResultManifest
    {
        /// <summary>
        /// Result format: "arrow_stream", "json_array", or "csv".
        /// </summary>
        [JsonPropertyName("format")]
        public string Format { get; set; } = string.Empty;

        /// <summary>
        /// Schema information for the result.
        /// </summary>
        [JsonPropertyName("schema")]
        public ResultSchema Schema { get; set; } = new ResultSchema();

        /// <summary>
        /// Total number of chunks in the result.
        /// </summary>
        [JsonPropertyName("total_chunk_count")]
        public int TotalChunkCount { get; set; }

        /// <summary>
        /// List of result chunks.
        /// </summary>
        [JsonPropertyName("chunks")]
        public List<ResultChunk>? Chunks { get; set; }

        /// <summary>
        /// Total number of rows in the result.
        /// </summary>
        [JsonPropertyName("total_row_count")]
        public long TotalRowCount { get; set; }

        /// <summary>
        /// Total byte count of the result.
        /// </summary>
        [JsonPropertyName("total_byte_count")]
        public long TotalByteCount { get; set; }

        /// <summary>
        /// Compression codec used: "lz4", "gzip", or "none".
        /// </summary>
        [JsonPropertyName("result_compression")]
        public string? ResultCompression { get; set; }

        /// <summary>
        /// Whether results were truncated by row_limit or byte_limit.
        /// </summary>
        [JsonPropertyName("truncated")]
        public bool? Truncated { get; set; }

        /// <summary>
        /// Whether this is a Unity Catalog Volume operation.
        /// </summary>
        [JsonPropertyName("is_volume_operation")]
        public bool? IsVolumeOperation { get; set; }
    }

    /// <summary>
    /// A chunk of result data.
    /// </summary>
    public class ResultChunk
    {
        /// <summary>
        /// Index of this chunk in the result set.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public int ChunkIndex { get; set; }

        /// <summary>
        /// Number of rows in this chunk.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long RowCount { get; set; }

        /// <summary>
        /// Row offset of this chunk in the result set.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long RowOffset { get; set; }

        /// <summary>
        /// Byte count of this chunk.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long ByteCount { get; set; }

        /// <summary>
        /// External links for downloading chunk data (EXTERNAL_LINKS disposition).
        /// </summary>
        [JsonPropertyName("external_links")]
        public List<ExternalLink>? ExternalLinks { get; set; }

        /// <summary>
        /// Inline data array (INLINE disposition).
        /// </summary>
        [JsonPropertyName("data_array")]
        public List<List<object>>? DataArray { get; set; }

        /// <summary>
        /// Binary attachment for special result types.
        /// </summary>
        [JsonPropertyName("attachment")]
        public byte[]? Attachment { get; set; }

        /// <summary>
        /// Index of the next chunk (for pagination).
        /// </summary>
        [JsonPropertyName("next_chunk_index")]
        public long? NextChunkIndex { get; set; }

        /// <summary>
        /// Internal link to the next chunk (for pagination).
        /// </summary>
        [JsonPropertyName("next_chunk_internal_link")]
        public string? NextChunkInternalLink { get; set; }
    }

    /// <summary>
    /// External link for downloading result data from cloud storage.
    /// </summary>
    public class ExternalLink
    {
        /// <summary>
        /// Presigned URL for downloading the result data.
        /// </summary>
        [JsonPropertyName("external_link")]
        public string ExternalLinkUrl { get; set; } = string.Empty;

        /// <summary>
        /// Expiration time of the presigned URL (ISO 8601 format).
        /// </summary>
        [JsonPropertyName("expiration")]
        public string Expiration { get; set; } = string.Empty;

        /// <summary>
        /// Chunk index this link belongs to.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public long ChunkIndex { get; set; }

        /// <summary>
        /// Number of rows in this link.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long RowCount { get; set; }

        /// <summary>
        /// Row offset of this link in the result set.
        /// </summary>
        [JsonPropertyName("row_offset")]
        public long RowOffset { get; set; }

        /// <summary>
        /// Byte count of this link.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long ByteCount { get; set; }

        /// <summary>
        /// Optional HTTP headers required for downloading (e.g., for cloud storage auth).
        /// </summary>
        [JsonPropertyName("http_headers")]
        public Dictionary<string, string>? HttpHeaders { get; set; }

        /// <summary>
        /// Index of the next chunk (for pagination).
        /// </summary>
        [JsonPropertyName("next_chunk_index")]
        public long? NextChunkIndex { get; set; }

        /// <summary>
        /// Internal link to the next chunk (for pagination).
        /// </summary>
        [JsonPropertyName("next_chunk_internal_link")]
        public string? NextChunkInternalLink { get; set; }
    }

    /// <summary>
    /// Schema information for result data.
    /// </summary>
    public class ResultSchema
    {
        /// <summary>
        /// List of columns in the result.
        /// </summary>
        [JsonPropertyName("columns")]
        public List<ColumnInfo>? Columns { get; set; }
    }

    /// <summary>
    /// Column metadata information.
    /// </summary>
    public class ColumnInfo
    {
        /// <summary>
        /// Column name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Column data type name.
        /// </summary>
        [JsonPropertyName("type_name")]
        public string TypeName { get; set; } = string.Empty;

        /// <summary>
        /// Column type in text format.
        /// </summary>
        [JsonPropertyName("type_text")]
        public string? TypeText { get; set; }

        /// <summary>
        /// Precision for numeric types.
        /// </summary>
        [JsonPropertyName("precision")]
        public int? Precision { get; set; }

        /// <summary>
        /// Scale for numeric types.
        /// </summary>
        [JsonPropertyName("scale")]
        public int? Scale { get; set; }

        /// <summary>
        /// Position of the column in the result set.
        /// </summary>
        [JsonPropertyName("position")]
        public int Position { get; set; }

        /// <summary>
        /// Whether the column is nullable.
        /// </summary>
        [JsonPropertyName("nullable")]
        public bool? Nullable { get; set; }

        /// <summary>
        /// Comment or description for the column.
        /// </summary>
        [JsonPropertyName("comment")]
        public string? Comment { get; set; }
    }

    /// <summary>
    /// Statement parameter for parameterized queries.
    /// </summary>
    public class StatementParameter
    {
        /// <summary>
        /// Parameter name.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Parameter data type.
        /// </summary>
        [JsonPropertyName("type")]
        public string Type { get; set; } = string.Empty;

        /// <summary>
        /// Parameter value.
        /// </summary>
        [JsonPropertyName("value")]
        public object? Value { get; set; }
    }

    /// <summary>
    /// Statement result data (for inline results).
    /// </summary>
    public class StatementResult
    {
        /// <summary>
        /// Row count in the result.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long? RowCount { get; set; }

        /// <summary>
        /// Byte count of the result.
        /// </summary>
        [JsonPropertyName("byte_count")]
        public long? ByteCount { get; set; }

        /// <summary>
        /// Result data for inline disposition.
        /// </summary>
        [JsonPropertyName("data_array")]
        public List<List<object>>? DataArray { get; set; }

        /// <summary>
        /// External links for external disposition.
        /// </summary>
        [JsonPropertyName("external_links")]
        public List<ExternalLink>? ExternalLinks { get; set; }
    }

    /// <summary>
    /// Result data for chunk retrieval.
    /// </summary>
    public class ResultData
    {
        /// <summary>
        /// Chunk index.
        /// </summary>
        [JsonPropertyName("chunk_index")]
        public int ChunkIndex { get; set; }

        /// <summary>
        /// Row count in this chunk.
        /// </summary>
        [JsonPropertyName("row_count")]
        public long RowCount { get; set; }

        /// <summary>
        /// Inline data array.
        /// </summary>
        [JsonPropertyName("data_array")]
        public List<List<object>>? DataArray { get; set; }

        /// <summary>
        /// External links for this chunk.
        /// </summary>
        [JsonPropertyName("external_links")]
        public List<ExternalLink>? ExternalLinks { get; set; }
    }
}

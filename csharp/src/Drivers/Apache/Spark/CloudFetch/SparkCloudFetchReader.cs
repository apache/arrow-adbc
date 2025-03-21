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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch
{
    /// <summary>
    /// Reader for CloudFetch results from Databricks Spark Thrift server.
    /// Handles downloading and processing URL-based result sets.
    /// </summary>
    internal sealed class SparkCloudFetchReader : IArrowArrayStream
    {
        private const int MaxRetries = 3;
        private const int RetryDelayMs = 500;

        private HiveServer2Statement? statement;
        private readonly Schema schema;
        private List<TSparkArrowResultLink>? resultLinks;
        private int linkIndex;
        private ArrowStreamReader? currentReader;
        private readonly bool isLz4Compressed;
        private long startOffset;

        // Lazy initialization of HttpClient
        private readonly Lazy<HttpClient> _httpClient = new Lazy<HttpClient>(() =>
        {
            var client = new HttpClient();
            client.Timeout = TimeSpan.FromMinutes(5); // Set a reasonable timeout for large downloads
            return client;
        });

        /// <summary>
        /// Initializes a new instance of the <see cref="SparkCloudFetchReader"/> class.
        /// </summary>
        /// <param name="statement">The HiveServer2 statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        public SparkCloudFetchReader(HiveServer2Statement statement, Schema schema, bool isLz4Compressed)
        {
            this.statement = statement;
            this.schema = schema;
            this.isLz4Compressed = isLz4Compressed;
        }

        /// <summary>
        /// Gets the Arrow schema.
        /// </summary>
        public Schema Schema { get { return schema; } }

        private HttpClient HttpClient
        {
            get { return _httpClient.Value; }
        }

        /// <summary>
        /// Reads the next record batch from the result set.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next record batch, or null if there are no more batches.</returns>
        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                // If we have a current reader, try to read the next batch
                if (this.currentReader != null)
                {
                    RecordBatch? next = await this.currentReader.ReadNextRecordBatchAsync(cancellationToken);
                    if (next != null)
                    {
                        return next;
                    }
                    else
                    {
                        this.currentReader.Dispose();
                        this.currentReader = null;
                    }
                }

                // If we have more links to process, download and process the next one
                if (this.resultLinks != null && this.linkIndex < this.resultLinks.Count)
                {
                    var link = this.resultLinks[this.linkIndex++];
                    byte[]? fileData = null;

                    // Retry logic for downloading files
                    for (int retry = 0; retry < MaxRetries; retry++)
                    {
                        try
                        {
                            fileData = await DownloadFileAsync(link.FileLink, cancellationToken);
                            break; // Success, exit retry loop
                        }
                        catch (Exception ex) when (retry < MaxRetries - 1)
                        {
                            // Log the error and retry
                            Console.WriteLine($"Error downloading file (attempt {retry + 1}/{MaxRetries}): {ex.Message}");
                            await Task.Delay(RetryDelayMs * (retry + 1), cancellationToken);
                        }
                    }

                    if (fileData == null)
                    {
                        // All retries failed, continue to the next link or fetch more links
                        continue;
                    }

                    // Process the downloaded file data
                    MemoryStream dataStream;

                    // If the data is LZ4 compressed, decompress it
                    if (this.isLz4Compressed)
                    {
                        try
                        {
                            dataStream = new MemoryStream();
                            using (var inputStream = new MemoryStream(fileData))
                            using (var decompressor = LZ4Stream.Decode(inputStream))
                            {
                                await decompressor.CopyToAsync(dataStream);
                            }
                            dataStream.Position = 0;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error decompressing data: {ex.Message}");
                            continue; // Skip this link and try the next one
                        }
                    }
                    else
                    {
                        dataStream = new MemoryStream(fileData);
                    }

                    try
                    {
                        this.currentReader = new ArrowStreamReader(dataStream);
                        continue;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error creating Arrow reader: {ex.Message}");
                        dataStream.Dispose();
                        continue; // Skip this link and try the next one
                    }
                }

                this.resultLinks = null;
                this.linkIndex = 0;

                // If there's no statement, we're done
                if (this.statement == null)
                {
                    return null;
                }

                // Fetch more results from the server
                TFetchResultsReq request = new TFetchResultsReq(this.statement.OperationHandle, TFetchOrientation.FETCH_NEXT, this.statement.BatchSize);

                // Set the start row offset if we have processed some links already
                if (this.startOffset > 0)
                {
                    request.StartRowOffset = this.startOffset;
                }

                TFetchResultsResp response;
                try
                {
                    response = await this.statement.Connection.Client!.FetchResults(request, cancellationToken);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error fetching results from server: {ex.Message}");
                    this.statement = null; // Mark as done due to error
                    return null;
                }

                // Check if we have URL-based results
                if (response.Results.__isset.resultLinks &&
                    response.Results.ResultLinks != null &&
                    response.Results.ResultLinks.Count > 0)
                {
                    this.resultLinks = response.Results.ResultLinks;

                    // Update the start offset for the next fetch by calculating it from the links
                    if (this.resultLinks.Count > 0)
                    {
                        var lastLink = this.resultLinks[this.resultLinks.Count - 1];
                        this.startOffset = lastLink.StartRowOffset + lastLink.RowCount;
                    }

                    // If the server indicates there are no more rows, we can close the statement
                    if (!response.HasMoreRows)
                    {
                        this.statement = null;
                    }
                }
                else
                {
                    // If there are no more results, we're done
                    this.statement = null;
                    return null;
                }
            }
        }

        /// <summary>
        /// Downloads a file from a URL.
        /// </summary>
        /// <param name="url">The URL to download from.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The downloaded file data.</returns>
        private async Task<byte[]> DownloadFileAsync(string url, CancellationToken cancellationToken)
        {
            using HttpResponseMessage response = await HttpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();

            // Get the content length if available
            long? contentLength = response.Content.Headers.ContentLength;
            if (contentLength.HasValue && contentLength.Value > 0)
            {
                Console.WriteLine($"Downloading file of size: {contentLength.Value / 1024.0 / 1024.0:F2} MB");
            }

            return await response.Content.ReadAsByteArrayAsync();
        }

        /// <summary>
        /// Disposes the reader.
        /// </summary>
        public void Dispose()
        {
            if (this.currentReader != null)
            {
                this.currentReader.Dispose();
                this.currentReader = null;
            }

            // Dispose the HttpClient if it was created
            if (_httpClient.IsValueCreated)
            {
                _httpClient.Value.Dispose();
            }
        }
    }
}

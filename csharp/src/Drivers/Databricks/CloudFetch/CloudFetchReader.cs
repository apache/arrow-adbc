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
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch
{
    /// <summary>
    /// Reader for CloudFetch results from Databricks Spark Thrift server.
    /// Handles downloading and processing URL-based result sets.
    /// </summary>
    internal sealed class CloudFetchReader : BaseDatabricksReader
    {
        private ICloudFetchDownloadManager? downloadManager;
        private ArrowStreamReader? currentReader;
        private IDownloadResult? currentDownloadResult;
        private bool isPrefetchEnabled;

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudFetchReader"/> class.
        /// </summary>
        /// <param name="statement">The Databricks statement.</param>
        /// <param name="schema">The Arrow schema.</param>
        /// <param name="isLz4Compressed">Whether the results are LZ4 compressed.</param>
        public CloudFetchReader(DatabricksStatement statement, Schema schema, TFetchResultsResp? initialResults, bool isLz4Compressed, HttpClient httpClient)
            : base(statement, schema, isLz4Compressed)
        {
            // Check if prefetch is enabled
            var connectionProps = statement.Connection.Properties;
            isPrefetchEnabled = true; // Default to true
            if (connectionProps.TryGetValue(DatabricksParameters.CloudFetchPrefetchEnabled, out string? prefetchEnabledStr))
            {
                if (bool.TryParse(prefetchEnabledStr, out bool parsedPrefetchEnabled))
                {
                    isPrefetchEnabled = parsedPrefetchEnabled;
                }
                else
                {
                    throw new ArgumentException($"Invalid value for {DatabricksParameters.CloudFetchPrefetchEnabled}: {prefetchEnabledStr}. Expected a boolean value.");
                }
            }

            // Initialize the download manager
            if (isPrefetchEnabled)
            {
                downloadManager = new CloudFetchDownloadManager(statement, schema, initialResults, isLz4Compressed, httpClient);
                downloadManager.StartAsync().Wait();
            }
            else
            {
                // For now, we only support the prefetch implementation
                // This flag is reserved for future use if we need to support a non-prefetch mode
                downloadManager = new CloudFetchDownloadManager(statement, schema, initialResults, isLz4Compressed, httpClient);
                downloadManager.StartAsync().Wait();
            }
        }

        /// <summary>
        /// Reads the next record batch from the result set.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The next record batch, or null if there are no more batches.</returns>
        public override async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async _ =>
            {
                ThrowIfDisposed();

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
                            // Clean up the current reader and download result
                            this.currentReader.Dispose();
                            this.currentReader = null;

                            if (this.currentDownloadResult != null)
                            {
                                this.currentDownloadResult.Dispose();
                                this.currentDownloadResult = null;
                            }
                        }
                    }

                    // If we don't have a current reader, get the next downloaded file
                    if (this.downloadManager != null)
                    {
                        // Start the download manager if it's not already started
                        if (!this.isPrefetchEnabled)
                        {
                            throw new InvalidOperationException("Prefetch must be enabled.");
                        }

                        try
                        {
                            // Get the next downloaded file
                            this.currentDownloadResult = await this.downloadManager.GetNextDownloadedFileAsync(cancellationToken);
                            if (this.currentDownloadResult == null)
                            {
                                this.downloadManager.Dispose();
                                this.downloadManager = null;
                                // No more files
                                return null;
                            }

                            await this.currentDownloadResult.DownloadCompletedTask;

                            // Create a new reader for the downloaded file
                            try
                            {
                                this.currentReader = new ArrowStreamReader(this.currentDownloadResult.DataStream);
                                continue;
                            }
                            catch (Exception ex)
                            {
                                Debug.WriteLine($"Error creating Arrow reader: {ex.Message}");
                                this.currentDownloadResult.Dispose();
                                this.currentDownloadResult = null;
                                throw;
                            }
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine($"Error getting next downloaded file: {ex.Message}");
                            throw;
                        }
                    }

                    // If we get here, there are no more files
                    return null;
                }
            });
        }

        protected override void Dispose(bool disposing)
        {
            if (this.currentReader != null)
            {
                this.currentReader.Dispose();
                this.currentReader = null;
            }

            if (this.currentDownloadResult != null)
            {
                this.currentDownloadResult.Dispose();
                this.currentDownloadResult = null;
            }

            if (this.downloadManager != null)
            {
                this.downloadManager.Dispose();
                this.downloadManager = null;
            }
            base.Dispose(disposing);
        }
    }
}

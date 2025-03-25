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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkStatement : HiveServer2Statement
    {
        // Default maximum bytes per file for CloudFetch
        private const long DefaultMaxBytesPerFile = 20 * 1024 * 1024; // 20MB

        // CloudFetch configuration
        private bool useCloudFetch = true;
        private bool canDecompressLz4 = true;
        private long maxBytesPerFile = DefaultMaxBytesPerFile;

        internal SparkStatement(SparkConnection connection)
            : base(connection)
        {
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            // TODO: Ensure this is set dynamically depending on server capabilities.
            statement.EnforceResultPersistenceMode = false;
            statement.ResultPersistenceMode = TResultPersistenceMode.ALL_RESULTS;
            // This seems like a good idea to have the server timeout so it doesn't keep processing unnecessarily.
            // Set in combination with a CancellationToken.
            statement.QueryTimeout = QueryTimeoutSeconds;
            statement.CanReadArrowResult = true;

            // Set CloudFetch capabilities
            statement.CanDownloadResult = useCloudFetch;
            statement.CanDecompressLZ4Result = canDecompressLz4;
            statement.MaxBytesPerFile = maxBytesPerFile;

#pragma warning disable CS0618 // Type or member is obsolete
            statement.ConfOverlay = SparkConnection.timestampConfig;
#pragma warning restore CS0618 // Type or member is obsolete
            statement.UseArrowNativeTypes = new TSparkArrowTypes
            {
                TimestampAsArrow = true,
                DecimalAsArrow = true,

                // set to false so they return as string
                // otherwise, they return as ARRAY_TYPE but you can't determine
                // the object type of the items in the array
                ComplexTypesAsArrow = false,
                IntervalTypesAsArrow = false,
            };
        }

        public override void SetOption(string key, string value)
        {
            switch (key)
            {
                case Options.UseCloudFetch:
                    if (bool.TryParse(value, out bool useCloudFetchValue))
                    {
                        this.useCloudFetch = useCloudFetchValue;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a boolean value.");
                    }
                    break;
                case Options.CanDecompressLz4:
                    if (bool.TryParse(value, out bool canDecompressLz4Value))
                    {
                        this.canDecompressLz4 = canDecompressLz4Value;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a boolean value.");
                    }
                    break;
                case Options.MaxBytesPerFile:
                    if (long.TryParse(value, out long maxBytesPerFileValue))
                    {
                        this.maxBytesPerFile = maxBytesPerFileValue;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a long value.");
                    }
                    break;
                default:
                    base.SetOption(key, value);
                    break;
            }
        }

        /// <summary>
        /// Sets whether to use CloudFetch for retrieving results.
        /// </summary>
        /// <param name="useCloudFetch">Whether to use CloudFetch.</param>
        internal void SetUseCloudFetch(bool useCloudFetch)
        {
            this.useCloudFetch = useCloudFetch;
        }

        /// <summary>
        /// Gets whether CloudFetch is enabled.
        /// </summary>
        public bool UseCloudFetch => useCloudFetch;

        /// <summary>
        /// Sets whether the client can decompress LZ4 compressed results.
        /// </summary>
        /// <param name="canDecompressLz4">Whether the client can decompress LZ4.</param>
        internal void SetCanDecompressLz4(bool canDecompressLz4)
        {
            this.canDecompressLz4 = canDecompressLz4;
        }

        /// <summary>
        /// Gets whether LZ4 decompression is enabled.
        /// </summary>
        public bool CanDecompressLz4 => canDecompressLz4;

        /// <summary>
        /// Sets the maximum bytes per file for CloudFetch.
        /// </summary>
        /// <param name="maxBytesPerFile">The maximum bytes per file.</param>
        internal void SetMaxBytesPerFile(long maxBytesPerFile)
        {
            this.maxBytesPerFile = maxBytesPerFile;
        }

        /// <summary>
        /// Gets the maximum bytes per file for CloudFetch.
        /// </summary>
        public long MaxBytesPerFile => maxBytesPerFile;

        /// <summary>
        /// Provides the constant string key values to the <see cref="AdbcStatement.SetOption(string, string)" /> method.
        /// </summary>
        public sealed class Options : ApacheParameters
        {
            // CloudFetch options
            public const string UseCloudFetch = "adbc.spark.cloudfetch.enabled";
            public const string CanDecompressLz4 = "adbc.spark.cloudfetch.lz4.enabled";
            public const string MaxBytesPerFile = "adbc.spark.cloudfetch.max_bytes_per_file";
        }
    }
}

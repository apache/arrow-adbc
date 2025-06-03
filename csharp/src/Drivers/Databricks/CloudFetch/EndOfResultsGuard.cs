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
using System.IO;
using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch
{
    /// <summary>
    /// Special marker class that indicates the end of results in the download queue.
    /// </summary>
    internal sealed class EndOfResultsGuard : IDownloadResult
    {
        private static readonly Task CompletedTask = Task.CompletedTask;

        /// <summary>
        /// Gets the singleton instance of the <see cref="EndOfResultsGuard"/> class.
        /// </summary>
        public static EndOfResultsGuard Instance { get; } = new EndOfResultsGuard();

        private EndOfResultsGuard()
        {
            // Private constructor to enforce singleton pattern
        }

        /// <inheritdoc />
        public TSparkArrowResultLink Link => throw new NotSupportedException("EndOfResultsGuard does not have a link.");

        /// <inheritdoc />
        public Stream DataStream => throw new NotSupportedException("EndOfResultsGuard does not have a data stream.");

        /// <inheritdoc />
        public long Size => 0;

        /// <inheritdoc />
        public Task DownloadCompletedTask => CompletedTask;

        /// <inheritdoc />
        public bool IsCompleted => true;

        /// <inheritdoc />
        public int RefreshAttempts => 0;

        /// <inheritdoc />
        public void SetCompleted(Stream dataStream, long size) => throw new NotSupportedException("EndOfResultsGuard cannot be completed.");

        /// <inheritdoc />
        public void SetFailed(Exception exception) => throw new NotSupportedException("EndOfResultsGuard cannot fail.");

        /// <inheritdoc />
        public void UpdateWithRefreshedLink(TSparkArrowResultLink refreshedLink) => throw new NotSupportedException("EndOfResultsGuard cannot be updated with a refreshed link.");

        /// <inheritdoc />
        public bool IsExpiredOrExpiringSoon(int expirationBufferSeconds = 60) => false;

        /// <inheritdoc />
        public void Dispose()
        {
            // Nothing to dispose
        }
    }
}

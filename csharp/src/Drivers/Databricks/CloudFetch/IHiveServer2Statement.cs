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

using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Databricks.CloudFetch
{
    /// <summary>
    /// Interface for accessing HiveServer2Statement properties needed by CloudFetchResultFetcher.
    /// </summary>
    internal interface IHiveServer2Statement
    {
        /// <summary>
        /// Gets the operation response.
        /// </summary>
        IResponse? Response { get; }

        /// <summary>
        /// Gets the client.
        /// </summary>
        TCLIService.IAsync Client { get; }

        /// <summary>
        /// Checks if direct results are available.
        /// </summary>
        /// <returns>True if direct results are available and contain result data, false otherwise.</returns>
        bool HasDirectResults { get; }
    }
}

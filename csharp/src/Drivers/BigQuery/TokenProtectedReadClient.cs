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
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    /// <summary>
    /// Manages a <see cref="BigQueryReadClient"/> that is protected by a token.
    /// </summary>
    internal class TokenProtectedReadClientManger : ITokenProtectedResource
    {
        BigQueryReadClient bigQueryReadClient;

        public TokenProtectedReadClientManger(GoogleCredential credential)
        {
            UpdateCredential(credential);

            if (bigQueryReadClient == null)
            {
                throw new InvalidOperationException("could not create a read client");
            }
        }

        public BigQueryReadClient ReadClient => bigQueryReadClient;

        public void UpdateCredential(GoogleCredential? credential)
        {
            if (credential == null)
            {
                throw new ArgumentNullException(nameof(credential));
            }

            BigQueryReadClientBuilder readClientBuilder = new BigQueryReadClientBuilder();
            readClientBuilder.Credential = credential;
            this.bigQueryReadClient = readClientBuilder.Build();
        }

        public Func<Task>? UpdateToken { get; set; }

        public bool TokenRequiresUpdate(Exception ex) => BigQueryUtils.TokenRequiresUpdate(ex);
    }
}

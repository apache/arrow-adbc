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

using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using K4os.Compression.LZ4;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    public class Lz4CompressionHandler : DelegatingHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // Compress the request content if it exists
            if (request.Content != null)
            {
                var originalContent = await request.Content.ReadAsByteArrayAsync();
                var compressedContent = LZ4Pickler.Pickle(originalContent);
                request.Content = new ByteArrayContent(compressedContent);
                request.Content.Headers.ContentEncoding.Add("lz4");
            }

            // Send the request
            var response = await base.SendAsync(request, cancellationToken);

            // Decompress the response content if it is compressed
            if (response.Content != null && response.Content.Headers.ContentEncoding.Contains("lz4"))
            {
                var compressedContent = await response.Content.ReadAsByteArrayAsync();
                var decompressedContent = LZ4Pickler.Unpickle(compressedContent);
                response.Content = new ByteArrayContent(decompressedContent);
            }

            return response;
        }
    }
}

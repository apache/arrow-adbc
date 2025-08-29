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
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.BigQuery;
using Apache.Arrow.Ipc;
using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Grpc.Core;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    public class BigQueryStatementTests
    {
        [Fact]
        public void ReadChunkWithRetries_CalledMoreThanOnce()
        {
            TokenProtectedReadClientManger clientMgr = GetMockTokenProtectedReadClientManger();
            var mockReadRowsStream = GetMockReadRowsStream(clientMgr);
            mockReadRowsStream.Setup(s => s.GetResponseStream())
                .Throws(new InvalidOperationException("GetAsyncEnumerator can only be called once for a gRPC response stream wrapper."));

            var statement = CreateBigQueryStatementForTest();
            SetupRetryValues(statement);

            // this should remain an issue because it indicates we aren't doing something correctly
            // due to the setup, it looks like:
            //----System.Reflection.TargetInvocationException : Exception has been thrown by the target of an invocation.
            //--------Apache.Arrow.Adbc.AdbcException : Cannot execute<ReadChunkWithRetries>b__0 after 5 tries.Last exception: InvalidOperationException: GetAsyncEnumerator can only be called once for a gRPC response stream wrapper.
            //------------ System.InvalidOperationException : GetAsyncEnumerator can only be called once for a gRPC response stream wrapper.

            Assert.Throws<TargetInvocationException>(() => { statement.ReadChunkWithRetriesForTest(clientMgr, "test-stream", null); });
        }

        [Theory]
        [InlineData(true)]  //.MoveNextAsync throws the error
        [InlineData(false)] //.Current throws the error
        public void ReadChunkWithRetries_ThrowsInvalidOperationExceptionOnReadRowsResponse(bool moveNextThrowsError)
        {
            var clientMgr = GetMockTokenProtectedReadClientManger();
            var mockReadRowsStream = GetMockReadRowsStream(clientMgr);

            var mockAsyncResponseStream = new Mock<IAsyncStreamReader<ReadRowsResponse>>();

            if (moveNextThrowsError)
            {
                mockAsyncResponseStream
                    .Setup(s => s.MoveNext(CancellationToken.None))
                    .Throws(new InvalidOperationException("No current element is available."));
            }
            else
            {
                mockAsyncResponseStream
                    .Setup(s => s.MoveNext(CancellationToken.None))
                    .Returns(Task.FromResult(true));

                mockAsyncResponseStream
                    .SetupGet(s => s.Current)
                    .Throws(new InvalidOperationException("No current element is available."));
            }

            AsyncResponseStream<ReadRowsResponse>? mockedResponseStream = typeof(AsyncResponseStream<ReadRowsResponse>)
                .GetConstructor(
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    new Type[] { typeof(IAsyncStreamReader<ReadRowsResponse>) },
                    null)?
                .Invoke(new object[] { mockAsyncResponseStream.Object }) as AsyncResponseStream<ReadRowsResponse>;

            Assert.True(mockedResponseStream != null);

            mockReadRowsStream.Setup(c => c.GetResponseStream())
                .Returns(mockedResponseStream);

            var statement = CreateBigQueryStatementForTest();
            SetupRetryValues(statement);

            var result = statement.ReadChunkWithRetriesForTest(clientMgr, "test-stream", null);
            Assert.Null(result);
        }

        private Mock<BigQueryReadClient.ReadRowsStream> GetMockReadRowsStream(TokenProtectedReadClientManger clientMgr)
        {
            var mockReadClient = new Mock<BigQueryReadClient>(MockBehavior.Strict);
            typeof(TokenProtectedReadClientManger)
                .GetField("bigQueryReadClient", BindingFlags.NonPublic | BindingFlags.Instance)?
                .SetValue(clientMgr, mockReadClient.Object);

            var mockReadRowsStream = new Mock<BigQueryReadClient.ReadRowsStream>();
            mockReadClient
                .Setup(c => c.ReadRows(It.IsAny<ReadRowsRequest>(), null))
                .Returns(mockReadRowsStream.Object);

            return mockReadRowsStream;
        }

        private TokenProtectedReadClientManger GetMockTokenProtectedReadClientManger()
        {
            var credential = GoogleCredential.FromAccessToken("dummy-token");
            var clientMgr = new TokenProtectedReadClientManger(credential);
            return clientMgr;
        }

        private void SetupRetryValues(BigQueryStatement statement)
        {
            var connection = typeof(BigQueryStatement)
                .GetField("bigQueryConnection", BindingFlags.NonPublic | BindingFlags.Instance)?
                .GetValue(statement) as BigQueryConnection;

            if (connection != null)
            {
                typeof(BigQueryConnection)
                    .GetField("maxRetryAttempts", BindingFlags.NonPublic | BindingFlags.Instance)?
                    .SetValue(connection, 2);

                typeof(BigQueryConnection)
                    .GetField("retryDelayMs", BindingFlags.NonPublic | BindingFlags.Instance)?
                    .SetValue(connection, 50);
            }
        }

        private BigQueryStatement CreateBigQueryStatementForTest()
        {
            var properties = new Dictionary<string, string>
            {
                ["projectid"] = "test-project"
            };

            var connection = new BigQueryConnection(properties);
            return new BigQueryStatement(connection);
        }
    }

    public static class BigQueryStatementExtensions
    {
        internal static IArrowReader? ReadChunkWithRetriesForTest(
            this BigQueryStatement statement,
            TokenProtectedReadClientManger clientMgr,
            string streamName,
            Activity? activity)
        {
            var method = typeof(BigQueryStatement).GetMethod(
                "ReadChunkWithRetries",
                BindingFlags.NonPublic | BindingFlags.Instance);

            return (IArrowReader?)method?.Invoke(statement, new object[] { clientMgr, streamName, activity! });
        }
    }
}

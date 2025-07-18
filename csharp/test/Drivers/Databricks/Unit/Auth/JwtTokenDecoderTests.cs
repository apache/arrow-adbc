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
using System.Text;
using System.Text.Json;
using Apache.Arrow.Adbc.Drivers.Databricks.Auth;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit.Auth
{
    public class JwtTokenDecoderTests
    {
        [Fact]
        public void TryGetExpirationTime_ValidToken_ReturnsTrue()
        {
            string token = CreateTestToken(DateTime.UtcNow.AddMinutes(30));

            bool result = JwtTokenDecoder.TryGetExpirationTime(token, out DateTime expiryTime);

            Assert.True(result);
            Assert.True(expiryTime > DateTime.UtcNow);
            Assert.True(expiryTime < DateTime.UtcNow.AddMinutes(31));
        }

        [Fact]
        public void TryGetExpirationTime_ExpiredToken_ReturnsTrue()
        {
            string token = CreateTestToken(DateTime.UtcNow.AddMinutes(-30));

            bool result = JwtTokenDecoder.TryGetExpirationTime(token, out DateTime expiryTime);

            Assert.True(result);
            Assert.True(expiryTime < DateTime.UtcNow);
            Assert.True(expiryTime > DateTime.UtcNow.AddMinutes(-31));
        }

        [Fact]
        public void TryGetExpirationTime_InvalidToken_ReturnsFalse()
        {
            string token = "invalid.token.format";

            bool result = JwtTokenDecoder.TryGetExpirationTime(token, out DateTime expiryTime);

            Assert.False(result);
            Assert.Equal(DateTime.MinValue, expiryTime);
        }

        [Fact]
        public void TryGetExpirationTime_MissingExpClaim_ReturnsFalse()
        {
            string token = CreateTestTokenWithoutExpClaim();

            bool result = JwtTokenDecoder.TryGetExpirationTime(token, out DateTime expiryTime);

            Assert.False(result);
            Assert.Equal(DateTime.MinValue, expiryTime);
        }

        private string CreateTestToken(DateTime expiryTime)
        {
            // Create a simple JWT token with expiration claim
            var header = new { alg = "HS256", typ = "JWT" };
            var payload = new { exp = ((DateTimeOffset)expiryTime).ToUnixTimeSeconds() };

            string headerJson = JsonSerializer.Serialize(header);
            string payloadJson = JsonSerializer.Serialize(payload);

            string headerBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(headerJson))
                .Replace('+', '-')
                .Replace('/', '_')
                .TrimEnd('=');

            string payloadBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(payloadJson))
                .Replace('+', '-')
                .Replace('/', '_')
                .TrimEnd('=');

            // For testing purposes, we don't need a valid signature
            string signature = "signature";

            return $"{headerBase64}.{payloadBase64}.{signature}";
        }

        private string CreateTestTokenWithoutExpClaim()
        {
            // Create a simple JWT token without expiration claim
            var header = new { alg = "HS256", typ = "JWT" };
            var payload = new { sub = "test" };

            string headerJson = JsonSerializer.Serialize(header);
            string payloadJson = JsonSerializer.Serialize(payload);

            string headerBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(headerJson))
                .Replace('+', '-')
                .Replace('/', '_')
                .TrimEnd('=');

            string payloadBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(payloadJson))
                .Replace('+', '-')
                .Replace('/', '_')
                .TrimEnd('=');

            // For testing purposes, we don't need a valid signature
            string signature = "signature";

            return $"{headerBase64}.{payloadBase64}.{signature}";
        }
    }
}
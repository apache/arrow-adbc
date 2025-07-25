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
            string token = CreateTestToken(expiryTime: DateTime.UtcNow.AddMinutes(30));

            bool result = JwtTokenDecoder.TryGetExpirationTime(token, out DateTime expiryTime);

            Assert.True(result);
            Assert.True(expiryTime > DateTime.UtcNow);
            Assert.True(expiryTime < DateTime.UtcNow.AddMinutes(31));
        }

        [Fact]
        public void TryGetExpirationTime_ExpiredToken_ReturnsTrue()
        {
            string token = CreateTestToken(expiryTime: DateTime.UtcNow.AddMinutes(-30));

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
            string token = CreateTestToken(expiryTime: null);

            bool result = JwtTokenDecoder.TryGetExpirationTime(token, out DateTime expiryTime);

            Assert.False(result);
            Assert.Equal(DateTime.MinValue, expiryTime);
        }

        [Fact]
        public void TryGetIssuer_ValidToken_ReturnsTrue()
        {
            string expectedIssuer = "https://test.databricks.com/oidc";
            string token = CreateTestToken(issuer: expectedIssuer);

            bool result = JwtTokenDecoder.TryGetIssuer(token, out string issuer);

            Assert.True(result);
            Assert.Equal(expectedIssuer, issuer);
        }

        [Fact]
        public void TryGetIssuer_InvalidToken_ReturnsFalse()
        {
            string token = "invalid.token.format";

            bool result = JwtTokenDecoder.TryGetIssuer(token, out string issuer);

            Assert.False(result);
            Assert.Empty(issuer);
        }

        [Fact]
        public void TryGetIssuer_MissingIssClaim_ReturnsFalse()
        {
            string token = CreateTestToken(issuer: null);

            bool result = JwtTokenDecoder.TryGetIssuer(token, out string issuer);

            Assert.False(result);
            Assert.Empty(issuer);
        }

        [Fact]
        public void TryGetIssuer_EmptyIssuer_ReturnsFalse()
        {
            string token = CreateTestToken(issuer: "");

            bool result = JwtTokenDecoder.TryGetIssuer(token, out string issuer);

            Assert.False(result);
            Assert.Empty(issuer);
        }

        [Fact]
        public void TryGetIssuer_TokenWithOnlyTwoParts_ReturnsFalse()
        {
            string token = "header.payload"; // Missing signature part

            bool result = JwtTokenDecoder.TryGetIssuer(token, out string issuer);

            Assert.False(result);
            Assert.Empty(issuer);
        }

        [Fact]
        public void TryGetIssuer_MalformedBase64_ReturnsFalse()
        {
            string token = "invalid!base64.invalid!base64.signature";

            bool result = JwtTokenDecoder.TryGetIssuer(token, out string issuer);

            Assert.False(result);
            Assert.Empty(issuer);
        }

        private string CreateTestToken(
            string? issuer = null,
            DateTime? expiryTime = null)
        {
            // Create a simple JWT token with optional claims
            var header = new { alg = "HS256", typ = "JWT" };

            // Build payload dynamically based on parameters
            var payloadDict = new Dictionary<string, object> { { "sub", "test" } };

            if (issuer != null)
            {
                payloadDict["iss"] = issuer;
            }

            if (expiryTime.HasValue)
            {
                payloadDict["exp"] = ((DateTimeOffset)expiryTime.Value).ToUnixTimeSeconds();
            }

            string headerJson = JsonSerializer.Serialize(header);
            string payloadJson = JsonSerializer.Serialize(payloadDict);

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

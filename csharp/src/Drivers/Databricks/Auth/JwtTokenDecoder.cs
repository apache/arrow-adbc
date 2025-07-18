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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Auth
{
    /// <summary>
    /// Utility class for decoding JWT tokens and extracting claims.
    /// </summary>
    internal static class JwtTokenDecoder
    {
        /// <summary>
        /// Tries to parse a JWT token and extract its expiration time.
        /// </summary>
        /// <param name="token">The JWT token to parse.</param>
        /// <param name="expiryTime">The extracted expiration time, if successful.</param>
        /// <returns>True if the expiration time was successfully extracted, false otherwise.</returns>
        public static bool TryGetExpirationTime(string token, out DateTime expiryTime)
        {
            expiryTime = DateTime.MinValue;

            try
            {
                // JWT tokens have three parts separated by dots: header.payload.signature
                string[] parts = token.Split('.');
                if (parts.Length != 3)
                {
                    return false;
                }

                string payload = DecodeBase64Url(parts[1]);

                using JsonDocument jsonDoc = JsonDocument.Parse(payload);

                if (!jsonDoc.RootElement.TryGetProperty("exp", out JsonElement expElement))
                {
                    return false;
                }

                // The exp claim is a Unix timestamp (seconds since epoch)
                if (!expElement.TryGetInt64(out long expSeconds))
                {
                    return false;
                }

                expiryTime = DateTimeOffset.FromUnixTimeSeconds(expSeconds).UtcDateTime;
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Decodes a base64url encoded string to a regular string.
        /// </summary>
        /// <param name="base64Url">The base64url encoded string.</param>
        /// <returns>The decoded string.</returns>
        private static string DecodeBase64Url(string base64Url)
        {
            // Convert base64url to base64
            string base64 = base64Url
                .Replace('-', '+')
                .Replace('_', '/');

            // Add padding if needed
            switch (base64.Length % 4)
            {
                case 2: base64 += "=="; break;
                case 3: base64 += "="; break;
            }

            byte[] bytes = Convert.FromBase64String(base64);

            return Encoding.UTF8.GetString(bytes);
        }
    }
}

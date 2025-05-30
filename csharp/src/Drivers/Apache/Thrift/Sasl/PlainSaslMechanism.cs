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

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    /// <summary>
    /// Implements the SASL PLAIN mechanism for simple username/password authentication.
    /// </summary>
    internal class PlainSaslMechanism : ISaslMechanism
    {
        private readonly string _username;
        private readonly string _password;
        private readonly string _authorizationId;
        private bool _isNegotiationCompleted;

        /// <summary>
        /// Initializes a new instance of the <see cref="PlainSaslMechanism"/> class.
        /// </summary>
        /// <param name="username">The username for authentication.</param>
        /// <param name="password">The password for authentication.</param>
        public PlainSaslMechanism(string username, string password, string authorizationId = "")
        {
            _username = username ?? throw new ArgumentNullException(nameof(username));
            _password = password ?? throw new ArgumentNullException(nameof(password));
            _authorizationId = authorizationId;
        }

        public string Name => "PLAIN";

        public byte[] EvaluateChallenge(byte[]? challenge)
        {
            if (_isNegotiationCompleted)
            {
                // PLAIN is single-step, so return empty array if already done
                return [];
            }

            string message = $"{_authorizationId}\0{_username}\0{_password}";
            _isNegotiationCompleted = true;
            return Encoding.UTF8.GetBytes(message);
        }

        public bool IsNegotiationCompleted
        {
            get => _isNegotiationCompleted;
            set => _isNegotiationCompleted = value;
        }
    }
}

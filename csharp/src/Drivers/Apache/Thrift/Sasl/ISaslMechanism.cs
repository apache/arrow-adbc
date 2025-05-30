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

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    /// <summary>
    /// Defines the contract for implementing SASL authentication mechanisms (e.g., PLAIN, GSSAPI).
    /// This interface allows the client to authenticate over a Thrift transport using the selected SASL mechanism.
    /// </summary>
    internal interface ISaslMechanism
    {
        /// <summary>
        /// Gets the name of the SASL mechanism (e.g., "PLAIN", "GSSAPI").
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Evaluates the challenge from the server and returns the appropriate response payload.
        /// </summary>
        /// <param name="challenge">The server challenge; may be null or empty for mechanisms like PLAIN.</param>
        /// <returns>The response payload to send to the server.</returns>
        byte[] EvaluateChallenge(byte[]? challenge);

        /// <summary>
        /// Gets a value indicating whether the SASL negotiation process has completed.
        /// </summary>
        bool IsNegotiationCompleted { get; set; }
    }
}

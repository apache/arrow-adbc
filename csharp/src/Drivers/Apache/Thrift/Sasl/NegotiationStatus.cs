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
    /// Represents the status of a SASL negotiation between client and server.
    /// </summary>
    internal sealed class NegotiationStatus
    {
        public static readonly NegotiationStatus Start = new NegotiationStatus(0x01, nameof(Start));
        public static readonly NegotiationStatus Ok = new NegotiationStatus(0x02, nameof(Ok));
        public static readonly NegotiationStatus Bad = new NegotiationStatus(0x03, nameof(Bad));
        public static readonly NegotiationStatus Error = new NegotiationStatus(0x04, nameof(Error));
        public static readonly NegotiationStatus Complete = new NegotiationStatus(0x05, nameof(Complete));

        /// <summary>
        /// Gets the byte value representing the negotiation status.
        /// </summary>
        public byte Value { get; }

        /// <summary>
        /// Gets the name of the status (e.g., "Start", "Ok").
        /// </summary>
        public string Name { get; }

        private NegotiationStatus(byte value, string name)
        {
            Value = value;
            Name = name;
        }

        /// <summary>
        /// Gets a <see cref="NegotiationStatus"/> instance by its byte value.
        /// </summary>
        /// <param name="value">The byte value of the status.</param>
        /// <returns>The corresponding <see cref="NegotiationStatus"/>, or null if unknown.</returns>
        public static NegotiationStatus? FromValue(byte value)
        {
            return value switch
            {
                0x01 => Start,
                0x02 => Ok,
                0x03 => Bad,
                0x04 => Error,
                0x05 => Complete,
                _ => null
            };
        }
    }
}

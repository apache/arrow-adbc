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
using System.Security.Authentication;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Thrift;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    internal class TSaslTransport : TEndpointTransport
    {
        private readonly TTransport _innerTransport;
        private readonly ISaslMechanism _saslMechanism;
        private readonly TConfiguration _configuration;
        private byte[] messageHeader = new byte[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];

        protected const int MECHANISM_NAME_BYTES = 1;
        protected const int STATUS_BYTES = 1;
        protected const int PAYLOAD_LENGTH_BYTES = 4;

        public TSaslTransport(TTransport innerTransport, ISaslMechanism saslMechanism, TConfiguration config)
            : base(config)
        {
            _innerTransport = innerTransport ?? throw new ArgumentNullException(nameof(innerTransport));
            _saslMechanism = saslMechanism ?? throw new ArgumentNullException(nameof(saslMechanism));
            _configuration = config ?? new TConfiguration();
        }

        public override bool IsOpen => _innerTransport.IsOpen;

        public override async Task OpenAsync(CancellationToken cancellationToken = default)
        {
            // Open the transport and negotiate SASL authentication
            await _innerTransport.OpenAsync(cancellationToken).ConfigureAwait(false);
            await NegotiateAsync(cancellationToken).ConfigureAwait(false);
        }

        public override void Close()
        {
            _innerTransport.Close();
        }

        public override ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
        {
            return _innerTransport.ReadAsync(buffer, offset, length, cancellationToken);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
        {
            return _innerTransport.WriteAsync(buffer, offset, length, cancellationToken);
        }

        public override Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return _innerTransport.FlushAsync(cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _innerTransport.Dispose();
            }
        }

        private async Task NegotiateAsync(CancellationToken cancellationToken)
        {
            // Send the SASL mechanism name
            await SendMechanismAsync(_saslMechanism.Name, cancellationToken).ConfigureAwait(false);

            // Send the authentication message
            var authMessage = _saslMechanism.EvaluateChallenge(null);
            await SendSaslMessageAsync(NegotiationStatus.Ok, authMessage, cancellationToken).ConfigureAwait(false);

            // Receive server's response (authentication status)
            var serverResponse = await ReceiveSaslMessageAsync(cancellationToken).ConfigureAwait(false);

            if (serverResponse.status == null || serverResponse.status != NegotiationStatus.Complete)
            {
                throw new AuthenticationException($"SASL {_saslMechanism.Name} authentication failed.");
            }

            _saslMechanism.IsNegotiationCompleted = true;
        }

        private async Task SendMechanismAsync(string mechanismName, CancellationToken cancellationToken)
        {
            // Send the mechanism name to the server
            byte[] mechanismNameBytes = Encoding.UTF8.GetBytes(mechanismName);
            await SendSaslMessageAsync(NegotiationStatus.Start, mechanismNameBytes, cancellationToken);
        }

        protected async Task SendSaslMessageAsync(NegotiationStatus status, byte[] payload, CancellationToken cancellationToken)
        {
            payload ??= [];
            // Set status byte
            messageHeader[0] = status.Value;
            // Encode payload length
            EncodeBigEndian(payload.Length, messageHeader, STATUS_BYTES);
            await _innerTransport.WriteAsync(messageHeader, 0, messageHeader.Length);
            await _innerTransport.WriteAsync(payload, 0, payload.Length);
            await _innerTransport.FlushAsync(cancellationToken);
        }

        protected async Task<SaslResponse> ReceiveSaslMessageAsync(CancellationToken cancellationToken = default)
        {
            await _innerTransport.ReadAllAsync(messageHeader, 0, messageHeader.Length, cancellationToken);

            byte statusByte = messageHeader[0];

            NegotiationStatus? status = NegotiationStatus.FromValue(statusByte);
            if (status == null)
            {
                throw new TTransportException("Received invalid SASL negotiation status. The status byte was null.");
            }

            int payloadBytes = DecodeBigEndian(messageHeader, STATUS_BYTES);
            if (payloadBytes < 0 || payloadBytes > _configuration.MaxMessageSize)
            {
                throw new TTransportException($"Received payload size out of range: {payloadBytes}. Expected between 0 and {new TConfiguration().MaxMessageSize}.");
            }

            byte[] payload = new byte[payloadBytes];
            await _innerTransport.ReadAllAsync(payload, 0, payload.Length, cancellationToken);

            if (status == NegotiationStatus.Bad || status == NegotiationStatus.Error)
            {
                string remoteMessage = Encoding.UTF8.GetString(payload);
                throw new TTransportException($"Peer indicated failure: {remoteMessage}");
            }

            return new SaslResponse(status, payload);
        }

        private int DecodeBigEndian(byte[] buf, int offset)
        {
            return ((buf[offset] & 0xff) << 24)
                 | ((buf[offset + 1] & 0xff) << 16)
                 | ((buf[offset + 2] & 0xff) << 8)
                 | (buf[offset + 3] & 0xff);
        }

        private void EncodeBigEndian(int value, byte[] buf, int offset)
        {
            buf[offset] = (byte)((value >> 24) & 0xff);
            buf[offset + 1] = (byte)((value >> 16) & 0xff);
            buf[offset + 2] = (byte)((value >> 8) & 0xff);
            buf[offset + 3] = (byte)(value & 0xff);
        }
    }

    internal class SaslResponse
    {
        public NegotiationStatus status;
        public byte[] payload;

        public SaslResponse(NegotiationStatus status, byte[] payload)
        {
            this.status = status;
            this.payload = payload;
        }
    }
}

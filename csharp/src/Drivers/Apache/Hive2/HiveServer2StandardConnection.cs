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
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2StandardConnection : HiveServer2ExtendedConnection
    {
        public HiveServer2StandardConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        protected override void ValidateAuthentication()
        {
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(HiveServer2Parameters.AuthType, out string? authType);
            if (!HiveServer2AuthTypeParser.TryParse(authType, out HiveServer2AuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(HiveServer2Parameters.AuthType, authType, $"Unsupported {HiveServer2Parameters.AuthType} value.");
            }
            switch (authTypeValue)
            {
                case HiveServer2AuthType.None:
                    break;
                case HiveServer2AuthType.Basic:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameter '{HiveServer2Parameters.AuthType}' is set to '{HiveServer2AuthTypeConstants.Basic}' but parameters '{AdbcOptions.Username}' or '{AdbcOptions.Password}' are not set. Please provide a values for these parameters.",
                            nameof(Properties));
                    break;
                case HiveServer2AuthType.Empty:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameters must include valid authentiation settings. Please provide '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.",
                            nameof(Properties));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(HiveServer2Parameters.AuthType, authType, $"Unsupported {HiveServer2Parameters.AuthType} value.");
            }
        }

        protected override void ValidateConnection()
        {
            // HostName is required parameter
            Properties.TryGetValue(HiveServer2Parameters.HostName, out string? hostName);
            if (Uri.CheckHostName(hostName) == UriHostNameType.Unknown)
            {
                throw new ArgumentException(
                    $"Required parameter '{HiveServer2Parameters.HostName}' is missing or invalid. Please provide a valid hostname for the data source.",
                    nameof(Properties));
            }

            // Validate port range
            Properties.TryGetValue(HiveServer2Parameters.Port, out string? port);
            if (int.TryParse(port, out int portNumber) && (portNumber <= IPEndPoint.MinPort || portNumber > IPEndPoint.MaxPort))
                throw new ArgumentOutOfRangeException(
                    nameof(Properties),
                    port,
                    $"Parameter '{HiveServer2Parameters.Port}' value is not in the valid range of {IPEndPoint.MinPort + 1} .. {IPEndPoint.MaxPort}.");
        }

        protected override void ValidateOptions()
        {
            Properties.TryGetValue(HiveServer2Parameters.DataTypeConv, out string? dataTypeConv);
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);
            TlsOptions = HiveServer2TlsImpl.GetStandardTlsOptions(Properties);
        }

        protected override TTransport CreateTransport()
        {
            // Required properties (validated previously)
            Properties.TryGetValue(HiveServer2Parameters.HostName, out string? hostName);
            Properties.TryGetValue(HiveServer2Parameters.Port, out string? port);
            Properties.TryGetValue(HiveServer2Parameters.AuthType, out string? authType);

            if (!HiveServer2AuthTypeParser.TryParse(authType, out HiveServer2AuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(HiveServer2Parameters.AuthType, authType, $"Unsupported {HiveServer2Parameters.AuthType} value.");
            }

            // Delay the open connection until later.
            bool connectClient = false;
            int portValue = int.Parse(port!);

            // TLS setup
            TTransport baseTransport;
            if (TlsOptions.IsTlsEnabled)
            {
                X509Certificate2? trustedCert = !string.IsNullOrEmpty(TlsOptions.TrustedCertificatePath)
                    ? new X509Certificate2(TlsOptions.TrustedCertificatePath!)
                    : null;

                RemoteCertificateValidationCallback certValidator = (sender, cert, chain, errors) => HiveServer2TlsImpl.ValidateCertificate(cert, errors, TlsOptions);

                if (IPAddress.TryParse(hostName!, out var ipAddress))
                {
                    baseTransport = new TTlsSocketTransport(ipAddress, portValue, config: new(), 0, trustedCert, certValidator);
                }
                else
                {
                    baseTransport = new TTlsSocketTransport(hostName!, portValue, config: new(), 0, trustedCert, certValidator);
                }
            }
            else
            {
                baseTransport = new TSocketTransport(hostName!, portValue, connectClient, config: new());
            }

            TBufferedTransport bufferedTransport = new TBufferedTransport(baseTransport);
            switch (authTypeValue)
            {
                case HiveServer2AuthType.None:
                    return bufferedTransport;

                case HiveServer2AuthType.Basic:
                    Properties.TryGetValue(AdbcOptions.Username, out string? username);
                    Properties.TryGetValue(AdbcOptions.Password, out string? password);

                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                    {
                        throw new InvalidOperationException("Username and password must be provided for this authentication type.");
                    }

                    PlainSaslMechanism saslMechanism = new(username, password);
                    TSaslTransport saslTransport = new(bufferedTransport, saslMechanism, config: new());
                    return new TFramedTransport(saslTransport);

                default:
                    throw new NotSupportedException($"Authentication type '{authTypeValue}' is not supported.");
            }
        }

        protected override async Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default)
        {
            if (!transport.IsOpen) await transport.OpenAsync(cancellationToken);
            return new TBinaryProtocol(transport, true, true);
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            TOpenSessionReq request = new TOpenSessionReq
            {
                Client_protocol = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
                CanUseMultipleCatalogs = true,
            };
            return request;
        }

        protected override IEnumerable<TProtocolVersion> FallbackProtocolVersions => new[]
        {
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7
        };

        protected override HiveServer2TransportType Type => HiveServer2TransportType.Standard;

        public override string AssemblyName => s_assemblyName;

        public override string AssemblyVersion => s_assemblyVersion;
    }
}

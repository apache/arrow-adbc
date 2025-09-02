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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkStandardConnection : SparkConnection
    {
        public SparkStandardConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        protected override void ValidateAuthentication()
        {
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            if (!SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
            }
            switch (authTypeValue)
            {
                case SparkAuthType.None:
                    break;
                case SparkAuthType.UsernameOnly:
                    if (string.IsNullOrWhiteSpace(username))
                        throw new ArgumentException(
                            $"Parameter '{SparkParameters.AuthType}' is set to '{SparkAuthTypeConstants.UsernameOnly}' but parameters '{AdbcOptions.Username}' is not set. Please provide a value for this parameter.",
                            nameof(Properties));
                    break;
                case SparkAuthType.Basic:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameter '{SparkParameters.AuthType}' is set to '{SparkAuthTypeConstants.Basic}' but parameters '{AdbcOptions.Username}' or '{AdbcOptions.Password}' are not set. Please provide a values for these parameters.",
                            nameof(Properties));
                    break;
                case SparkAuthType.Empty:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameters must include valid authentiation settings. Please provide either '{SparkParameters.Token}'; or '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.",
                            nameof(Properties));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
            }
        }

        protected override void ValidateConnection()
        {
            // HostName is required parameter
            Properties.TryGetValue(SparkParameters.HostName, out string? hostName);
            if (Uri.CheckHostName(hostName) == UriHostNameType.Unknown)
            {
                throw new ArgumentException(
                    $"Required parameter '{SparkParameters.HostName}' is missing or invalid. Please provide a valid hostname for the data source.",
                    nameof(Properties));
            }

            // Validate port range
            Properties.TryGetValue(SparkParameters.Port, out string? port);
            if (int.TryParse(port, out int portNumber) && (portNumber <= IPEndPoint.MinPort || portNumber > IPEndPoint.MaxPort))
                throw new ArgumentOutOfRangeException(
                    nameof(Properties),
                    port,
                    $"Parameter '{SparkParameters.Port}' value is not in the valid range of 1 .. {IPEndPoint.MaxPort}.");

        }

        protected override TTransport CreateTransport()
        {
            // Assumption: hostName and port have already been validated.
            Properties.TryGetValue(SparkParameters.HostName, out string? hostName);
            Properties.TryGetValue(SparkParameters.Port, out string? port);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);

            if (!SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
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
                case SparkAuthType.None:
                    return bufferedTransport;

                case SparkAuthType.Basic:
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
            // Assumption: user name and password have already been validated.
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            if (!SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
            }
            TOpenSessionReq request = new TOpenSessionReq
            {
                Client_protocol = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7,
                CanUseMultipleCatalogs = true,
            };
            switch (authTypeValue)
            {
                case SparkAuthType.UsernameOnly:
                case SparkAuthType.Basic:
                case SparkAuthType.Empty when !string.IsNullOrEmpty(username):
                    request.Username = username!;
                    break;
            }
            switch (authTypeValue)
            {
                case SparkAuthType.Basic:
                case SparkAuthType.Empty when !string.IsNullOrEmpty(password):
                    request.Password = password!;
                    break;
            }
            return request;
        }

        protected override void ValidateOptions()
        {
            Properties.TryGetValue(SparkParameters.DataTypeConv, out string? dataTypeConv);
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);
            TlsOptions = HiveServer2TlsImpl.GetStandardTlsOptions(Properties);
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, IResponse response, TGetResultSetMetadataResp? metadataResp = null) =>
            new HiveServer2Reader(statement, schema, response, dataTypeConversion: statement.Connection.DataTypeConversion);


        internal override SparkServerType ServerType => SparkServerType.Standard;

        public override string AssemblyName => s_assemblyName;

        public override string AssemblyVersion => s_assemblyVersion;

        protected override int ColumnMapIndexOffset => 0;
    }
}

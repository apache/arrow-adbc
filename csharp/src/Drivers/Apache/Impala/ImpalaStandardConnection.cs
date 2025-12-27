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
using System.Net;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal class ImpalaStandardConnection : ImpalaConnection
    {
        public ImpalaStandardConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        protected override void ValidateAuthentication()
        {
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(ImpalaParameters.AuthType, out string? authType);
            if (!ImpalaAuthTypeParser.TryParse(authType, out ImpalaAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(ImpalaParameters.AuthType, authType, $"Unsupported {ImpalaParameters.AuthType} value.");
            }
            switch (authTypeValue)
            {
                case ImpalaAuthType.None:
                    break;
                case ImpalaAuthType.UsernameOnly:
                    if (string.IsNullOrWhiteSpace(username))
                        throw new ArgumentException(
                            $"Parameter '{ImpalaParameters.AuthType}' is set to '{ImpalaAuthTypeConstants.UsernameOnly}' but parameters '{AdbcOptions.Username}' is not set. Please provide a value for this parameter.",
                            nameof(Properties));
                    break;
                case ImpalaAuthType.Basic:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameter '{ImpalaParameters.AuthType}' is set to '{ImpalaAuthTypeConstants.Basic}' but parameters '{AdbcOptions.Username}' or '{AdbcOptions.Password}' are not set. Please provide a values for these parameters.",
                            nameof(Properties));
                    break;
                case ImpalaAuthType.Empty:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameters must include valid authentiation settings. Please provide '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.",
                            nameof(Properties));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(ImpalaParameters.AuthType, authType, $"Unsupported {ImpalaParameters.AuthType} value.");
            }
        }

        protected override void ValidateConnection()
        {
            // HostName is required parameter
            Properties.TryGetValue(ImpalaParameters.HostName, out string? hostName);
            if (Uri.CheckHostName(hostName) == UriHostNameType.Unknown)
            {
                throw new ArgumentException(
                    $"Required parameter '{ImpalaParameters.HostName}' is missing or invalid. Please provide a valid hostname for the data source.",
                    nameof(Properties));
            }

            // Validate port range
            Properties.TryGetValue(ImpalaParameters.Port, out string? port);
            if (int.TryParse(port, out int portNumber) && (portNumber <= IPEndPoint.MinPort || portNumber > IPEndPoint.MaxPort))
                throw new ArgumentOutOfRangeException(
                    nameof(Properties),
                    port,
                    $"Parameter '{ImpalaParameters.Port}' value is not in the valid range of {IPEndPoint.MinPort + 1} .. {IPEndPoint.MaxPort}.");
        }

        protected override void ValidateOptions()
        {
            Properties.TryGetValue(ImpalaParameters.DataTypeConv, out string? dataTypeConv);
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);
            TlsOptions = HiveServer2TlsImpl.GetStandardTlsOptions(Properties);
        }

        protected override TTransport CreateTransport()
        {
            Activity? activity = Activity.Current;

            // Assumption: hostName and port have already been validated.
            Properties.TryGetValue(ImpalaParameters.HostName, out string? hostName);
            Properties.TryGetValue(ImpalaParameters.Port, out string? port);
            Properties.TryGetValue(ImpalaParameters.AuthType, out string? authType);
            if (!ImpalaAuthTypeParser.TryParse(authType, out ImpalaAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(ImpalaParameters.AuthType, authType, $"Unsupported {ImpalaParameters.AuthType} value.");
            }

            // Delay the open connection until later.
            bool connectClient = false;
            TConfiguration thriftConfig = GetTconfiguration();
            TTransport transport;
            if (TlsOptions.IsTlsEnabled)
            {
                RemoteCertificateValidationCallback certValidator = (sender, cert, chain, errors) => HiveServer2TlsImpl.ValidateCertificate(cert, errors, TlsOptions);
                if (IPAddress.TryParse(hostName!, out var address))
                {
                    transport = new TTlsSocketTransport(address!, int.Parse(port!), config: thriftConfig, 0, null, certValidator: certValidator);
                }
                else
                {
                    transport = new TTlsSocketTransport(hostName!, int.Parse(port!), config: thriftConfig, 0, null, certValidator: certValidator);
                }
                activity?.AddTag(ActivityKeys.Encrypted, true.ToString());
            }
            else
            {
                transport = new TSocketTransport(hostName!, int.Parse(port!), connectClient, config: thriftConfig);
                activity?.AddTag(ActivityKeys.Encrypted, false.ToString());
            }
            activity?.AddTag(ActivityKeys.Host, hostName);
            activity?.AddTag(ActivityKeys.Port, port);

            TBufferedTransport bufferedTransport = new(transport);
            switch (authTypeValue)
            {
                case ImpalaAuthType.None:
                    activity?.AddTag(ActivityKeys.TransportType, "buffered_socket");
                    return bufferedTransport;

                case ImpalaAuthType.Basic:
                    Properties.TryGetValue(AdbcOptions.Username, out string? username);
                    Properties.TryGetValue(AdbcOptions.Password, out string? password);
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                    {
                        throw new InvalidOperationException("Username and password must be provided for this authentication type.");
                    }

                    PlainSaslMechanism saslMechanism = new(username, password);
                    TSaslTransport saslTransport = new(bufferedTransport, saslMechanism, config: thriftConfig);
                    activity?.AddTag(ActivityKeys.TransportType, "sasl_buffered_socket");
                    return new TFramedTransport(saslTransport);

                default:
                    throw new NotSupportedException($"Authentication type '{authTypeValue}' is not supported.");
            }
        }

        protected override async Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default)
        {
            if (!transport.IsOpen) await transport.OpenAsync(cancellationToken);
            return new TBinaryProtocol(transport);
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            Activity? activity = Activity.Current;

            // Assumption: user name and password have already been validated.
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(ImpalaParameters.AuthType, out string? authType);
            if (!ImpalaAuthTypeParser.TryParse(authType, out ImpalaAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(ImpalaParameters.AuthType, authType, $"Unsupported {ImpalaParameters.AuthType} value.");
            }
            TOpenSessionReq request = new TOpenSessionReq
            {
                Client_protocol = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7,
                CanUseMultipleCatalogs = true,
            };
            switch (authTypeValue)
            {
                case ImpalaAuthType.UsernameOnly:
                case ImpalaAuthType.Basic:
                case ImpalaAuthType.Empty when !string.IsNullOrEmpty(username):
                    request.Username = username!;
                    break;
            }
            switch (authTypeValue)
            {
                case ImpalaAuthType.Basic:
                case ImpalaAuthType.Empty when !string.IsNullOrEmpty(password):
                    request.Password = password!;
                    break;
            }

            authTypeValue = authTypeValue == ImpalaAuthType.Empty && !string.IsNullOrEmpty(username)
                ? !string.IsNullOrEmpty(password)
                    ? ImpalaAuthType.Basic
                    : ImpalaAuthType.UsernameOnly
                : authTypeValue;
            activity?.AddTag(ActivityKeys.AuthType, authTypeValue.ToString());
            return request;
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, IResponse response, TGetResultSetMetadataResp? metadataResp = null) =>
            new HiveServer2Reader(statement, schema, response, dataTypeConversion: statement.Connection.DataTypeConversion);

        internal override ImpalaServerType ServerType => ImpalaServerType.Standard;

        protected override int ColumnMapIndexOffset => 0;

        public override string AssemblyName => s_assemblyName;

        public override string AssemblyVersion => s_assemblyVersion;
    }
}

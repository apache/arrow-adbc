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
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Thrift.Sasl;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2SaslConnection : HiveServer2Connection
    {

        private const string ProductVersionDefault = "1.0.0";
        private const string DriverName = "ADBC Hive Driver";
        private const string ArrowVersion = "1.0.0";
        private readonly Lazy<string> _productVersion;

        protected override string GetProductVersionDefault() => ProductVersionDefault;

        protected override string ProductVersion => _productVersion.Value;

        internal override SchemaParser SchemaParser => new HiveServer2SchemaParser();

        public HiveServer2SaslConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
            ValidateProperties();
            _productVersion = new Lazy<string>(() => GetProductVersion(), LazyThreadSafetyMode.PublicationOnly);
        }

        private void ValidateProperties()
        {
            ValidateAuthentication();
            ValidateConnection();
            ValidateOptions();
        }

        protected void ValidateAuthentication()
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
                case HiveServer2AuthType.UsernameOnly:
                    if (string.IsNullOrWhiteSpace(username))
                        throw new ArgumentException(
                            $"Parameter '{HiveServer2Parameters.AuthType}' is set to '{HiveServer2AuthTypeConstants.UsernameOnly}' but parameters '{AdbcOptions.Username}' is not set. Please provide a value for this parameter.",
                            nameof(Properties));
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

        protected void ValidateConnection()
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

        protected void ValidateOptions()
        {
            Properties.TryGetValue(HiveServer2Parameters.DataTypeConv, out string? dataTypeConv);
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);
            TlsOptions = HiveServer2TlsImpl.GetStandardTlsOptions(Properties);
        }

        protected override TTransport CreateTransport()
        {
            // Assumption: hostName and port have already been validated.
            Properties.TryGetValue(HiveServer2Parameters.HostName, out string? hostName);
            Properties.TryGetValue(HiveServer2Parameters.Port, out string? port);
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);

            // Delay the open connection until later.
            bool connectClient = false;
            TTransport transport;
            if (TlsOptions.IsTlsEnabled)
            {
                if (IPAddress.TryParse(hostName!, out var address))
                {
                    transport = new TTlsSocketTransport(address!, int.Parse(port!), config: new(), 0, !string.IsNullOrEmpty(TlsOptions.TrustedCertificatePath) ? new X509Certificate2(TlsOptions.TrustedCertificatePath!) : null, certValidator: HiveServer2TlsImpl.GetCertificateValidator(TlsOptions));
                }
                else
                {
                    transport = new TTlsSocketTransport(hostName!, int.Parse(port!), config: new(), 0, !string.IsNullOrEmpty(TlsOptions.TrustedCertificatePath) ? new X509Certificate2(TlsOptions.TrustedCertificatePath!) : null, certValidator: HiveServer2TlsImpl.GetCertificateValidator(TlsOptions));
                }
            }
            else
            {
                transport = new TSocketTransport(hostName!, int.Parse(port!), connectClient, config: new());
            }
            //transport = new TSocketTransport(hostName!, int.Parse(port!), connectClient, config: new());
            if(username != null && password != null)
            {
                PlainSaslMechanism plainSaslMechanism = new PlainSaslMechanism(username, password);
                TSaslTransport sasltransport = new TSaslTransport(transport, plainSaslMechanism, config: new());
                TFramedTransport framedTransport = new(sasltransport);
                return framedTransport;
            }
            return transport;
            //TODO correction here
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

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected internal override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetPrimaryKeysResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected internal override Task<TRowSet> GetRowSetAsync(TGetPrimaryKeysResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);

        protected internal override int PositionRequiredOffset => 0;

        protected override string InfoDriverName => DriverName;

        protected override string InfoDriverArrowVersion => ArrowVersion;

        protected override bool IsColumnSizeValidForDecimal => false;

        protected override bool GetObjectsPatternsRequireLowerCase => false;

        public override AdbcStatement CreateStatement()
        {
            return new HiveServer2Statement(this);
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, TGetResultSetMetadataResp? metadataResp = null) => new HiveServer2Reader(statement, schema, dataTypeConversion: statement.Connection.DataTypeConversion, enableBatchSizeStopCondition: false);

        internal override void SetPrecisionScaleAndTypeName(
            short colType,
            string typeName,
            TableInfo? tableInfo,
            int columnSize,
            int decimalDigits)
        {
            // Keep the original type name
            tableInfo?.TypeName.Add(typeName);
            switch (colType)
            {
                case (short)ColumnTypeId.DECIMAL:
                case (short)ColumnTypeId.NUMERIC:
                    {
                        // Precision/scale is provide in the API call.
                        SqlDecimalParserResult result = SqlTypeNameParser<SqlDecimalParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(columnSize);
                        tableInfo?.Scale.Add((short)decimalDigits);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                case (short)ColumnTypeId.CHAR:
                case (short)ColumnTypeId.NCHAR:
                case (short)ColumnTypeId.VARCHAR:
                case (short)ColumnTypeId.LONGVARCHAR:
                case (short)ColumnTypeId.LONGNVARCHAR:
                case (short)ColumnTypeId.NVARCHAR:
                    {
                        // Precision is provide in the API call.
                        SqlCharVarcharParserResult result = SqlTypeNameParser<SqlCharVarcharParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(columnSize);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                default:
                    {
                        SqlTypeNameParserResult result = SqlTypeNameParser<SqlTypeNameParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(null);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }
            }
        }

        protected override ColumnsMetadataColumnNames GetColumnsMetadataColumnNames()
        {
            return new ColumnsMetadataColumnNames()
            {
                TableCatalog = TableCat,
                TableSchema = TableSchem,
                TableName = TableName,
                ColumnName = ColumnName,
                DataType = DataType,
                TypeName = TypeName,
                Nullable = Nullable,
                ColumnDef = ColumnDef,
                OrdinalPosition = OrdinalPosition,
                IsNullable = IsNullable,
                IsAutoIncrement = IsAutoIncrement,
                ColumnSize = ColumnSize,
                DecimalDigits = DecimalDigits,
            };
        }

        protected override int ColumnMapIndexOffset => 0;
    }
}

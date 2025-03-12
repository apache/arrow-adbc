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
using System.IO;
using System.Collections.Generic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Net.Http;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    class TlsProperties
    {
        public bool IsSslEnabled { get; set; }
        public bool EnableServerCertificateValidation { get; set; }
        public bool AllowHostnameMismatch { get; set; }
        public bool AllowSelfSigned { get; set; }
        public string? TrustedCertificatePath { get; set; }
    }
    static class HiveServer2TlsImpl
    {
        static internal TlsProperties GetTlsOptions(IReadOnlyDictionary<string, string> Properties)
        {
            TlsProperties tlsProperties = new TlsProperties();
            Properties.TryGetValue(TlsOptions.IsSslEnabled, out string? isSslEnabled);
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);
            if (!string.IsNullOrWhiteSpace(uri))
            {
                var uriValue = new Uri(uri);
                tlsProperties.IsSslEnabled = uriValue.Scheme == Uri.UriSchemeHttps || (bool.TryParse(isSslEnabled, out bool isSslEnabledBool) && isSslEnabledBool);
            }
            else
            {
                tlsProperties.IsSslEnabled = bool.TryParse(isSslEnabled, out bool isSslEnabledBool) && isSslEnabledBool;
            }
            if (!tlsProperties.IsSslEnabled)
            {
                return tlsProperties;
            }
            tlsProperties.IsSslEnabled = true;
            Properties.TryGetValue(TlsOptions.EnableServerCertificateValidation, out string? enableServerCertificateValidation);
            if (bool.TryParse(enableServerCertificateValidation, out bool enableServerCertificateValidationBool) && !enableServerCertificateValidationBool)
            {
                tlsProperties.EnableServerCertificateValidation = false;
                return tlsProperties;
            }
            tlsProperties.EnableServerCertificateValidation = true;
            Properties.TryGetValue(TlsOptions.AllowHostnameMismatch, out string? allowHostnameMismatch);
            tlsProperties.AllowHostnameMismatch = bool.TryParse(allowHostnameMismatch, out bool allowHostnameMismatchBool) && allowHostnameMismatchBool;
            Properties.TryGetValue(TlsOptions.AllowSelfSigned, out string? allowSelfSigned);
            tlsProperties.AllowSelfSigned = bool.TryParse(allowSelfSigned, out bool allowSelfSignedBool) && allowSelfSignedBool;
            if (tlsProperties.AllowSelfSigned)
            {
                Properties.TryGetValue(TlsOptions.TrustedCertificatePath, out string? trustedCertificatePath);
                if (trustedCertificatePath == null) return tlsProperties;
                tlsProperties.TrustedCertificatePath = trustedCertificatePath != "" && File.Exists(trustedCertificatePath) ? trustedCertificatePath : throw new FileNotFoundException("Trusted certificate path is invalid or file does not exist.");
            }
            return tlsProperties;
        }

        static internal HttpClientHandler NewHttpClientHandler(TlsProperties tlsProperties)
        {
            HttpClientHandler httpClientHandler = new();
            if (tlsProperties.IsSslEnabled)
            {
                httpClientHandler.ServerCertificateCustomValidationCallback = (request, certificate, chain, policyErrors) =>
                {
                    if (policyErrors == SslPolicyErrors.None || !tlsProperties.EnableServerCertificateValidation) return true;
                    if (string.IsNullOrEmpty(tlsProperties.TrustedCertificatePath))
                    {
                        return
                            (!policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors) || tlsProperties.AllowSelfSigned)
                        && (!policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch) || tlsProperties.AllowHostnameMismatch);
                    }
                    if (certificate == null)
                        return false;
                    X509Certificate2 customCertificate = new X509Certificate2(tlsProperties.TrustedCertificatePath);
                    X509Chain chain2 = new X509Chain();
                    chain2.ChainPolicy.ExtraStore.Add(customCertificate);

                    // "tell the X509Chain class that I do trust this root certs and it should check just the certs in the chain and nothing else"
                    chain2.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

                    // Build the chain and verify
                    return chain2.Build(certificate);
                };
            }
            return httpClientHandler;
        }
    }
}

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
using System.IO;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    class TlsProperties
    {
        public bool IsTlsEnabled { get; set; } = true;
        public bool DisableServerCertificateValidation { get; set; }
        public bool AllowHostnameMismatch { get; set; }
        public bool AllowSelfSigned { get; set; }
        public string? TrustedCertificatePath { get; set; }
    }

    static class HiveServer2TlsImpl
    {
        static internal TlsProperties GetHttpTlsOptions(IReadOnlyDictionary<string, string> properties)
        {
            TlsProperties tlsProperties = new TlsProperties();
            if (properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrWhiteSpace(uri))
            {
                var uriValue = new Uri(uri);
                tlsProperties.IsTlsEnabled = uriValue.Scheme == Uri.UriSchemeHttps || (properties.TryGetValue(HttpTlsOptions.IsTlsEnabled, out string? isSslEnabled) && bool.TryParse(isSslEnabled, out bool isSslEnabledBool) && isSslEnabledBool);
            }
            else if (properties.TryGetValue(HttpTlsOptions.IsTlsEnabled, out string? isSslEnabled) && bool.TryParse(isSslEnabled, out bool isSslEnabledBool))
            {
                tlsProperties.IsTlsEnabled = isSslEnabledBool;
            }
            if (!tlsProperties.IsTlsEnabled)
            {
                return tlsProperties;
            }

            if (properties.TryGetValue(HttpTlsOptions.DisableServerCertificateValidation, out string? disableServerCertificateValidation) && bool.TryParse(disableServerCertificateValidation, out bool disableServerCertificateValidationBool) && disableServerCertificateValidationBool)
            {
                tlsProperties.DisableServerCertificateValidation = true;
                return tlsProperties;
            }
            tlsProperties.DisableServerCertificateValidation = false;
            tlsProperties.AllowHostnameMismatch = properties.TryGetValue(HttpTlsOptions.AllowHostnameMismatch, out string? allowHostnameMismatch) && bool.TryParse(allowHostnameMismatch, out bool allowHostnameMismatchBool) && allowHostnameMismatchBool;
            tlsProperties.AllowSelfSigned = properties.TryGetValue(HttpTlsOptions.AllowSelfSigned, out string? allowSelfSigned) && bool.TryParse(allowSelfSigned, out bool allowSelfSignedBool) && allowSelfSignedBool;
            if (tlsProperties.AllowSelfSigned)
            {
                if (!properties.TryGetValue(HttpTlsOptions.TrustedCertificatePath, out string? trustedCertificatePath)) return tlsProperties;
                tlsProperties.TrustedCertificatePath = trustedCertificatePath != "" && File.Exists(trustedCertificatePath) ? trustedCertificatePath : throw new FileNotFoundException("Trusted certificate path is invalid or file does not exist.");
            }
            return tlsProperties;
        }

        static internal HttpClientHandler NewHttpClientHandler(TlsProperties tlsProperties)
        {
            HttpClientHandler httpClientHandler = new();
            if (tlsProperties.IsTlsEnabled)
            {
                httpClientHandler.ServerCertificateCustomValidationCallback = (request, certificate, chain, policyErrors) =>
                {
                    if (policyErrors == SslPolicyErrors.None || tlsProperties.DisableServerCertificateValidation) return true;
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

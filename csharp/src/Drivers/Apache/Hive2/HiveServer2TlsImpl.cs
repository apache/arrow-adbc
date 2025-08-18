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
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;

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
            TlsProperties tlsProperties = new();
            if (properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrWhiteSpace(uri))
            {
                var uriValue = new Uri(uri);
                tlsProperties.IsTlsEnabled = uriValue.Scheme == Uri.UriSchemeHttps || !properties.TryGetValue(HttpTlsOptions.IsTlsEnabled, out string? isTlsEnabled) || !bool.TryParse(isTlsEnabled, out bool isTlsEnabledBool) || isTlsEnabledBool;
            }
            else if (!properties.TryGetValue(HttpTlsOptions.IsTlsEnabled, out string? isTlsEnabled) || !bool.TryParse(isTlsEnabled, out bool isTlsEnabledBool))
            {
                tlsProperties.IsTlsEnabled = true;
            }
            else
            {
                tlsProperties.IsTlsEnabled = isTlsEnabledBool;
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
            if (!properties.TryGetValue(HttpTlsOptions.TrustedCertificatePath, out string? trustedCertificatePath)) return tlsProperties;
            tlsProperties.TrustedCertificatePath = trustedCertificatePath != "" && File.Exists(trustedCertificatePath) ? trustedCertificatePath : throw new FileNotFoundException("Trusted certificate path is invalid or file does not exist.");
            return tlsProperties;
        }

        static internal HttpClientHandler NewHttpClientHandler(TlsProperties tlsProperties, HiveServer2ProxyConfigurator proxyConfigurator)
        {
            HttpClientHandler httpClientHandler = new();
            if (tlsProperties.IsTlsEnabled)
            {
                httpClientHandler.ServerCertificateCustomValidationCallback = (request, cert, chain, errors) => ValidateCertificate(cert, errors, tlsProperties);
            }
            proxyConfigurator.ConfigureProxy(httpClientHandler);
            httpClientHandler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
            return httpClientHandler;
        }

        static private bool IsSelfSigned(X509Certificate2 cert)
        {
            return cert.Subject == cert.Issuer && IsSignedBy(cert, cert);
        }

        static private bool IsSignedBy(X509Certificate2 cert, X509Certificate2 issuer)
        {
            try
            {
                using (var chain = new X509Chain())
                {
                    chain.ChainPolicy.ExtraStore.Add(issuer);
                    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
                    chain.ChainPolicy.RevocationMode = X509RevocationMode.Online;

                    return chain.Build(cert)
                        && chain.ChainElements.Count == 1
                        && chain.ChainElements[0].Certificate.Thumbprint == issuer.Thumbprint;
                }
            }
            catch
            {
                return false;
            }
        }

        static internal TlsProperties GetStandardTlsOptions(IReadOnlyDictionary<string, string> properties)
        {
            TlsProperties tlsProperties = new();
            // tls is enabled by default
            if (!properties.TryGetValue(StandardTlsOptions.IsTlsEnabled, out string? isTlsEnabled) || !bool.TryParse(isTlsEnabled, out bool isTlsEnabledBool))
            {
                tlsProperties.IsTlsEnabled = true;
            }
            else
            {
                tlsProperties.IsTlsEnabled = isTlsEnabledBool;
            }
            if (!tlsProperties.IsTlsEnabled)
            {
                return tlsProperties;
            }

            if (properties.TryGetValue(StandardTlsOptions.DisableServerCertificateValidation, out string? disableServerCertificateValidation) && bool.TryParse(disableServerCertificateValidation, out bool disableServerCertificateValidationBool) && disableServerCertificateValidationBool)
            {
                tlsProperties.DisableServerCertificateValidation = true;
                return tlsProperties;
            }
            tlsProperties.DisableServerCertificateValidation = false;
            tlsProperties.AllowHostnameMismatch = properties.TryGetValue(StandardTlsOptions.AllowHostnameMismatch, out string? allowHostnameMismatch) && bool.TryParse(allowHostnameMismatch, out bool allowHostnameMismatchBool) && allowHostnameMismatchBool;
            tlsProperties.AllowSelfSigned = properties.TryGetValue(StandardTlsOptions.AllowSelfSigned, out string? allowSelfSigned) && bool.TryParse(allowSelfSigned, out bool allowSelfSignedBool) && allowSelfSignedBool;
            if (!properties.TryGetValue(StandardTlsOptions.TrustedCertificatePath, out string? trustedCertificatePath)) return tlsProperties;
            tlsProperties.TrustedCertificatePath = trustedCertificatePath != "" && File.Exists(trustedCertificatePath) ? trustedCertificatePath : throw new FileNotFoundException("Trusted certificate path is invalid or file does not exist.");
            return tlsProperties;
        }

        public static List<X509Certificate2> LoadPemCertificates(string pemPath)
        {
            List<X509Certificate2> certs = new();
            string pemContent = File.ReadAllText(pemPath);

            MatchCollection matches = Regex.Matches(
                pemContent,
                "-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----",
                RegexOptions.Singleline);

            foreach (Match match in matches)
            {
                string base64 = match.Groups[1].Value
                    .Replace("\r", "")
                    .Replace("\n", "")
                    .Trim();

                byte[] rawData = Convert.FromBase64String(base64);
                certs.Add(new X509Certificate2(rawData));
            }

            return certs;
        }

        static internal bool ValidateCertificate(X509Certificate? cert, SslPolicyErrors policyErrors, TlsProperties tlsProperties)
        {
            if (policyErrors == SslPolicyErrors.None || tlsProperties.DisableServerCertificateValidation)
                return true;

            if (cert == null || !(cert is X509Certificate2 cert2))
                return false;

            bool isNameMismatchError = policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch) && !tlsProperties.AllowHostnameMismatch;

            if (isNameMismatchError) return false;

            if (string.IsNullOrEmpty(tlsProperties.TrustedCertificatePath))
            {
                return !policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors) || (tlsProperties.AllowSelfSigned && IsSelfSigned(cert2));
            }

            X509Chain customChain = new();
            var collection = LoadPemCertificates(tlsProperties.TrustedCertificatePath!);

            foreach (var trustedCert in collection)
            {
                customChain.ChainPolicy.ExtraStore.Add(trustedCert);
            }
            customChain.ChainPolicy.RevocationMode = X509RevocationMode.Online;

            bool chainValid = customChain.Build(cert2);
            return chainValid || (tlsProperties.AllowSelfSigned && IsSelfSigned(cert2));
        }
    }
}

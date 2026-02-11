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
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    class TlsProperties
    {
        public bool IsTlsEnabled { get; set; } = true;
        public X509RevocationMode RevocationMode { get; set; } = X509RevocationMode.Online;
        public bool DisableServerCertificateValidation { get; set; }
        public bool AllowHostnameMismatch { get; set; }
        public bool AllowSelfSigned { get; set; }
        public string? TrustedCertificatePath { get; set; }
    }

    static class HiveServer2TlsImpl
    {
        /// <summary>
        /// Parses a revocation mode string into the corresponding X509RevocationMode enum value.
        /// </summary>
        /// <param name="value">The revocation mode string (case-insensitive): "online", "offline", or "nocheck"</param>
        /// <returns>The parsed X509RevocationMode, or X509RevocationMode.Online if the value is invalid</returns>
        private static X509RevocationMode ParseRevocationMode(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return X509RevocationMode.Online;
            }

            return value!.Trim().ToLowerInvariant() switch
            {
                "online" => X509RevocationMode.Online,
                "offline" => X509RevocationMode.Offline,
                "nocheck" => X509RevocationMode.NoCheck,
                _ => X509RevocationMode.Online
            };
        }

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

            // Parse revocation mode if specified
            if (properties.TryGetValue(HttpTlsOptions.RevocationMode, out string? revocationMode))
            {
                tlsProperties.RevocationMode = ParseRevocationMode(revocationMode);
            }

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
                    // Note: NoCheck is intentional here - this helper only validates cryptographic signatures,
                    // not full certificate chain validation. Revocation checking happens in ValidateCertificate()
                    // which uses the configurable tlsProperties.RevocationMode.
                    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

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

            // Parse revocation mode if specified
            if (properties.TryGetValue(StandardTlsOptions.RevocationMode, out string? revocationMode))
            {
                tlsProperties.RevocationMode = ParseRevocationMode(revocationMode);
            }

            if (!properties.TryGetValue(StandardTlsOptions.TrustedCertificatePath, out string? trustedCertificatePath)) return tlsProperties;
            tlsProperties.TrustedCertificatePath = trustedCertificatePath != "" && File.Exists(trustedCertificatePath) ? trustedCertificatePath : throw new FileNotFoundException("Trusted certificate path is invalid or file does not exist.");
            return tlsProperties;
        }

        /// <summary>
        /// Analyzes certificate chain validation errors and throws descriptive exceptions with actionable guidance.
        /// </summary>
        private static void ThrowDetailedCertificateError(X509Chain chain, TlsProperties tlsProperties)
        {
            foreach (var status in chain.ChainStatus)
            {
                switch (status.Status)
                {
                    case X509ChainStatusFlags.UntrustedRoot:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: The root certificate is not trusted. " +
                            "This occurs when the Certificate Authority (CA) that signed the server certificate is not in your system's trusted root store. " +
                            "To resolve this: (1) Install the CA certificate in your system's trusted root store, or " +
                            "(2) Specify the trusted certificate using 'adbc.http_options.tls.trusted_certificate_path', or " +
                            "(3) For testing only, set 'adbc.http_options.tls.disable_server_certificate_validation=true' (NOT recommended for production).");

                    case X509ChainStatusFlags.PartialChain:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: The certificate chain is incomplete. " +
                            "This occurs when one or more intermediate certificates are missing from the chain. " +
                            "The server should send all intermediate certificates, but some servers are misconfigured. " +
                            "To resolve this: (1) Contact the server administrator to fix the certificate chain configuration, or " +
                            "(2) Install the missing intermediate certificates in your system's certificate store, or " +
                            "(3) Specify the complete certificate chain using 'adbc.http_options.tls.trusted_certificate_path'.");

                    case X509ChainStatusFlags.RevocationStatusUnknown:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: Unable to determine the revocation status of the certificate. " +
                            "This typically occurs when the Certificate Revocation List (CRL) or Online Certificate Status Protocol (OCSP) servers are unreachable. " +
                            "Common causes: AWS PrivateLink, corporate firewalls blocking port 80, or network restrictions. " +
                            $"Current revocation mode: {tlsProperties.RevocationMode}. " +
                            "To resolve this: Set 'adbc.http_options.tls.revocation_mode=nocheck' to skip revocation checking (safe in isolated networks), " +
                            "or set 'revocation_mode=offline' to use only cached CRL data.");

                    case X509ChainStatusFlags.OfflineRevocation:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: The revocation server is offline or unreachable. " +
                            "The certificate contains revocation URLs (CRL/OCSP) that require outbound HTTP (port 80) access, but the connection failed. " +
                            "This is common in AWS PrivateLink environments or networks with restrictive egress rules. " +
                            "To resolve this: Set 'adbc.http_options.tls.revocation_mode=nocheck' to disable revocation checking, " +
                            "or set 'revocation_mode=offline' to use only locally cached revocation data.");

                    case X509ChainStatusFlags.Revoked:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: The certificate has been revoked by the Certificate Authority. " +
                            "This is a critical security issue - the certificate is no longer valid and should not be trusted. " +
                            "To resolve this: Contact the server administrator to install a new, valid certificate. " +
                            "Do NOT disable certificate validation to work around this error.");

                    case X509ChainStatusFlags.NotTimeValid:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: The certificate is not valid for the current date/time. " +
                            "The certificate may be expired or not yet valid. " +
                            $"Certificate details: {status.StatusInformation}. " +
                            "To resolve this: (1) Verify your system clock is correct, or " +
                            "(2) Contact the server administrator to renew the expired certificate.");

                    case X509ChainStatusFlags.NotSignatureValid:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: The certificate signature is invalid. " +
                            "This indicates the certificate may be corrupted, tampered with, or improperly signed. " +
                            "This is a critical security issue. " +
                            "To resolve this: Contact the server administrator - the certificate needs to be replaced.");

                    case X509ChainStatusFlags.InvalidNameConstraints:
                    case X509ChainStatusFlags.HasNotPermittedNameConstraint:
                    case X509ChainStatusFlags.HasExcludedNameConstraint:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: Certificate name constraints violation. " +
                            "The certificate is not permitted to be used for this hostname or domain. " +
                            $"Details: {status.StatusInformation}. " +
                            "To resolve this: Verify you are connecting to the correct hostname, or contact the server administrator.");

                    case X509ChainStatusFlags.InvalidBasicConstraints:
                        throw new System.Security.Authentication.AuthenticationException(
                            "Certificate validation failed: Invalid basic constraints. " +
                            "The certificate chain violates basic constraints (e.g., a non-CA certificate is being used as a CA). " +
                            "This indicates a misconfigured certificate hierarchy. " +
                            "To resolve this: Contact the server administrator to fix the certificate chain.");

                    default:
                        // For any other error, include the raw status information
                        throw new System.Security.Authentication.AuthenticationException(
                            $"Certificate validation failed: {status.Status}. " +
                            $"Details: {status.StatusInformation}. " +
                            "This may indicate a certificate configuration issue. Please contact your system administrator.");
                }
            }
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
                // If chain errors exist, provide detailed error information
                if (policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors))
                {
                    if (tlsProperties.AllowSelfSigned && IsSelfSigned(cert2))
                        return true;

                    // Build a chain to get detailed error information
                    using (X509Chain defaultChain = new X509Chain())
                    {
                        defaultChain.ChainPolicy.RevocationMode = tlsProperties.RevocationMode;
                        defaultChain.Build(cert2);

                        // Throw detailed error with actionable guidance (never returns)
                        ThrowDetailedCertificateError(defaultChain, tlsProperties);
                    }

                    // Unreachable - ThrowDetailedCertificateError always throws
                    throw new InvalidOperationException("ThrowDetailedCertificateError should have thrown an exception");
                }
                return true;
            }

            X509Certificate2 trustedRoot = new X509Certificate2(tlsProperties.TrustedCertificatePath);
            X509Chain customChain = new();
            customChain.ChainPolicy.ExtraStore.Add(trustedRoot);
            // "tell the X509Chain class that I do trust this root certs and it should check just the certs in the chain and nothing else"
            customChain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            customChain.ChainPolicy.RevocationMode = tlsProperties.RevocationMode;

            bool chainValid = customChain.Build(cert2);

            // If self-signed is allowed and this is self-signed, accept it
            if (tlsProperties.AllowSelfSigned && IsSelfSigned(cert2))
                return true;

            // If validation failed, throw detailed error (never returns)
            if (!chainValid)
                ThrowDetailedCertificateError(customChain, tlsProperties);

            return true;
        }
    }
}

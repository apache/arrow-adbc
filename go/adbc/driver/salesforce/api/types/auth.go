package types

// AuthConfig holds configuration for Salesforce JWT authentication.
type AuthConfig struct {
	LoginURL      string // Salesforce login endpoint (e.g. "https://login.salesforce.com")
	ClientID      string // Connected App consumer key
	Username      string // Salesforce user to authenticate as
	PrivateKeyPEM string // PEM-encoded RSA private key for JWT signing
	APIVersion    string // Salesforce API version (e.g. "v64.0")
}

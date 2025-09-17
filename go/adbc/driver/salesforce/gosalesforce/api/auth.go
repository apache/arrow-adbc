// Package auth provides authentication functionality for Salesforce APIs
//
// This package supports multiple authentication flows:
//   - JWT Bearer flow (recommended for server-to-server)
//   - Username/Password flow (for user authentication)
//   - Refresh Token flow (for token renewal)
//   - CDP Token Exchange (for Customer Data Platform)
//
// Example JWT Authentication:
//
//	config := &auth.AuthConfig{
//		LoginURL:   "https://login.salesforce.com",
//		ClientID:   "your-client-id",
//		Username:   "your-username",
//		PrivateKey: "-----BEGIN RSA PRIVATE KEY-----\n...",
//	}
//
//	client := auth.NewClient(config)
//	token, err := client.Authenticate(context.Background())
//	if err != nil {
//		log.Fatal(err)
//	}
//
// Example Username/Password Authentication:
//
//	config := &auth.AuthConfig{
//		LoginURL:     "https://login.salesforce.com",
//		ClientID:     "your-client-id",
//		ClientSecret: "your-client-secret",
//		Username:     "your-username",
//		Password:     "your-password-with-security-token",
//	}
//
//	client := auth.NewClient(config)
//	token, err := client.Authenticate(context.Background())
package api

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// NewJWTConfig creates a new AuthConfig for JWT authentication
func NewJWTConfig(loginURL, clientID, username, privateKeyData string) (*AuthConfig, error) {
	config := DefaultAuthConfig()
	config.LoginURL = loginURL
	config.ClientID = clientID
	config.Username = username
	config.PrivateKey = string(privateKeyData)

	// Validate the private key
	if err := validatePrivateKey(config.PrivateKey); err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	return config, nil
}

// NewUsernamePasswordConfig creates a new AuthConfig for username/password authentication
func NewUsernamePasswordConfig(loginURL, clientID, clientSecret, username, password string) *AuthConfig {
	config := DefaultAuthConfig()
	config.LoginURL = loginURL
	config.ClientID = clientID
	config.ClientSecret = clientSecret
	config.Username = username
	config.Password = password
	return config
}

// validatePrivateKey validates that the private key is properly formatted
func validatePrivateKey(privateKeyPEM string) error {
	block, _ := pem.Decode([]byte(privateKeyPEM))
	if block == nil {
		return fmt.Errorf("failed to decode PEM block")
	}

	switch block.Type {
	case "RSA PRIVATE KEY":
		_, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		return err
	case "PRIVATE KEY":
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return err
		}
		if _, ok := key.(*rsa.PrivateKey); !ok {
			return fmt.Errorf("not an RSA private key")
		}
		return nil
	default:
		return fmt.Errorf("unsupported private key type: %s", block.Type)
	}
}

// GetInstanceFromToken extracts the Salesforce instance from a token's instance URL
func GetInstanceFromToken(token *Token) string {
	if token == nil || token.InstanceURL == "" {
		return ""
	}

	// Parse instance from URL like https://na1.salesforce.com
	// This is a simple implementation - in production you might want more robust parsing
	url := token.InstanceURL
	if len(url) > 8 && url[:8] == "https://" {
		url = url[8:]
	}
	if len(url) > 7 && url[:7] == "http://" {
		url = url[7:]
	}

	// Find the first dot
	dotIndex := -1
	for i, c := range url {
		if c == '.' {
			dotIndex = i
			break
		}
	}

	if dotIndex > 0 {
		return url[:dotIndex]
	}

	return ""
}

// ExecuteSqlQuery is a convenience function to execute a SQL query using a Data Cloud token
// This combines authentication and query execution in a single step
func ExecuteSqlQuery(ctx context.Context, client *Client, queryRequest *SqlQueryRequest) (*SqlQueryResponse, error) {
	if client == nil {
		return nil, &SfdcError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	if err := validateToken(client.accessToken, "CDP token"); err != nil {
		return nil, err
	}

	return client.ExecuteSqlQuery(ctx, queryRequest)
}

// ExecuteSqlQueryLegacy is a convenience function to execute a v2 SQL query using a Data Cloud token
func ExecuteSqlQueryLegacy(ctx context.Context, client *Client, query string, enableArrowStream bool) (*QueryV2Response, error) {
	if client == nil {
		return nil, &SfdcError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	if err := validateToken(client.cdpToken, "CDP token"); err != nil {
		return nil, err
	}

	return client.ExecuteQueryV2(ctx, query, enableArrowStream)
}

// GetNextBatchV2WithToken is a convenience function to get the next batch of v2 query results using a Data Cloud token
func GetNextBatchV2WithToken(ctx context.Context, client *Client, nextBatchId string, enableArrowStream bool) (*QueryV2Response, error) {
	if client == nil {
		return nil, &SfdcError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	if err := validateToken(client.GetDataCloudToken(), "CDP token"); err != nil {
		return nil, err
	}

	cdpToken := client.GetDataCloudToken()
	return client.GetNextBatchV2(ctx, cdpToken.InstanceURL, cdpToken.AccessToken, nextBatchId, enableArrowStream)
}

// GetMetadata is a convenience function to retrieve Data Cloud metadata using a token
func GetMetadata(ctx context.Context, client *Client, dataspace, entityCategory, entityName, entityType string) (*MetadataResponse, error) {
	if client == nil {
		return nil, &SfdcError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	if err := validateToken(client.cdpToken, "CDP token"); err != nil {
		return nil, err
	}

	return client.GetMetadata(ctx, dataspace, entityCategory, entityName, entityType)
}

// CreateJob is a convenience function to create a data ingestion job using a token
func CreateJob(ctx context.Context, client *Client, request *CreateJobRequest) (*CreateJobResponse, error) {
	if client == nil {
		return nil, &SfdcError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	if err := validateToken(client.cdpToken, "CDP token"); err != nil {
		return nil, err
	}

	return client.CreateJob(ctx, request)
}

// UploadJobData is a convenience function to upload data to a job using a token
func UploadJobData(ctx context.Context, client *Client, jobID string, data []byte, contentType string) error {
	if client == nil {
		return &SfdcError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	if err := validateToken(client.cdpToken, "CDP token"); err != nil {
		return err
	}

	return client.UploadJobData(ctx, jobID, data, contentType)
}

// CloseJob is a convenience function to close or abort a job using a token
func CloseJob(ctx context.Context, client *Client, jobID string, state string) (*CloseJobResponse, error) {
	if client == nil {
		return nil, &SfdcError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	if err := validateToken(client.cdpToken, "CDP token"); err != nil {
		return nil, err
	}

	return client.CloseJob(ctx, jobID, state)
}

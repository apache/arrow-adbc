package api

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"bytes"

	"github.com/golang-jwt/jwt/v5"
)

// Client provides authentication and query functionality for Salesforce Data Cloud
type Client struct {
	config      *AuthConfig
	httpClient  *http.Client
	accessToken *Token
	cdpToken    *Token
	version     string
}

// NewClient creates a new authentication client
func NewClient(config *AuthConfig, version string) *Client {
	if config == nil {
		config = DefaultAuthConfig()
	}

	httpClient := &http.Client{
		Timeout: config.Timeout,
	}

	return &Client{
		config:     config,
		httpClient: httpClient,
		version:    version,
	}
}

func (c *Client) GetToken() *Token {
	return c.accessToken
}

func (c *Client) GetDataCloudToken() *Token {
	return c.cdpToken
}

func (c *Client) buildServicesURL(instanceURL, path string) string {
	return fmt.Sprintf("%s/services/data/%s/ssot/%s", instanceURL, c.version, path)
}

// normalizeURL ensures the URL has the https:// protocol
func normalizeURL(instanceURL string) string {
	if !strings.HasPrefix(instanceURL, "http://") && !strings.HasPrefix(instanceURL, "https://") {
		return "https://" + instanceURL
	}
	return instanceURL
}

// executeHTTPRequest performs an HTTP request with retries and exponential backoff
func (c *Client) executeHTTPRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	for i := 0; i < c.config.MaxRetries; i++ {
		resp, err = c.httpClient.Do(req)
		if err == nil {
			break
		}
		if i < c.config.MaxRetries-1 {
			// Exponential backoff with jitter
			delay := time.Duration(rand.Intn(5)+1) * time.Second
			time.Sleep(delay)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("HTTP request failed after %d retries: %w", c.config.MaxRetries, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("no response received from HTTP request")
	}

	return resp, nil
}

// handleErrorResponse handles common error response patterns
func handleErrorResponse(statusCode int, body []byte, errorType string) error {
	var errorResp map[string]interface{}
	if json.Unmarshal(body, &errorResp) == nil {
		if errorMsg, ok := errorResp["message"].(string); ok {
			return &AuthError{
				Code:    statusCode,
				Message: errorMsg,
				Type:    errorType,
			}
		}
	}

	return &AuthError{
		Code:    statusCode,
		Message: fmt.Sprintf("Request failed with code %d: %s", statusCode, string(body)),
		Type:    errorType,
	}
}

// setCommonHeaders sets common headers for API requests
func setCommonHeaders(req *http.Request, accessToken string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
}

// validateToken checks if a token is valid and not expired
func validateToken(token *Token, tokenName string) error {
	if token == nil {
		return &AuthError{
			Code:    400,
			Message: fmt.Sprintf("%s cannot be nil", tokenName),
			Type:    "invalid_token",
		}
	}

	if token.IsExpired() {
		return &AuthError{
			Code:    401,
			Message: fmt.Sprintf("%s is expired", tokenName),
			Type:    "token_expired",
		}
	}

	return nil
}

// Authenticate with the Data Cloud API
// reference: https://developer.salesforce.com/docs/marketing/marketing-cloud-growth/guide/mc-connect-apis-data-cloud.html
func (c *Client) Authenticate(ctx context.Context) error {
	var token *Token
	var err error
	if c.config.PrivateKey != "" && c.config.Username != "" && c.config.ClientID != "" {
		token, err = c.authenticateJWT(ctx)
	} else if c.config.Username != "" && c.config.Password != "" && c.config.ClientID != "" && c.config.ClientSecret != "" {
		token, err = c.authenticateUsernamePassword(ctx)
	} else if c.config.RefreshToken != "" && c.config.ClientID != "" && c.config.ClientSecret != "" {
		token, err = c.authenticateRefreshToken(ctx)
	}

	if err != nil {
		return err
	}

	c.accessToken = token
	return nil
}

// authenticateJWT performs JWT Bearer flow authentication
func (c *Client) authenticateJWT(ctx context.Context) (*Token, error) {
	// Parse the private key
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(c.config.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Create JWT token
	jwtToken, err := c.createJWTAssertion(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT assertion: %w", err)
	}

	// Prepare form data
	data := url.Values{}
	data.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	data.Set("assertion", jwtToken)

	// Make the request
	tokenURL := c.config.LoginURL + "/services/oauth2/token"
	return c.requestAccessToken(ctx, tokenURL, data)
}

// authenticateUsernamePassword performs username/password flow authentication
func (c *Client) authenticateUsernamePassword(ctx context.Context) (*Token, error) {
	// Prepare form data
	data := url.Values{}
	data.Set("grant_type", "password")
	data.Set("client_id", c.config.ClientID)
	data.Set("client_secret", c.config.ClientSecret)
	data.Set("username", c.config.Username)
	data.Set("password", c.config.Password)

	// Make the request
	tokenURL := c.config.LoginURL + "/services/oauth2/token"
	return c.requestAccessToken(ctx, tokenURL, data)
}

// authenticateRefreshToken performs refresh token flow authentication
func (c *Client) authenticateRefreshToken(ctx context.Context) (*Token, error) {
	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", c.config.RefreshToken)
	data.Set("client_id", c.config.ClientID)
	data.Set("client_secret", c.config.ClientSecret)

	tokenURL := c.config.LoginURL + "/services/oauth2/token"
	return c.requestAccessToken(ctx, tokenURL, data)
}

// RefreshToken refreshes an existing token
func (c *Client) RefreshToken(ctx context.Context, token *Token) (*Token, error) {
	if token.RefreshToken == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "No refresh token available",
			Type:    "no_refresh_token",
		}
	}

	// Temporarily set the refresh token in config
	originalRefreshToken := c.config.RefreshToken
	c.config.RefreshToken = token.RefreshToken
	defer func() {
		c.config.RefreshToken = originalRefreshToken
	}()

	return c.authenticateRefreshToken(ctx)
}

// Exchanges the access token for a Data Cloud token
// reference: https://developer.salesforce.com/docs/marketing/marketing-cloud-growth/guide/mc-connect-apis-data-cloud.html
func (c *Client) ExchangeAndSetDataCloudToken(ctx context.Context) error {
	data := url.Values{}
	data.Set("grant_type", "urn:salesforce:grant-type:external:cdp")
	data.Set("subject_token_type", "urn:ietf:params:oauth:token-type:access_token")
	data.Set("subject_token", c.accessToken.AccessToken)

	tokenURL := c.accessToken.InstanceURL + "/services/a360/token"
	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create Data Cloud token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read data cloud token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &AuthError{
			Code:    resp.StatusCode,
			Message: fmt.Sprintf("data cloud token retrieval failed with code %d: %s", resp.StatusCode, string(body)),
			Type:    "data_cloud_token_failed",
		}
	}

	type DataCloudTokenResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		InstanceURL string `json:"instance_url"`
		TokenType   string `json:"token_type"`
	}

	var respParsed DataCloudTokenResponse
	if err := json.Unmarshal(body, &respParsed); err != nil {
		return fmt.Errorf("failed to parse CDP token response: %w", err)
	}

	expiresAt := time.Now().Add(time.Duration(respParsed.ExpiresIn) * time.Second)

	c.cdpToken = &Token{
		AccessToken: respParsed.AccessToken,
		InstanceURL: respParsed.InstanceURL,
		TokenType:   respParsed.TokenType,
		ExpiresAt:   expiresAt,
	}

	return nil
}

// RevokeToken revokes the given token
func (c *Client) RevokeToken(ctx context.Context, token string) error {
	data := url.Values{}
	data.Set("token", token)

	revokeURL := c.config.LoginURL + "/services/oauth2/revoke"
	req, err := http.NewRequestWithContext(ctx, "POST", revokeURL, strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create revoke request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("revoke request failed: %w", err)
	}
	defer resp.Body.Close()

	// Token revocation typically returns 200 even for invalid tokens
	return nil
}

// ExecuteSqlQuery executes a SQL query against Data Cloud
func (c *Client) ExecuteSqlQuery(ctx context.Context, queryRequest *SqlQueryRequest) (*SqlQueryResponse, error) {
	if queryRequest == nil {
		return nil, &AuthError{
			Code:    400,
			Message: "SQL query request cannot be nil",
			Type:    "invalid_request",
		}
	}

	if queryRequest.SQL == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "SQL query cannot be empty",
			Type:    "invalid_request",
		}
	}

	// Prepare the request body
	requestBody, err := json.Marshal(queryRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SQL query request: %w", err)
	}

	queryURL := c.buildServicesURL(c.accessToken.InstanceURL, "query-sql")

	if queryRequest.Dataspace != "" {
		queryURL += "?dataspace=" + url.QueryEscape(queryRequest.Dataspace)
	}
	if queryRequest.WorkloadName != "" {
		separator := "?"
		if queryRequest.Dataspace != "" {
			separator = "&"
		}
		queryURL += separator + "workloadName=" + url.QueryEscape(queryRequest.WorkloadName)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, strings.NewReader(string(requestBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL query request: %w", err)
	}

	setCommonHeaders(req, c.accessToken.AccessToken)

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL query response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, handleErrorResponse(resp.StatusCode, body, "sql_query_failed")
	}

	var queryResponse SqlQueryResponse
	if err := json.Unmarshal(body, &queryResponse); err != nil {
		return nil, fmt.Errorf("failed to parse SQL query response: %w", err)
	}

	return &queryResponse, nil
}

// ExecuteQueryV2 executes a SQL query against the Data Cloud v2 query API
func (c *Client) ExecuteQueryV2(ctx context.Context, query string, enableArrowStream bool) (*QueryV2Response, error) {
	if query == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "SQL query cannot be empty",
			Type:    "invalid_request",
		}
	}

	// Prepare the request body
	queryRequest := &QueryV2Request{
		Sql: query,
	}

	requestBody, err := json.Marshal(queryRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query v2 request: %w", err)
	}

	// Construct the URL for v2 API
	instanceURL := normalizeURL(c.cdpToken.InstanceURL)
	queryURL := instanceURL + "/api/v2/query"

	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, strings.NewReader(string(requestBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create query v2 request: %w", err)
	}

	setCommonHeaders(req, c.cdpToken.AccessToken)
	// Note: Removed Accept-Encoding: gzip as it causes parsing issues

	if enableArrowStream {
		req.Header.Set("enable-arrow-stream", "true")
	}

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read query v2 response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(resp.StatusCode, body, "query_v2_failed")
	}

	var queryResponse QueryV2Response
	if err := json.Unmarshal(body, &queryResponse); err != nil {
		return nil, fmt.Errorf("failed to parse query v2 response: %w", err)
	}

	return &queryResponse, nil
}

// GetNextBatchV2 fetches the next batch of results for a v2 query
func (c *Client) GetNextBatchV2(ctx context.Context, instanceURL, accessToken, nextBatchId string, enableArrowStream bool) (*QueryV2Response, error) {
	if nextBatchId == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Next batch ID cannot be empty",
			Type:    "invalid_request",
		}
	}

	instanceURL = normalizeURL(instanceURL)
	batchURL := instanceURL + "/api/v2/query/" + nextBatchId

	req, err := http.NewRequestWithContext(ctx, "GET", batchURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create next batch request: %w", err)
	}

	setCommonHeaders(req, accessToken)
	// Note: Removed Accept-Encoding: gzip as it causes parsing issues

	if enableArrowStream {
		req.Header.Set("enable-arrow-stream", "true")
	}

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read next batch response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(resp.StatusCode, body, "next_batch_failed")
	}

	var batchResponse QueryV2Response
	if err := json.Unmarshal(body, &batchResponse); err != nil {
		return nil, fmt.Errorf("failed to parse next batch response: %w", err)
	}

	return &batchResponse, nil
}

// createJWTAssertion creates a JWT assertion for authentication
func (c *Client) createJWTAssertion(privateKey *rsa.PrivateKey) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": c.config.ClientID,
		"sub": c.config.Username,
		"aud": c.config.LoginURL,
		"exp": now.Add(time.Hour).Unix(),
		"iat": now.Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(privateKey)
}

// Requests an access token
// reference: https://developer.salesforce.com/docs/marketing/marketing-cloud-growth/guide/mc-connect-apis-data-cloud.html#step-4-request-an-access-token
func (c *Client) requestAccessToken(ctx context.Context, tokenURL string, data url.Values) (*Token, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// Handle special case for token endpoint error format
		var errorResp map[string]interface{}
		if json.Unmarshal(body, &errorResp) == nil {
			if errorMsg, ok := errorResp["error_description"].(string); ok {
				return nil, &AuthError{
					Code:    resp.StatusCode,
					Message: errorMsg,
					Type:    fmt.Sprintf("%v", errorResp["error"]),
				}
			}
		}

		return nil, &AuthError{
			Code:    resp.StatusCode,
			Message: fmt.Sprintf("Authentication failed with code %d: %s", resp.StatusCode, string(body)),
			Type:    "authentication_failed",
		}
	}

	type TokenResponse struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token,omitempty"`
		InstanceURL  string `json:"instance_url"`
		ID           string `json:"id,omitempty"`
		TokenType    string `json:"token_type"`
		IssuedAt     string `json:"issued_at,omitempty"`
		Signature    string `json:"signature,omitempty"`
		ExpiresIn    int    `json:"expires_in,omitempty"`
		Scope        string `json:"scope,omitempty"`
	}
	var tokenResp TokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}

	var expiresAt time.Time
	if tokenResp.ExpiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	}

	return &Token{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		InstanceURL:  tokenResp.InstanceURL,
		TokenType:    tokenResp.TokenType,
		ExpiresAt:    expiresAt,
	}, nil
}

// GetMetadata retrieves metadata for Data Cloud objects
func (c *Client) GetMetadata(ctx context.Context, dataspace, entityCategory, entityName, entityType string) (*MetadataResponse, error) {
	token := c.accessToken
	if err := validateToken(token, "Access token"); err != nil {
		return nil, err
	}

	// Build metadata URL
	metadataURL := c.buildServicesURL(token.InstanceURL, "metadata")

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "GET", metadataURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata request: %w", err)
	}

	// Add query parameters if provided
	query := req.URL.Query()
	if dataspace != "" {
		query.Add("dataspace", dataspace)
	}
	if entityCategory != "" {
		query.Add("entityCategory", entityCategory)
	}
	if entityName != "" {
		query.Add("entityName", entityName)
	}
	if entityType != "" {
		query.Add("entityType", entityType)
	}
	req.URL.RawQuery = query.Encode()

	// Set headers
	setCommonHeaders(req, token.AccessToken)

	// Execute request
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata response: %w", err)
	}

	// Check for success
	if resp.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(resp.StatusCode, body, "metadata")
	}

	// Parse response
	var metadataResp MetadataResponse
	if err := json.Unmarshal(body, &metadataResp); err != nil {
		return nil, fmt.Errorf("failed to parse metadata response: %w", err)
	}

	return &metadataResp, nil
}

// reference: https://developer.salesforce.com/docs/data/data-cloud-int/guide/c360-a-ingestion-api.html

// CreateJob creates a new data ingestion job in Data Cloud
// Note: Depending on your org configuration, this may work with either the original Salesforce token
// or the CDP token. Try both if one fails.
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-upload-job-data.html
func (c *Client) CreateJob(ctx context.Context, request *CreateJobRequest) (*CreateJobResponse, error) {
	token := c.cdpToken
	// Validate token
	if err := validateToken(token, "CDP token"); err != nil {
		return nil, err
	}

	// Build job creation URL
	jobURL := normalizeURL(token.InstanceURL) + "/api/v1/ingest/jobs"

	fmt.Println("jobURL", jobURL)
	// print request
	fmt.Println("request", request)

	// Marshal request body
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job request: %w", err)
	}

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "POST", jobURL, strings.NewReader(string(requestBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create job request: %w", err)
	}

	// Set headers
	setCommonHeaders(req, token.AccessToken)

	// Execute request
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read job creation response: %w", err)
	}

	// Check for success (201 Created)
	if resp.StatusCode != http.StatusCreated {
		return nil, handleErrorResponse(resp.StatusCode, body, "job creation")
	}

	// Parse response
	var jobResp CreateJobResponse
	if err := json.Unmarshal(body, &jobResp); err != nil {
		return nil, fmt.Errorf("failed to parse job creation response: %w", err)
	}

	return &jobResp, nil
}

// UploadJobData uploads data to an existing job
// Note: Use the same token type that was successful for CreateJob
// contentType should be "text/csv" for CSV data or "application/json" for JSON data
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-upload-job-data.html
func (c *Client) UploadJobData(ctx context.Context, jobID string, data []byte, contentType string) error {
	token := c.cdpToken
	// Validate token
	if err := validateToken(token, "CDP token"); err != nil {
		return err
	}

	if jobID == "" {
		return fmt.Errorf("job ID cannot be empty")
	}

	// Build upload URL
	uploadURL := normalizeURL(token.InstanceURL) + "/api/v1/ingest/jobs/" + jobID + "/batches"

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create upload request: %w", err)
	}

	// Set headers for data upload
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)

	// Execute request
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check for success (202 Accepted)
	if resp.StatusCode == http.StatusAccepted {
		return nil
	} else {
		body, _ := io.ReadAll(resp.Body)
		return handleErrorResponse(resp.StatusCode, body, "job data upload")
	}
}

// CloseJob closes or aborts an ingestion job
// state should be "UploadComplete" to close the job or "Aborted" to abort it
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-close-abort-job.html
func (c *Client) CloseJob(ctx context.Context, jobID string, state string) (*CloseJobResponse, error) {
	token := c.cdpToken
	// Validate token
	if err := validateToken(token, "CDP token"); err != nil {
		return nil, err
	}

	if jobID == "" {
		return nil, fmt.Errorf("job ID cannot be empty")
	}

	if state != "UploadComplete" && state != "Aborted" {
		return nil, fmt.Errorf("state must be 'UploadComplete' or 'Aborted', got: %s", state)
	}

	// Build close job URL
	closeURL := normalizeURL(token.InstanceURL) + "/api/v1/ingest/jobs/" + jobID

	// Create request body
	request := &CloseJobRequest{State: state}
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal close job request: %w", err)
	}

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "PATCH", closeURL, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create close job request: %w", err)
	}

	// Set headers
	setCommonHeaders(req, token.AccessToken)

	// Execute request
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	fmt.Println("body", string(body))
	if err != nil {
		return nil, fmt.Errorf("failed to read close job response: %w", err)
	}

	// Check for success (200 OK)
	if resp.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(resp.StatusCode, body, "job close/abort")
	}

	// Parse response
	var jobResp CloseJobResponse
	if err := json.Unmarshal(body, &jobResp); err != nil {
		return nil, fmt.Errorf("failed to parse close job response: %w", err)
	}

	return &jobResp, nil
}

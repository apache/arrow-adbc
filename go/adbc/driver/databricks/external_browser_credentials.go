// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package databricks

import (
	"context"
	"fmt"
	"html"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/config/credentials"
	"github.com/databricks/databricks-sdk-go/config/experimental/auth"
	"github.com/databricks/databricks-sdk-go/logger"
	"github.com/pkg/browser"
	"golang.org/x/oauth2"
)

const (
	defaultClientID          = "dbt-databricks"
	defaultRedirectURI       = "http://localhost:8020"
	defaultPort              = 8020
	tokenPath                = "oidc/v1/token"
	authorizePath            = "oidc/v1/authorize"
	browserErrorWithDescHtml = `
<!DOCTYPE html>
<html>
<head>
    <title>Databricks Authentication - Error</title>
</head>
<body>
    <h1>Authentication Failed</h1>
    <p>Error: %s</p>
    <p>Description: %s</p>
    <p>You can close this window and try again.</p>
</body>
</html>`
	browserErrorGenericHtml = `
<!DOCTYPE html>
<html>
<head>
    <title>Databricks Authentication - Error</title>
</head>
<body>
    <h1>Authentication Failed</h1>
    <p>No authorization code received.</p>
    <p>You can close this window and try again.</p>
</body>
</html>
`
	browserSuccessHtml = `
<!DOCTYPE html>
<html>
<head>
    <title>Databricks Authentication - Success</title>
</head>
<body>
    <h1>Login Successful!</h1>
    <p>You can close this window and return to your application.</p>
</body>
</html>`
)

// ExternalBrowserCredentials implements CredentialsStrategy for OAuth 2 PKCE flow for external browser authentication
// https://github.com/databricks/databricks-sdk-go/blob/56af964ae7d5f86bd454dfdba4edac42aac4d19f/config/config.go#L26
type ExternalBrowserCredentials struct {
	Host        string
	ClientID    string
	RedirectURI string
	tokenCache  *TokenCache
}

// From CredentialsStrategy interface
func (c *ExternalBrowserCredentials) Name() string {
	return "external-browser"
}

// From CredentialsStrategy interface
func (c *ExternalBrowserCredentials) Configure(ctx context.Context, cfg *config.Config) (credentials.CredentialsProvider, error) {
	if c.Host == "" && cfg.Host != "" {
		c.Host = cfg.Host
	}
	if c.ClientID == "" {
		c.ClientID = defaultClientID
	}
	if c.RedirectURI == "" {
		c.RedirectURI = defaultRedirectURI
	}

	tokenCache, err := NewTokenCache()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize token cache: %w", err)
	}
	c.tokenCache = tokenCache

	return c.createCredentialsProvider(), nil
}

// getOrRefreshCachedToken attempts to retrieve and optionally refresh a cached token
func (c *ExternalBrowserCredentials) getOrRefreshCachedToken(ctx context.Context) (*oauth2.Token, error) {
	if c.tokenCache == nil {
		return nil, fmt.Errorf("token cache not initialized")
	}

	entry, err := c.tokenCache.LoadToken(ctx, c.Host, c.ClientID)
	if err != nil {
		return c.performOAuthFlow(ctx)
	}

	if entry.RefreshToken != "" {
		refreshedToken, err := c.performTokenRefresh(ctx, entry.RefreshToken)
		if err == nil {
			// Save refreshed token to cache
			if cacheErr := c.tokenCache.SaveToken(ctx, refreshedToken, c.Host, c.ClientID); cacheErr != nil {
				logger.Warnf(ctx, "failed to save refreshed token to cache: %v", cacheErr)
			}
			return refreshedToken, nil
		}
		logger.Warnf(ctx, "token refresh failed, falling back to full OAuth flow: %v", err)
	}

	return c.performOAuthFlow(ctx)
}

// creates a CredentialsProvider that uses our access token
func (c *ExternalBrowserCredentials) createCredentialsProvider() credentials.CredentialsProvider {
	// TokenSourceFn wraps a token fetching function to be invoked for getting new tokens
	tokenSource := auth.TokenSourceFn(func(ctx context.Context) (*oauth2.Token, error) {
		return c.getOrRefreshCachedToken(ctx)
	})

	cachedTokenSource := auth.NewCachedTokenSource(tokenSource)
	return credentials.NewOAuthCredentialsProviderFromTokenSource(cachedTokenSource)
}

// base OAuth2 config
func (c *ExternalBrowserCredentials) buildOAuth2Config() *oauth2.Config {
	return &oauth2.Config{
		ClientID: c.ClientID,
		Endpoint: oauth2.Endpoint{
			TokenURL: c.buildTokenURL(),
		},
		Scopes: []string{"all-apis", "offline_access"},
	}
}

func (c *ExternalBrowserCredentials) buildOAuth2ConfigWithAuth(redirectURI string) *oauth2.Config {
	config := c.buildOAuth2Config()
	config.Endpoint.AuthURL = c.buildAuthBaseURL()
	config.RedirectURL = redirectURI
	return config
}

func (c *ExternalBrowserCredentials) buildOAuth2ConfigWithRedirect(redirectURI string) *oauth2.Config {
	config := c.buildOAuth2Config()
	config.RedirectURL = redirectURI
	return config
}

// builds the token endpoint URL with proper protocol scheme
func (c *ExternalBrowserCredentials) buildTokenURL() string {
	host := c.Host
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "https://" + host
	}
	return strings.TrimSuffix(host, "/") + "/" + tokenPath
}

// performs full OAuth PKCE flow
// OAuth2: RFC9470
// PKCE: RFC7636
func (c *ExternalBrowserCredentials) performOAuthFlow(ctx context.Context) (*oauth2.Token, error) {
	// PKCE verifier
	verifier := oauth2.GenerateVerifier()

	// State for CSRF protection
	state := oauth2.GenerateVerifier()

	// Find available port
	port := c.findAvailablePort()
	redirectURI := fmt.Sprintf("http://localhost:%d", port)

	config := c.buildOAuth2ConfigWithAuth(redirectURI)

	authURL := config.AuthCodeURL(state,
		oauth2.S256ChallengeOption(verifier),
		oauth2.AccessTypeOffline)

	codeChan := make(chan string, 1)
	errorChan := make(chan error, 1)

	// local server to receive auth code from redirect
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			code := r.URL.Query().Get("code")
			errorParam := r.URL.Query().Get("error")

			if errorParam != "" {
				errorDesc := r.URL.Query().Get("error_description")
				escapedError := html.EscapeString(errorParam)
				escapedErrorDesc := html.EscapeString(errorDesc)
				fmt.Fprintf(w, browserErrorWithDescHtml, escapedError, escapedErrorDesc)
				errorChan <- fmt.Errorf("oauth error: %s - %s", errorParam, errorDesc)
				return
			}

			if code == "" {
				fmt.Fprint(w, browserErrorGenericHtml)
				errorChan <- fmt.Errorf("no authorization code received")
				return
			}

			// Verify state parameter matches to prevent CSRF attacks
			if r.URL.Query().Get("state") != state {
				fmt.Fprintf(w, browserErrorWithDescHtml, "state mismatch", "state parameter did not match. Possible CSRF attack.")
				errorChan <- fmt.Errorf("state mismatch - possible CSRF attack")
				return
			}

			fmt.Fprint(w, browserSuccessHtml)
			codeChan <- code
		}),
	}

	// start local server
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errorChan <- fmt.Errorf("server error: %w", err)
		}
	}()

	// wait for server to start up
	time.Sleep(100 * time.Millisecond)

	if err := openBrowser(ctx, authURL); err != nil {
		return nil, err
	}

	// Wait for authorization code or 10 min timeout
	var authCode string
	select {
	case authCode = <-codeChan:
	case err := <-errorChan:
		server.Shutdown(ctx)
		return nil, fmt.Errorf("oauth callback error: %w", err)
	case <-time.After(10 * time.Minute):
		server.Shutdown(ctx)
		return nil, fmt.Errorf("authentication timeout after 10 minutes - please ensure you complete the browser authentication flow")
	case <-ctx.Done():
		server.Shutdown(ctx)
		return nil, fmt.Errorf("authentication cancelled: %w", ctx.Err())
	}

	// Shutdown server
	server.Shutdown(ctx)

	// Exchange code for tokens
	token, err := c.exchangeCodeForTokens(ctx, authCode, verifier, redirectURI)
	if err != nil {
		return nil, err
	}

	// Save token to cache for future use
	if c.tokenCache != nil {
		if cacheErr := c.tokenCache.SaveToken(ctx, token, c.Host, c.ClientID); cacheErr != nil {
			// Log warning but don't fail the OAuth flow
			logger.Warnf(ctx, "failed to save token to cache: %v", cacheErr)
		}
	}

	return token, nil
}

// performTokenRefresh refreshes a token using the refresh token
func (c *ExternalBrowserCredentials) performTokenRefresh(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	config := c.buildOAuth2Config()

	// Create a token with just the refresh token to use for refreshing
	token := &oauth2.Token{
		RefreshToken: refreshToken,
	}

	// Use the token source to refresh
	tokenSource := config.TokenSource(ctx, token)
	refreshedToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %w", err)
	}

	return refreshedToken, nil
}

// findAvailablePort finds an available port starting from the default
func (c *ExternalBrowserCredentials) findAvailablePort() int {
	// Try the default port first
	if c.isPortAvailable(defaultPort) {
		return defaultPort
	}

	// Try ports in a reasonable range
	for port := defaultPort + 1; port < defaultPort+100; port++ {
		if c.isPortAvailable(port) {
			logger.Infof(context.Background(), "Port %d was in use, using port %d instead", defaultPort, port)
			return port
		}
	}

	// If we can't find any available port, return the default and let it fail with a clear error
	logger.Warnf(context.Background(), "Could not find an available port, using default port %d", defaultPort)
	return defaultPort
}

// isPortAvailable checks if a port is available
func (c *ExternalBrowserCredentials) isPortAvailable(port int) bool {
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		ln.Close()
	}
	return err != nil
}

// base oidc endpoint
func (c *ExternalBrowserCredentials) buildAuthBaseURL() string {
	host := c.Host
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "https://" + host
	}
	return strings.TrimSuffix(host, "/") + "/" + authorizePath
}

// exchangeCodeForTokens exchanges the authorization code for access/refresh tokens
func (c *ExternalBrowserCredentials) exchangeCodeForTokens(ctx context.Context, code, codeVerifier, redirectURI string) (*oauth2.Token, error) {
	config := c.buildOAuth2ConfigWithRedirect(redirectURI)

	// exchange for token with PKCE
	return config.Exchange(ctx, code,
		oauth2.SetAuthURLParam("code_verifier", codeVerifier),
		oauth2.S256ChallengeOption(oauth2.GenerateVerifier()),
	)
}

// will fail if no display (headless shell)
func openBrowser(ctx context.Context, browserURL string) error {
	parsedURL, err := url.ParseRequestURI(browserURL)
	if err != nil {
		logger.Errorf(ctx, "error parsing url %v, err: %v", browserURL, err)
		return err
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("invalid browser URL: %v", browserURL)
	}
	err = browser.OpenURL(browserURL)
	if err != nil {
		logger.Errorf(ctx, "failed to open a browser. err: %v", err)
		return err
	}
	return nil
}

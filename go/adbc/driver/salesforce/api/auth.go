package api

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/oauth2/jwt"
	"resty.dev/v3"
)

func (c *Client) Authenticate(ctx context.Context) error {
	// Potential BUG: calling this multiple times will lead to multiple middlewares in the chain

	privateKeyPEM := c.config.PrivateKeyPEM
	if !strings.Contains(privateKeyPEM, "BEGIN") {
		return fmt.Errorf("invalid private key: must be PEM-encoded")
	}

	tokenURL := strings.TrimRight(c.config.LoginURL, "/") + "/services/oauth2/token"

	jwtConf := &jwt.Config{
		Email:      c.config.ClientID,
		Subject:    c.config.Username,
		PrivateKey: []byte(privateKeyPEM),
		TokenURL:   tokenURL,
		Audience:   c.config.LoginURL,
	}

	// Get the initial token to extract instance_url from the JWT response.
	token, err := jwtConf.TokenSource(ctx).Token()
	if err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	instanceURL, ok := token.Extra("instance_url").(string)
	if !ok || instanceURL == "" {
		return fmt.Errorf("authentication succeeded but instance_url not found in response")
	}

	c.http.SetBaseURL(instanceURL)
	c.tokenSource = jwtConf.TokenSource(ctx)

	// Install a request middleware that refreshes the token automatically.
	// oauth2.TokenSource caches the token and only refreshes when expired.
	c.http.AddRequestMiddleware(func(_ *resty.Client, req *resty.Request) error {
		t, err := c.tokenSource.Token()
		if err != nil {
			return fmt.Errorf("token refresh failed: %w", err)
		}
		req.SetAuthScheme(t.Type()).SetAuthToken(t.AccessToken)
		return nil
	})

	c.logger.DebugContext(ctx, "authenticated", "instance_url", instanceURL)
	return nil
}

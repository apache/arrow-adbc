package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// GetJSON performs a GET request with JSON response handling
// ResponseType is the response payload type
func GetJSON[ResponseType any](c *Client, ctx context.Context, path string, queryParams url.Values) (*ResponseType, error) {
	req, err := createRequest(c, ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}

	// Add query parameters
	if queryParams != nil {
		query := req.URL.Query()
		for key, values := range queryParams {
			for _, value := range values {
				query.Add(key, value)
			}
		}
		req.URL.RawQuery = query.Encode()
	}

	return executeJSONRequest[ResponseType](c, ctx, req)
}

// PostJSON performs a POST request with JSON request/response handling
// RequestType is the request payload type, ResponseType is the response payload type
func PostJSON[RequestType any, ResponseType any](c *Client, ctx context.Context, path string, request *RequestType) (*ResponseType, error) {
	// Marshal request to JSON
	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := createRequest(c, ctx, "POST", path, strings.NewReader(string(reqBody)))
	if err != nil {
		return nil, err
	}

	return executeJSONRequest[ResponseType](c, ctx, req)
}

// DeleteJSON performs a DELETE request
func DeleteJSON(c *Client, ctx context.Context, path string) error {
	req, err := createRequest(c, ctx, "DELETE", path, nil)
	if err != nil {
		return err
	}

	return executeRequest(c, ctx, req)
}

// executeJSONRequest performs an HTTP request and handles the response with JSON unmarshaling
func executeJSONRequest[ResponseType any](c *Client, ctx context.Context, req *http.Request) (*ResponseType, error) {
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	statusOK := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !statusOK {
		return nil, handleErrorResponse(resp.StatusCode, responseBody, "request_failed")
	}

	var response ResponseType
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &response, nil
}

// executeRequest performs an HTTP request and handles the response without JSON unmarshaling
func executeRequest(c *Client, ctx context.Context, req *http.Request) error {
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	statusOK := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !statusOK {
		return handleErrorResponse(resp.StatusCode, responseBody, "request_failed")
	}

	return nil
}

// createRequest creates an HTTP request with common headers
func createRequest(c *Client, ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	url := c.buildServicesURL(c.accessToken.InstanceURL, path)

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	setCommonHeaders(req, c.accessToken.AccessToken)
	return req, nil
}

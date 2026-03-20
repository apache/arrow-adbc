package api

import (
	"encoding/json"
	"fmt"

	"resty.dev/v3"
)

// SalesforceError represents an error response from the Salesforce API.
// The API typically returns a JSON array of error objects, though usually only one.
type SalesforceError struct {
	// TODO: it probably makes more sense to embed the *http.Response with a `json:"-"` tag
	// We can unmarshal the response body
	StatusCode int
	Code       string `json:"errorCode"`
	Message    string `json:"message"`
	Type       string `json:"type,omitempty"`
}

func (e *SalesforceError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("salesforce %d %s: %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("salesforce %d: %s", e.StatusCode, e.Message)
}

func (e *SalesforceError) IsNotFound() bool    { return e.StatusCode == 404 }
func (e *SalesforceError) IsRateLimited() bool { return e.StatusCode == 429 }

// checkError parses a Salesforce error response.
// The API returns either a single error object or an array of error objects.
func checkError(resp *resty.Response) error {
	if resp.IsSuccess() {
		return nil
	}

	sfErr := &SalesforceError{
		StatusCode: resp.StatusCode(),
		Message:    resp.Status(),
	}

	body := resp.Bytes()
	if len(body) == 0 {
		return sfErr
	}

	// Try as array of error objects (most common)
	var arr []SalesforceError
	if err := json.Unmarshal(body, &arr); err == nil && len(arr) > 0 {
		arr[0].StatusCode = resp.StatusCode()
		return &arr[0]
	}

	// Try as single error object
	if err := json.Unmarshal(body, sfErr); err == nil && sfErr.Message != resp.Status() {
		return sfErr
	}

	return sfErr
}
